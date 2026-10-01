import { signWebhookToken } from "@typebot.io/lib/signWebhookToken";
import { verifyWebhookToken } from "@typebot.io/lib/verifyWebhookToken";

import {
  routePartykitRequest,
  Server,
  type Connection,
  type ConnectionContext,
} from "partyserver";


/* =========================================================
   ENV
   ========================================================= */

type Env = {
  Main: DurableObjectNamespace;
  WEBHOOK_RELAY_SECRET?: string;
};


/* =========================================================
   ESTADO DA CONEXÃO WEBSOCKET

   Esse estado é persistido pelo PartyServer
   e continua disponível após hibernação.
   ========================================================= */

type WebhookConnectionState = {
  token: string;
};


/* =========================================================
   DURABLE OBJECT
   ========================================================= */

export class Main extends Server<Env> {

  static options = {
    hibernate: true,
  };


  /* =======================================================
     QUANDO O WEBSOCKET CONECTAR

     Salva o token da assinatura dentro da conexão.

     Isso evita depender exclusivamente de connection.uri
     depois que o Durable Object hiberna/acorda.
     ======================================================= */

  onConnect(
    connection: Connection<WebhookConnectionState>,
    context: ConnectionContext,
  ) {

    const url =
      new URL(
        context.request.url,
      );


    const token =
      url.searchParams.get(
        "token",
      );


    if (!token) {

      console.error(
        "[TYPEBOT RELAY] WebSocket sem token",
        {
          room:
            this.name,

          connectionId:
            connection.id,
        },
      );


      connection.close(
        1008,
        "Missing subscription token",
      );


      return;

    }


    /*
      Persiste o token junto com o WebSocket.
    */

    connection.setState({
      token,
    });


    console.log(
      "[TYPEBOT RELAY] Listener conectado",
      {
        room:
          this.name,

        connectionId:
          connection.id,

        hasToken:
          true,
      },
    );

  }


  /* =======================================================
     PUBLICAÇÃO DO WEBHOOK
     ======================================================= */

  async onRequest(
    request: Request,
  ): Promise<Response> {

    /* =====================================================
       SOMENTE POST
       ===================================================== */

    if (
      request.method !==
      "POST"
    ) {

      return new Response(
        "Method not allowed",
        {
          status:
            405,
        },
      );

    }


    /* =====================================================
       SEGREDO
       ===================================================== */

    const secret =
      getSecret(
        this.env,
      );


    if (!secret) {

      console.error(
        "[TYPEBOT RELAY] WEBHOOK_RELAY_SECRET ausente",
      );


      return new Response(
        "Webhook relay secret is not configured",
        {
          status:
            500,
        },
      );

    }


    /* =====================================================
       TOKEN DE PUBLICAÇÃO
       ===================================================== */

    const body =
      await request.text();


    const publication =
      await verifyWebhookToken(
        body,
        secret,
      );


    /* =====================================================
       VALIDA PUBLICAÇÃO
       ===================================================== */

    if (!publication) {

      console.error(
        "[TYPEBOT RELAY] Publication token inválido",
        {
          room:
            this.name,
        },
      );


      return new Response(
        "Unauthorized",
        {
          status:
            401,
        },
      );

    }


    if (
      publication.purpose !==
      "publish"
    ) {

      console.error(
        "[TYPEBOT RELAY] Purpose inválido",
        {
          room:
            this.name,

          purpose:
            publication.purpose,
        },
      );


      return new Response(
        "Unauthorized",
        {
          status:
            401,
        },
      );

    }


    const encodedPublicationRoom =
      encodeURIComponent(
        publication.room,
      );


    if (
      encodedPublicationRoom !==
      this.name
    ) {

      console.error(
        "[TYPEBOT RELAY] Room incompatível",
        {
          durableObjectRoom:
            this.name,

          publicationRoom:
            publication.room,

          encodedPublicationRoom,
        },
      );


      return new Response(
        "Unauthorized",
        {
          status:
            401,
        },
      );

    }


    if (
      publication.payload ===
      undefined
    ) {

      console.error(
        "[TYPEBOT RELAY] Payload ausente",
        {
          room:
            this.name,

          blockId:
            publication.blockId,
        },
      );


      return new Response(
        "Unauthorized",
        {
          status:
            401,
        },
      );

    }


    console.log(
      "[TYPEBOT RELAY] Publicação recebida",
      {
        room:
          this.name,

        blockId:
          publication.blockId,

        waitNonce:
          publication.waitNonce,

        publicationNonce:
          publication.nonce,
      },
    );


    /* =====================================================
       ANTI-REPLAY

       Mesma lógica original do Typebot.
       ===================================================== */

    const claimed =
      await this.ctx.storage.transaction(
        async (
          storage,
        ) => {

          const publications =
            await storage.list<number>({
              prefix:
                "publication:",
            });


          const key =
            `publication:${publication.nonce}`;


          /*
            Publicação já usada.
          */

          if (
            publications.has(
              key,
            )
          ) {

            return false;

          }


          /*
            Remove nonces expirados.
          */

          const expired =
            [...publications]
              .filter(
                (
                  [
                    ,
                    expiresAt,
                  ],
                ) =>
                  expiresAt <=
                  Date.now(),
              )
              .map(
                (
                  [
                    publicationKey,
                  ],
                ) =>
                  publicationKey,
              );


          if (
            expired.length >
            0
          ) {

            await storage.delete(
              expired,
            );

          }


          /*
            Registra nonce atual.
          */

          await storage.put(
            key,
            publication.expiresAt,
          );


          return true;

        },
      );


    if (!claimed) {

      console.error(
        "[TYPEBOT RELAY] Publication já utilizada",
        {
          nonce:
            publication.nonce,

          room:
            this.name,
        },
      );


      return new Response(
        "Publication already used",
        {
          status:
            409,
        },
      );

    }


    /* =====================================================
       CONEXÕES ATIVAS
       ===================================================== */

    const connections =
      [
        ...this.getConnections(),
      ];


    console.log(
      "[TYPEBOT RELAY] Procurando listener",
      {
        room:
          this.name,

        connections:
          connections.length,

        publicationBlockId:
          publication.blockId,

        publicationWaitNonce:
          publication.waitNonce,
      },
    );


    /* =====================================================
       ENTREGA
       ===================================================== */

    let delivered =
      0;


    for (
      const rawConnection
      of connections
    ) {

      const connection =
        rawConnection as
          Connection<WebhookConnectionState>;


      /*
        Primeiro tenta pegar do state.

        Se for uma conexão criada antes deste deploy,
        usa connection.uri como fallback.
      */

      let subscriptionToken =
        connection.state?.token ??
        null;


      if (
        !subscriptionToken &&
        connection.uri
      ) {

        try {

          subscriptionToken =
            new URL(
              connection.uri,
            ).searchParams.get(
              "token",
            );

        } catch (
          error
        ) {

          console.error(
            "[TYPEBOT RELAY] Falha lendo connection.uri",
            {
              connectionId:
                connection.id,

              error:
                String(error),
            },
          );

        }

      }


      /* ===================================================
         NÃO TEM TOKEN
         =================================================== */

      if (
        !subscriptionToken
      ) {

        console.error(
          "[TYPEBOT RELAY] Conexão sem subscription token",
          {
            connectionId:
              connection.id,

            room:
              this.name,

            hasState:
              !!connection.state,

            hasUri:
              !!connection.uri,
          },
        );


        connection.close(
          1008,
          "Missing subscription token",
        );


        continue;

      }


      /* ===================================================
         VERIFICA SUBSCRIPTION TOKEN
         =================================================== */

      const subscription =
        await verifyWebhookToken(
          subscriptionToken,
          secret,
        );


      if (!subscription) {

        console.error(
          "[TYPEBOT RELAY] Subscription token inválido",
          {
            connectionId:
              connection.id,

            room:
              this.name,
          },
        );


        connection.close(
          1008,
          "Invalid subscription",
        );


        continue;

      }


      /* ===================================================
         PURPOSE
         =================================================== */

      if (
        subscription.purpose !==
        "subscribe"
      ) {

        console.error(
          "[TYPEBOT RELAY] Subscription purpose inválido",
          {
            connectionId:
              connection.id,

            purpose:
              subscription.purpose,
          },
        );


        connection.close(
          1008,
          "Invalid subscription purpose",
        );


        continue;

      }


      /* ===================================================
         ROOM
         =================================================== */

      const encodedSubscriptionRoom =
        encodeURIComponent(
          subscription.room,
        );


      if (
        encodedSubscriptionRoom !==
        this.name
      ) {

        console.error(
          "[TYPEBOT RELAY] Subscription room incompatível",
          {
            connectionId:
              connection.id,

            durableObjectRoom:
              this.name,

            subscriptionRoom:
              subscription.room,

            encodedSubscriptionRoom,
          },
        );


        connection.close(
          1008,
          "Invalid subscription room",
        );


        continue;

      }


      /* ===================================================
         DEBUG DA COMPARAÇÃO
         =================================================== */

      console.log(
        "[TYPEBOT RELAY] Comparando listener",
        {
          connectionId:
            connection.id,

          subscriptionBlockId:
            subscription.blockId,

          publicationBlockId:
            publication.blockId,

          subscriptionNonce:
            subscription.nonce,

          publicationWaitNonce:
            publication.waitNonce,

          blockMatches:
            subscription.blockId ===
            publication.blockId,

          nonceMatches:
            publication.waitNonce ===
              undefined ||
            subscription.nonce ===
              publication.waitNonce,
        },
      );


      /* ===================================================
         BLOCO / NONCE
         =================================================== */

      if (
        subscription.blockId !==
          publication.blockId ||
        (
          publication.waitNonce !==
            undefined &&
          subscription.nonce !==
            publication.waitNonce
        )
      ) {

        continue;

      }


      /* ===================================================
         ASSINA RESPOSTA
         =================================================== */

      const responseToken =
        await signWebhookToken(
          {
            ...subscription,

            purpose:
              "response",

            payload:
              publication.payload,
          },

          secret,
        );


      /* ===================================================
         ENVIA PARA O TYPEBOT
         =================================================== */

      connection.send(
        responseToken,
      );


      delivered++;


      console.log(
        "[TYPEBOT RELAY] Webhook entregue",
        {
          room:
            this.name,

          connectionId:
            connection.id,

          blockId:
            publication.blockId,
        },
      );

    }


    /* =====================================================
       RESULTADO
       ===================================================== */

    console.log(
      "[TYPEBOT RELAY] Resultado da publicação",
      {
        room:
          this.name,

        blockId:
          publication.blockId,

        connections:
          connections.length,

        delivered,
      },
    );


    /* =====================================================
       SEM LISTENER CORRESPONDENTE
       ===================================================== */

    if (
      delivered ===
      0
    ) {

      return new Response(
        "No matching webhook listener; retry the callback",
        {
          status:
            503,
        },
      );

    }


    /* =====================================================
       SUCESSO
       ===================================================== */

    return new Response(
      "OK",
      {
        status:
          200,
      },
    );

  }

}


/* =========================================================
   WORKER ROUTER
   ========================================================= */

export default {

  async fetch(
    request: Request,
    env: Env,
  ): Promise<Response> {

    const response =
      await routePartykitRequest(
        request,
        env,
        {

          /* ===============================================
             AUTENTICAÇÃO DO WEBSOCKET
             =============================================== */

          onBeforeConnect:
            async (
              request,
              lobby,
            ) => {

              const secret =
                getSecret(
                  env,
                );


              if (!secret) {

                console.error(
                  "[TYPEBOT RELAY] WEBHOOK_RELAY_SECRET ausente no connect",
                );


                return new Response(
                  "Webhook relay secret is not configured",
                  {
                    status:
                      500,
                  },
                );

              }


              const url =
                new URL(
                  request.url,
                );


              const token =
                url.searchParams.get(
                  "token",
                );


              if (!token) {

                console.error(
                  "[TYPEBOT RELAY] WebSocket sem token",
                  {
                    room:
                      lobby.name,
                  },
                );


                return new Response(
                  "Unauthorized",
                  {
                    status:
                      401,
                  },
                );

              }


              const claims =
                await verifyWebhookToken(
                  token,
                  secret,
                );


              if (!claims) {

                console.error(
                  "[TYPEBOT RELAY] Token WebSocket inválido",
                  {
                    room:
                      lobby.name,
                  },
                );


                return new Response(
                  "Unauthorized",
                  {
                    status:
                      401,
                  },
                );

              }


              const encodedClaimsRoom =
                encodeURIComponent(
                  claims.room,
                );


              console.log(
                "[TYPEBOT RELAY] WebSocket auth",
                {
                  lobbyName:
                    lobby.name,

                  claimsRoom:
                    claims.room,

                  encodedClaimsRoom,

                  purpose:
                    claims.purpose,

                  roomMatches:
                    encodedClaimsRoom ===
                    lobby.name,
                },
              );


              if (
                claims.purpose !==
                  "subscribe" ||
                encodedClaimsRoom !==
                  lobby.name
              ) {

                console.error(
                  "[TYPEBOT RELAY] WebSocket rejeitado",
                  {
                    lobbyName:
                      lobby.name,

                    purpose:
                      claims.purpose,

                    encodedClaimsRoom,
                  },
                );


                return new Response(
                  "Unauthorized",
                  {
                    status:
                      401,
                  },
                );

              }


              return request;

            },

        },
      );


    /* =====================================================
       ROTA NÃO ENCONTRADA
       ===================================================== */

    if (!response) {

      return new Response(
        "Not Found",
        {
          status:
            404,
        },
      );

    }


    return response;

  },

} satisfies ExportedHandler<Env>;


/* =========================================================
   SECRET
   ========================================================= */

const getSecret = (
  env: Env,
) => {

  return typeof
    env.WEBHOOK_RELAY_SECRET ===
    "string"
    ? env.WEBHOOK_RELAY_SECRET
    : undefined;

};