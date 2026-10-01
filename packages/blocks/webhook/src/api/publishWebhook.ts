import { ORPCError } from "@orpc/server";
import { env } from "@typebot.io/env";
import { signWebhookToken } from "@typebot.io/lib/signWebhookToken";
import PartySocket from "partysocket";
import { serializeWebhookResponse } from "./serializeWebhookResponse";

export const publishWebhook = async (
  room: string,
  blockId: string,
  body: unknown,
  waitNonce?: string,
) => {
  if (!env.NEXT_PUBLIC_PARTYKIT_HOST) {
    throw new ORPCError("NOT_FOUND", {
      message: "PartyKit not configured",
    });
  }

  const encodedRoom = encodeURIComponent(room);

  const token = await signWebhookToken(
    {
      purpose: "publish",
      room,
      blockId,
      waitNonce,
      nonce: crypto.randomUUID(),
      expiresAt: Date.now() + 60_000,
      payload: serializeWebhookResponse(body),
    },
    env.WEBHOOK_RELAY_SECRET,
  );

  console.log("[WEBHOOK PUBLISH] Sending publication", {
    host: env.NEXT_PUBLIC_PARTYKIT_HOST,
    party: "main",
    room,
    encodedRoom,
    blockId,
    waitNonce,
  });

  let response: Response;

  try {
    response = await PartySocket.fetch(
      {
        host: env.NEXT_PUBLIC_PARTYKIT_HOST,

        // Deixa explícito.
        party: "main",

        // PartySocket NÃO faz encode automaticamente.
        room: encodedRoom,
      },
      {
        method: "POST",
        body: token,
        headers: {
          "Content-Type": "text/plain;charset=UTF-8",
        },
      },
    );
  } catch (error) {
    console.error("[WEBHOOK PUBLISH] Network error", {
      error:
        error instanceof Error
          ? error.message
          : String(error),
    });

    throw new ORPCError("BAD_GATEWAY", {
      message: "Could not reach webhook relay",
    });
  }

  /*
   * Muito importante:
   *
   * lê o corpo enviado pelo Worker.
   *
   * Ex:
   * Unauthorized
   * Publication already used
   * No matching webhook listener; retry the callback
   */

  const responseBody = await response.text();

  console.log("[WEBHOOK PUBLISH] Relay response", {
    status: response.status,
    statusText: response.statusText,
    ok: response.ok,
    body: responseBody,
  });

  if (!response.ok) {
    console.error("[WEBHOOK PUBLISH] Relay rejected publication", {
      status: response.status,
      statusText: response.statusText,
      body: responseBody,
      room,
      encodedRoom,
      blockId,
      waitNonce,
    });

    throw new ORPCError("BAD_GATEWAY", {
      message: `Webhook relay rejected publication (${response.status}): ${responseBody}`,
    });
  }

  console.log("[WEBHOOK PUBLISH] Publication delivered", {
    room,
    blockId,
  });
};