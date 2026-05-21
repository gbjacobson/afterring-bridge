import http from "node:http";
import { URL } from "node:url";
import WebSocket, { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || process.env.AFTER_RING_REALTIME_PORT || 8787);
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || "gpt-realtime";
const APP_BASE_URL = (process.env.AFTER_RING_PUBLIC_BASE_URL || "http://localhost:3040").replace(/\/$/, "");
const CRON_SECRET = process.env.CRON_SECRET || "";
const REALTIME_VOICE = process.env.OPENAI_REALTIME_VOICE || "sage";

function json(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  res.end(JSON.stringify(body));
}

async function fetchSession(accountId) {
  const url = new URL("/api/ai-receptionist/session", APP_BASE_URL);
  url.searchParams.set("accountId", accountId);
  const response = await fetch(url, {
    headers: { Authorization: `Bearer ${CRON_SECRET}` },
  });
  if (!response.ok) {
    throw new Error(`Session fetch failed: ${response.status}`);
  }
  return response.json();
}

async function postAction(payload) {
  const response = await fetch(new URL("/api/ai-receptionist/actions", APP_BASE_URL), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${CRON_SECRET}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`Action failed: ${response.status} ${text}`);
  }
  return response.json();
}

function safeSend(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(typeof payload === "string" ? payload : JSON.stringify(payload));
  }
}

function parseToolArgs(value) {
  try {
    return JSON.parse(value || "{}");
  } catch {
    return {};
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    return json(res, 200, {
      ok: true,
      model: OPENAI_REALTIME_MODEL,
      hasOpenAI: Boolean(OPENAI_API_KEY),
      hasAppSecret: Boolean(CRON_SECRET),
    });
  }
  json(res, 404, { error: "Not found" });
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  if (url.pathname !== "/twilio/realtime") {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
});

wss.on("connection", (twilioWs) => {
  let openaiWs;
  let accountId = "";
  let leadId = "";
  let streamSid = "";
  let transcript = [];
  let pendingToolNames = new Map();

  async function connectOpenAI() {
    const session = await fetchSession(accountId);
    openaiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`, {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Safety-Identifier": `afterring-${accountId}`,
      },
    });

    openaiWs.on("open", () => {
      safeSend(openaiWs, {
        type: "session.update",
        session: {
          instructions: session.instructions,
          voice: REALTIME_VOICE,
          input_audio_format: "g711_ulaw",
          output_audio_format: "g711_ulaw",
          turn_detection: {
            type: "server_vad",
            threshold: 0.5,
            prefix_padding_ms: 250,
            silence_duration_ms: session.conciseMode ? 450 : 650,
          },
          temperature: 0.7,
          tools: [
            {
              type: "function",
              name: "send_booking_link",
              description: "Text the caller the booking link once they want to book or ask for a link.",
              parameters: {
                type: "object",
                properties: {
                  reason: { type: "string" },
                },
              },
            },
            {
              type: "function",
              name: "finish_call",
              description: "Save a concise owner-facing summary at the end of the call.",
              parameters: {
                type: "object",
                properties: {
                  intent: { type: "string" },
                  urgency: { type: "string", enum: ["low", "normal", "high"] },
                  summary: { type: "string" },
                },
                required: ["summary"],
              },
            },
          ],
        },
      });

      safeSend(openaiWs, {
        type: "conversation.item.create",
        item: {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "The phone call just connected. Greet the caller now." }],
        },
      });
      safeSend(openaiWs, { type: "response.create" });
    });

    openaiWs.on("message", async (raw) => {
      let event;
      try {
        event = JSON.parse(raw.toString());
      } catch {
        return;
      }

      if (event.type === "response.audio.delta" && event.delta && streamSid) {
        safeSend(twilioWs, {
          event: "media",
          streamSid,
          media: { payload: event.delta },
        });
      }

      if (event.type === "response.audio_transcript.delta" && event.delta) {
        transcript.push(event.delta);
      }

      if (event.type === "conversation.item.input_audio_transcription.completed" && event.transcript) {
        transcript.push(`\nCaller: ${event.transcript}\n`);
      }

      if (event.type === "input_audio_buffer.speech_started" && streamSid) {
        safeSend(twilioWs, { event: "clear", streamSid });
        safeSend(openaiWs, { type: "response.cancel" });
      }

      if (event.type === "response.output_item.done" && event.item?.type === "function_call") {
        pendingToolNames.set(event.item.call_id, event.item.name);
      }

      if (event.type === "response.function_call_arguments.done") {
        const toolName = event.name || pendingToolNames.get(event.call_id);
        const args = parseToolArgs(event.arguments);
        try {
          if (toolName === "send_booking_link") {
            await postAction({ action: "send_booking_link", accountId, leadId, reason: args.reason || "" });
            safeSend(openaiWs, {
              type: "conversation.item.create",
              item: {
                type: "function_call_output",
                call_id: event.call_id,
                output: "Booking link sent.",
              },
            });
          }
          if (toolName === "finish_call") {
            await postAction({
              action: "finish_call",
              accountId,
              leadId,
              intent: args.intent || args.summary || "AI receptionist call",
              urgency: args.urgency || "normal",
              summary: args.summary || "",
              transcript: transcript.join("").trim(),
            });
            safeSend(openaiWs, {
              type: "conversation.item.create",
              item: {
                type: "function_call_output",
                call_id: event.call_id,
                output: "Call summary saved.",
              },
            });
          }
          safeSend(openaiWs, { type: "response.create" });
        } catch (error) {
          console.error("[afterring-realtime] tool failure", error);
        }
      }

      if (event.type === "error") {
        console.error("[afterring-realtime] openai error", event.error || event);
      }
    });

    openaiWs.on("error", (error) => console.error("[afterring-realtime] openai socket error", error));
  }

  twilioWs.on("message", async (raw) => {
    let event;
    try {
      event = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (event.event === "start") {
      streamSid = event.start?.streamSid || "";
      accountId = event.start?.customParameters?.accountId || "";
      leadId = event.start?.customParameters?.leadId || "";
      if (!OPENAI_API_KEY || !CRON_SECRET || !accountId || !leadId) {
        console.error("[afterring-realtime] missing required startup config");
        twilioWs.close();
        return;
      }
      connectOpenAI().catch((error) => {
        console.error("[afterring-realtime] startup failure", error);
        twilioWs.close();
      });
    }

    if (event.event === "media" && event.media?.payload) {
      safeSend(openaiWs, {
        type: "input_audio_buffer.append",
        audio: event.media.payload,
      });
    }

    if (event.event === "stop") {
      if (accountId && leadId) {
        postAction({
          action: "finish_call",
          accountId,
          leadId,
          intent: "AI receptionist call ended",
          transcript: transcript.join("").trim(),
        }).catch((error) => console.error("[afterring-realtime] stop summary failure", error));
      }
      openaiWs?.close();
    }
  });

  twilioWs.on("close", () => openaiWs?.close());
  twilioWs.on("error", (error) => console.error("[afterring-realtime] twilio socket error", error));
});

server.listen(PORT, () => {
  console.log(`[afterring-realtime] listening on :${PORT}`);
});
