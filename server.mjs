import http from "node:http";
import { URL } from "node:url";
import WebSocket, { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 8787);
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
const GEMINI_MODEL = process.env.GEMINI_MODEL || "models/gemini-2.0-flash-live-001";
const GEMINI_VOICE = process.env.GEMINI_VOICE || "Puck";
const APP_BASE_URL = (process.env.AFTER_RING_PUBLIC_BASE_URL || "http://localhost:3040").replace(/\/$/, "");
const CRON_SECRET = process.env.CRON_SECRET || "";

// --- Audio conversion (Twilio ↔ Gemini) ---
// Twilio sends/receives: µ-law 8 kHz
// Gemini expects/returns: PCM16 16 kHz (in) / 24 kHz (out)

function ulawToLinear(b) {
  b = ~b & 0xff;
  const sign = b & 0x80 ? -1 : 1;
  const exp = (b >> 4) & 0x07;
  const mantissa = b & 0x0f;
  return sign * (((mantissa << 3) + 0x84) << exp) - sign * 0x84;
}

function linearToUlaw(s) {
  const BIAS = 0x84, CLIP = 32635;
  const sign = s < 0 ? 0x80 : 0;
  if (s < 0) s = -s;
  if (s > CLIP) s = CLIP;
  s += BIAS;
  let exp = 7;
  for (let m = 0x4000; (s & m) === 0 && exp > 0; exp--, m >>= 1) {}
  return (~(sign | (exp << 4) | ((s >> (exp + 3)) & 0x0f))) & 0xff;
}

function ulawBufToPcm16Buf(buf) {
  const out = new Int16Array(buf.length);
  for (let i = 0; i < buf.length; i++) out[i] = ulawToLinear(buf[i]);
  return Buffer.from(out.buffer);
}

function pcm16BufToUlawBuf(buf) {
  const samples = new Int16Array(buf.buffer, buf.byteOffset, buf.length >> 1);
  const out = Buffer.alloc(samples.length);
  for (let i = 0; i < samples.length; i++) out[i] = linearToUlaw(samples[i]);
  return out;
}

// 8 kHz → 16 kHz (linear interpolation)
function upsample8to16(buf) {
  const s = new Int16Array(buf.buffer, buf.byteOffset, buf.length >> 1);
  const out = new Int16Array(s.length * 2);
  for (let i = 0; i < s.length; i++) {
    out[i * 2] = s[i];
    out[i * 2 + 1] = i + 1 < s.length ? ((s[i] + s[i + 1]) >> 1) : s[i];
  }
  return Buffer.from(out.buffer);
}

// 24 kHz → 8 kHz (average every 3 samples)
function downsample24to8(buf) {
  const s = new Int16Array(buf.buffer, buf.byteOffset, buf.length >> 1);
  const outLen = Math.floor(s.length / 3);
  const out = new Int16Array(outLen);
  for (let i = 0; i < outLen; i++) {
    out[i] = Math.round((s[i * 3] + (s[i * 3 + 1] || 0) + (s[i * 3 + 2] || 0)) / 3);
  }
  return Buffer.from(out.buffer);
}

// --- HTTP helpers ---
function json(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  res.end(JSON.stringify(body));
}

async function fetchSession(accountId) {
  const url = new URL("/api/ai-receptionist/session", APP_BASE_URL);
  url.searchParams.set("accountId", accountId);
  const res = await fetch(url, { headers: { Authorization: `Bearer ${CRON_SECRET}` } });
  if (!res.ok) throw new Error(`Session fetch failed: ${res.status}`);
  return res.json();
}

async function postAction(payload) {
  const res = await fetch(new URL("/api/ai-receptionist/actions", APP_BASE_URL), {
    method: "POST",
    headers: { Authorization: `Bearer ${CRON_SECRET}`, "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`Action failed: ${res.status} ${await res.text().catch(() => "")}`);
  return res.json();
}

function safeSend(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN)
    ws.send(typeof payload === "string" ? payload : JSON.stringify(payload));
}

// --- HTTP server ---
const server = http.createServer((req, res) => {
  if (req.url === "/health")
    return json(res, 200, { ok: true, model: GEMINI_MODEL, hasGemini: Boolean(GEMINI_API_KEY), hasAppSecret: Boolean(CRON_SECRET) });
  json(res, 404, { error: "Not found" });
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  if (url.pathname !== "/twilio/realtime") { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
});

// --- Per-call handler ---
wss.on("connection", (twilioWs) => {
  let geminiWs;
  let accountId = "", leadId = "", streamSid = "";
  let transcript = [];

  async function connectGemini() {
    const session = await fetchSession(accountId);
    const geminiUrl = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;
    geminiWs = new WebSocket(geminiUrl);

    geminiWs.on("open", () => {
      safeSend(geminiWs, {
        setup: {
          model: GEMINI_MODEL,
          generationConfig: {
            responseModalities: ["AUDIO"],
            speechConfig: { voiceConfig: { prebuiltVoiceConfig: { voiceName: GEMINI_VOICE } } },
          },
          systemInstruction: { parts: [{ text: session.instructions }] },
          tools: [{
            functionDeclarations: [
              {
                name: "send_booking_link",
                description: "Text the caller the booking link when they want to book or ask for the link.",
                parameters: { type: "OBJECT", properties: { reason: { type: "STRING" } } },
              },
              {
                name: "finish_call",
                description: "Call at the end of every call. Write one specific sentence about what the caller needed — e.g. 'Wants a trim Saturday morning, asked about kids pricing.' Never write generic phrases.",
                parameters: {
                  type: "OBJECT",
                  properties: {
                    intent: { type: "STRING" },
                    urgency: { type: "STRING" },
                    summary: { type: "STRING" },
                  },
                  required: ["summary"],
                },
              },
            ],
          }],
        },
      });
    });

    geminiWs.on("message", async (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }

      // Gemini signals it's ready — send the greeting trigger
      if (msg.setupComplete) {
        safeSend(geminiWs, {
          clientContent: {
            turns: [{ role: "user", parts: [{ text: "The phone call just connected. Greet the caller now." }] }],
            turnComplete: true,
          },
        });
      }

      // Audio chunks → convert and forward to Twilio
      if (msg.serverContent?.modelTurn?.parts && streamSid) {
        for (const part of msg.serverContent.modelTurn.parts) {
          if (part.inlineData?.mimeType?.startsWith("audio/pcm") && part.inlineData.data) {
            const pcm24 = Buffer.from(part.inlineData.data, "base64");
            const ulaw = pcm16BufToUlawBuf(downsample24to8(pcm24));
            safeSend(twilioWs, { event: "media", streamSid, media: { payload: ulaw.toString("base64") } });
          }
          if (part.text) transcript.push(part.text);
        }
      }

      // Caller transcript
      if (msg.serverContent?.inputTranscription)
        transcript.push(`\nCaller: ${msg.serverContent.inputTranscription}\n`);

      // Barge-in: caller started talking — clear Twilio's audio buffer
      if (msg.serverContent?.interrupted && streamSid)
        safeSend(twilioWs, { event: "clear", streamSid });

      // Tool calls
      if (msg.toolCall?.functionCalls) {
        for (const call of msg.toolCall.functionCalls) {
          const args = call.args || {};
          try {
            if (call.name === "send_booking_link")
              await postAction({ action: "send_booking_link", accountId, leadId, reason: args.reason || "" });
            if (call.name === "finish_call")
              await postAction({ action: "finish_call", accountId, leadId, intent: args.intent || args.summary || "AI call", urgency: args.urgency || "normal", summary: args.summary || "", transcript: transcript.join("").trim() });
            safeSend(geminiWs, { toolResponse: { functionResponses: [{ id: call.id, name: call.name, response: { output: "Done." } }] } });
          } catch (err) { console.error("[afterring] tool error", err); }
        }
      }

      if (msg.error) console.error("[afterring] gemini error", msg.error);
    });

    geminiWs.on("error", (err) => console.error("[afterring] gemini socket error", err));
  }

  twilioWs.on("message", async (raw) => {
    let event;
    try { event = JSON.parse(raw.toString()); } catch { return; }

    if (event.event === "start") {
      streamSid = event.start?.streamSid || "";
      accountId = event.start?.customParameters?.accountId || "";
      leadId = event.start?.customParameters?.leadId || "";
      if (!GEMINI_API_KEY || !CRON_SECRET || !accountId || !leadId) {
        console.error("[afterring] missing config");
        twilioWs.close();
        return;
      }
      connectGemini().catch((err) => { console.error("[afterring] startup failure", err); twilioWs.close(); });
    }

    if (event.event === "media" && event.media?.payload && geminiWs?.readyState === WebSocket.OPEN) {
      // µ-law 8kHz → PCM16 16kHz → Gemini
      const ulaw = Buffer.from(event.media.payload, "base64");
      const pcm16 = upsample8to16(ulawBufToPcm16Buf(ulaw));
      safeSend(geminiWs, { realtimeInput: { mediaChunks: [{ mimeType: "audio/pcm;rate=16000", data: pcm16.toString("base64") }] } });
    }

    if (event.event === "stop") {
      if (accountId && leadId)
        postAction({ action: "finish_call", accountId, leadId, intent: "AI receptionist call ended", transcript: transcript.join("").trim() })
          .catch((err) => console.error("[afterring] stop summary failure", err));
      geminiWs?.close();
    }
  });

  twilioWs.on("close", () => geminiWs?.close());
  twilioWs.on("error", (err) => console.error("[afterring] twilio socket error", err));
});

server.listen(PORT, () => console.log(`[afterring] listening on :${PORT}`));
