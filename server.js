"use strict";
/* ============================================================================
   BOT PANEL — server.js  (নতুন করে সম্পূর্ণ পুনর্নির্মিত)
   এক ফাইলেই সব: Express সার্ভার + প্যানেল UI (HTML/CSS/JS) + বট প্রসেস ম্যানেজার
   স্পেসিফিকেশন অনুযায়ী: BOT_PANEL_SPECIFICATION.md (সব ১৫টা সেকশন কভার করা হয়েছে)
   ========================================================================== */

const express     = require("express");
const session      = require("express-session");
const path          = require("path");
const fs            = require("fs");
const fsp            = fs.promises;
const os            = require("os");
const crypto        = require("crypto");
const http          = require("http");
const { WebSocketServer } = require("ws");
const { fork, spawn } = require("child_process");
const archiver      = require("archiver");
const cookieParser  = require("cookie-parser");
const multer        = require("multer");

// ---------------------------------------------------------------------------
// ০. কনফিগ (env variable থেকে, না থাকলে ডিফল্ট)
// ---------------------------------------------------------------------------
const CONFIG = {
  PORT:            process.env.PORT || 3000,
  PANEL_PASSWORD:  process.env.PANEL_PASSWORD || "changeme123",
  SESSION_SECRET:  process.env.SESSION_SECRET || crypto.randomBytes(24).toString("hex"),
  MONGODB_URI:     process.env.MONGODB_URI || "",
  BOT_DIR:         path.resolve(process.env.BOT_DIR || path.join(__dirname, "bot")),
  BOT_ENTRY:       process.env.BOT_ENTRY || "index.js",
  GITHUB_REPO:     process.env.GITHUB_REPO || "",
  GITHUB_BRANCH:   process.env.GITHUB_BRANCH || "main",
  GITHUB_ZIP_PATH: process.env.GITHUB_ZIP_PATH || "bot.zip",
  GITHUB_TOKEN:    process.env.GITHUB_TOKEN || "",
  RENDER_API_KEY:  process.env.RENDER_API_KEY || "",
  RENDER_SERVICE_ID: process.env.RENDER_SERVICE_ID || "",
  MEM_LIMIT_MB:    512,
  RESTART_RAM_THRESHOLD_MB: 470,
  DAILY_RESTART_HOUR: process.env.DAILY_RESTART_HOUR ? Number(process.env.DAILY_RESTART_HOUR) : null, // 0-23, null = off
  BUILD_VERSION:   "panel-v1.0.0-" + new Date().toISOString().slice(0, 10),
};

const DEV_INFO = {
  name: process.env.DEV_NAME || "Belal YT",
  role: process.env.DEV_ROLE || "Bot Developer",
  contact: process.env.DEV_CONTACT || "facebook.com/",
  brand: process.env.DEV_BRAND || "BELAL BOTX666",
};

// GitHub লিংক/owner-repo যেকোনো ফরম্যাটে দিলেও ঠিকভাবে বের করে নেয়
function normalizeGithubRepo(input) {
  if (!input) return "";
  let v = String(input).trim();
  v = v.replace(/^https?:\/\/(www\.)?github\.com\//i, "");
  v = v.replace(/\.git$/i, "");
  v = v.replace(/\/+$/, "");
  return v;
}

// এই ফিল্ডগুলো env variable না দিলে প্যানেলের "⚙️ সেটিংস" পেজ থেকে MongoDB-তে সেভ করা যায় —
// এতে Render-এ বারবার গিয়ে Environment Variable এডিট করতে হয় না
const MONGO_OVERRIDABLE_KEYS = [
  "GITHUB_REPO", "GITHUB_BRANCH", "GITHUB_ZIP_PATH", "GITHUB_TOKEN",
  "RENDER_API_KEY", "RENDER_SERVICE_ID", "DAILY_RESTART_HOUR",
];
async function loadSettingsFromMongo() {
  if (!mongoDb) return;
  try {
    const doc = await mongoDb.collection("state").findOne({ _id: "settings" });
    if (!doc) return;
    MONGO_OVERRIDABLE_KEYS.forEach((k) => {
      if (!process.env[k] && doc[k] !== undefined && doc[k] !== "") CONFIG[k] = doc[k];
    });
    if (doc.DEV_INFO) Object.assign(DEV_INFO, doc.DEV_INFO);
    pushLog("info", "⚙️ MongoDB থেকে সেটিংস লোড হয়েছে (GitHub repo, ইত্যাদি)।");
  } catch (e) {
    pushLog("warning", "সেটিংস লোড ব্যর্থ: " + e.message);
  }
}
async function saveSettingsToMongo(fields) {
  if (!mongoDb) return { ok: false, msg: "MongoDB কনফিগার করা নেই — আগে MONGODB_URI বসান" };
  try {
    const update = {};
    if (fields.GITHUB_REPO !== undefined) { update.GITHUB_REPO = normalizeGithubRepo(fields.GITHUB_REPO); CONFIG.GITHUB_REPO = update.GITHUB_REPO; }
    if (fields.GITHUB_BRANCH) { update.GITHUB_BRANCH = fields.GITHUB_BRANCH.trim(); CONFIG.GITHUB_BRANCH = update.GITHUB_BRANCH; }
    if (fields.GITHUB_ZIP_PATH) { update.GITHUB_ZIP_PATH = fields.GITHUB_ZIP_PATH.trim(); CONFIG.GITHUB_ZIP_PATH = update.GITHUB_ZIP_PATH; }
    if (fields.GITHUB_TOKEN !== undefined) { update.GITHUB_TOKEN = fields.GITHUB_TOKEN.trim(); CONFIG.GITHUB_TOKEN = update.GITHUB_TOKEN; }
    if (fields.DAILY_RESTART_HOUR !== undefined) {
      const n = fields.DAILY_RESTART_HOUR === "" ? null : Number(fields.DAILY_RESTART_HOUR);
      update.DAILY_RESTART_HOUR = n; CONFIG.DAILY_RESTART_HOUR = n;
    }
    if (fields.DEV_INFO) { update.DEV_INFO = fields.DEV_INFO; Object.assign(DEV_INFO, fields.DEV_INFO); }
    await mongoDb.collection("state").updateOne({ _id: "settings" }, { $set: update }, { upsert: true });
    pushLog("success", "✅ সেটিংস সেভ হয়েছে।");
    return { ok: true };
  } catch (e) {
    return { ok: false, msg: e.message };
  }
}

// ---------------------------------------------------------------------------
// ১. স্টেট (মেমোরিতে, MongoDB দিয়ে ব্যাকআপ)
// ---------------------------------------------------------------------------
const STATE = {
  botProcess: null,
  botStatus: "offline",     // offline | booting | online
  botPid: null,
  startedAt: null,          // এই রানের শুরুর সময়
  serverStartedAt: Date.now(),
  logs: [],                 // { ts, type, text }
  MAX_LOGS: 2000,
  notifications: [],        // { ts, type, text, read }
  notifCooldowns: {},       // type -> lastTs
  restartHistory: [],       // { ts, uptimeMs, exitCode, reason }
  lifetime: {
    totalStarts: 0,
    totalCrashes: 0,
    totalUptimeMs: 0,
    trackingSince: Date.now(),
  },
  panelRamMax: 0,
  botRamMax: 0,
  mongoStorageMax: 0,
  heavyDownloadCount: 0,
  MAX_HEAVY: 2,
  heavyQueue: [],
  npmInstallLock: false,
  npmInstallStartedAt: null,
  backoffAttempt: 0,
  autoRestartEnabled: true, // স্পেসিফিকেশন অনুযায়ী সবসময় ON, বন্ধ করার UI নেই
  wantBotRunning: false,    // ব্যবহারকারী চালু রাখতে চেয়েছিল কিনা (persist)
  netPrev: null,
};

// ---------------------------------------------------------------------------
// ২. Mongo (ঐচ্ছিক — MONGODB_URI না থাকলে সব gracefully স্কিপ হয়)
// ---------------------------------------------------------------------------
let mongoClient = null, mongoDb = null;
async function mongoConnect() {
  if (!CONFIG.MONGODB_URI) {
    pushLog("warning", "MONGODB_URI সেট নেই — MongoDB ব্যাকআপ বন্ধ থাকবে (লোকাল মেমোরি/ডিস্কেই থাকবে)।");
    return { ok: false, msg: "MONGODB_URI সেট নেই", hint: "Render Environment ট্যাবে MONGODB_URI বসান" };
  }
  try {
    if (mongoClient) { try { await mongoClient.close(); } catch {} }
    const { MongoClient } = require("mongodb");
    mongoClient = new MongoClient(CONFIG.MONGODB_URI, { serverSelectionTimeoutMS: 8000 });
    await mongoClient.connect();
    await mongoClient.db("admin").command({ ping: 1 }); // সত্যিই কানেক্ট হয়েছে কিনা নিশ্চিত করতে ping
    mongoDb = mongoClient.db("bot_panel");
    pushLog("success", "✅ MongoDB Atlas সংযুক্ত হয়েছে।");
    return { ok: true };
  } catch (e) {
    mongoDb = null;
    const msg = e.message || String(e);
    let hint = "কারণ নির্দিষ্ট করা যায়নি — নিচের msg দেখে চেক করুন।";
    if (/bad auth|authentication failed/i.test(msg)) {
      hint = "ইউজারনেম/পাসওয়ার্ড ভুল — MongoDB Atlas-এ Database Access-এ গিয়ে পাসওয়ার্ড আবার চেক/রিসেট করুন। পাসওয়ার্ডে বিশেষ ক্যারেক্টার (@ # ইত্যাদি) থাকলে URL-encode করা হয়েছে কিনা দেখুন।";
    } else if (/whitelist|ip address|network|ETIMEDOUT|querySrv|ENOTFOUND|serverSelectionTimeoutMS/i.test(msg)) {
      hint = "সবচেয়ে সম্ভাব্য কারণ: MongoDB Atlas-এর Network Access-এ 0.0.0.0/0 (Allow access from anywhere) যোগ করা হয়নি — Atlas → Network Access → Add IP Address → Allow Access from Anywhere।";
    } else if (/Invalid connection string|invalid scheme/i.test(msg)) {
      hint = "MONGODB_URI ফরম্যাট ভুল — mongodb+srv:// দিয়ে শুরু হচ্ছে কিনা, এবং পুরো স্ট্রিং ঠিকমতো কপি হয়েছে কিনা চেক করুন।";
    }
    pushLog("error", `❌ MongoDB সংযোগ ব্যর্থ: ${msg}`);
    return { ok: false, msg, hint };
  }
}

async function mongoSaveState() {
  if (!mongoDb) return;
  try {
    await mongoDb.collection("state").updateOne(
      { _id: "lifetime" },
      { $set: { ...STATE.lifetime, restartHistory: STATE.restartHistory.slice(-100) } },
      { upsert: true }
    );
    await mongoDb.collection("state").updateOne(
      { _id: "notifications" },
      { $set: { items: STATE.notifications.slice(-300) } },
      { upsert: true }
    );
  } catch (e) { /* silent — MongoDB সাময়িক ডাউন হলে প্যানেল থেমে থাকবে না */ }
}

async function mongoRestoreState() {
  if (!mongoDb) return;
  try {
    const lifetime = await mongoDb.collection("state").findOne({ _id: "lifetime" });
    if (lifetime) {
      STATE.lifetime.totalStarts    = lifetime.totalStarts    || 0;
      STATE.lifetime.totalCrashes   = lifetime.totalCrashes   || 0;
      STATE.lifetime.totalUptimeMs  = lifetime.totalUptimeMs  || 0;
      STATE.lifetime.trackingSince  = lifetime.trackingSince  || Date.now();
      STATE.restartHistory          = lifetime.restartHistory || [];
    }
    const notif = await mongoDb.collection("state").findOne({ _id: "notifications" });
    if (notif) STATE.notifications = notif.items || [];
    pushLog("info", "☁️ MongoDB থেকে লাইফটাইম ডেটা রিস্টোর করা হয়েছে।");
  } catch (e) {
    pushLog("warning", "MongoDB রিস্টোর ব্যর্থ: " + e.message);
  }
}

// বট-ফাইল ব্যাকআপ (node_modules বাদে)
async function mongoBackupFiles() {
  if (!mongoDb) return { ok: false, msg: "MongoDB কনফিগার করা নেই" };
  try {
    const files = await walkFiles(CONFIG.BOT_DIR, CONFIG.BOT_DIR);
    const bulk = files
      .filter(f => !f.rel.split(path.sep).includes("node_modules"))
      .map(f => ({
        updateOne: {
          filter: { _id: f.rel },
          update: { $set: { content: f.content, mtime: Date.now() } },
          upsert: true,
        },
      }));
    if (bulk.length) await mongoDb.collection("bot_files").bulkWrite(bulk);
    pushLog("success", `💾 ${bulk.length}টা ফাইল MongoDB-তে ব্যাকআপ হয়েছে।`);
    return { ok: true, count: bulk.length };
  } catch (e) {
    pushLog("error", "ব্যাকআপ ব্যর্থ: " + e.message);
    return { ok: false, msg: e.message };
  }
}

async function mongoRestoreFiles() {
  if (!mongoDb) return { ok: false, msg: "MongoDB কনফিগার করা নেই" };
  try {
    const docs = await mongoDb.collection("bot_files").find({}).toArray();
    for (const d of docs) {
      const full = path.join(CONFIG.BOT_DIR, d._id);
      await fsp.mkdir(path.dirname(full), { recursive: true });
      await fsp.writeFile(full, d.content, "utf8");
    }
    pushLog("success", `☁️ ${docs.length}টা ফাইল MongoDB থেকে রিস্টোর হয়েছে।`);
    return { ok: true, count: docs.length };
  } catch (e) {
    pushLog("error", "রিস্টোর ব্যর্থ: " + e.message);
    return { ok: false, msg: e.message };
  }
}

// zip এক্সট্র্যাক্ট করার পর যদি সবকিছু একটামাত্র র‍্যাপার ফোল্ডারের ভেতরে থাকে (যেমন GitHub zip-এ
// "reponame-main/" বা কারো নিজের "bot/" ফোল্ডার), সেটার ভেতরের সবকিছু এক ধাপ উপরে তুলে আনে —
// নাহলে bot/bot/index.js এর মতো ডাবল-নেস্টেড পাথ তৈরি হয়ে বট চালু হতে ব্যর্থ হয়
async function flattenSingleRootFolder(dir) {
  let changed = false;
  for (let depth = 0; depth < 3; depth++) {
    let entries;
    try { entries = await fsp.readdir(dir, { withFileTypes: true }); } catch { return changed; }
    const visible = entries.filter((e) => e.name !== "__MACOSX" && !e.name.startsWith("."));
    if (visible.length !== 1 || !visible[0].isDirectory()) return changed; // একাধিক আইটেম বা সরাসরি ফাইল থাকলে flatten দরকার নেই
    const wrapperName = visible[0].name;
    const wrapperPath = path.join(dir, wrapperName);
    const inner = await fsp.readdir(wrapperPath, { withFileTypes: true });
    for (const item of inner) {
      await fsp.rename(path.join(wrapperPath, item.name), path.join(dir, item.name));
    }
    await fsp.rmdir(wrapperPath).catch(() => {});
    pushLog("info", `📦 zip-এর ভেতরের "${wrapperName}" র‍্যাপার ফোল্ডার সরিয়ে ফাইলগুলো রুটে আনা হলো।`);
    changed = true;
  }
  return changed;
}

// এন্ট্রি ফাইল (index.js) খুঁজে না পেলে ডিরেক্টরির আসল অবস্থা লগে দেখিয়ে দেয় —
// পরের বার screenshot চালাচালি না করেই কারণ বোঝা যায়
async function logBotDirStructureIfEntryMissing() {
  if (fs.existsSync(getBotEntryPath())) return;
  try {
    const entries = await fsp.readdir(CONFIG.BOT_DIR, { withFileTypes: true });
    const listing = entries.slice(0, 20).map((e) => (e.isDirectory() ? e.name + "/" : e.name)).join(", ") || "(খালি)";
    pushLog("error", `⚠️ ${CONFIG.BOT_ENTRY} পাওয়া যায়নি "${CONFIG.BOT_DIR}"-এ। ভেতরে যা আছে: ${listing}`);
  } catch (e) {
    pushLog("error", `⚠️ ${CONFIG.BOT_DIR} ফোল্ডার পড়া যায়নি: ${e.message}`);
  }
}

async function walkFiles(dir, base) {
  let out = [];
  let entries;
  try { entries = await fsp.readdir(dir, { withFileTypes: true }); } catch { return out; }
  for (const e of entries) {
    if (e.name === "node_modules" || e.name === ".git") continue;
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      out = out.concat(await walkFiles(full, base));
    } else {
      try {
        const stat = await fsp.stat(full);
        if (stat.size > 2 * 1024 * 1024) continue; // ২MB এর বড় ফাইল স্কিপ (Mongo doc সীমা)
        const content = await fsp.readFile(full, "utf8").catch(() => null);
        if (content !== null) out.push({ rel: path.relative(base, full), content });
      } catch { /* skip */ }
    }
  }
  return out;
}

// GitHub রিপোতে রাখা bot.zip — শুধু প্রথমবার (MongoDB-তে কোনো ফাইল ব্যাকআপ না থাকলে) ইম্পোর্ট হয়,
// এরপর MongoDB-ই সোর্স অফ ট্রুথ — GitHub থেকে zip মুছে দিলেও কোনো সমস্যা নেই
async function githubImportZipIfNeeded(force) {
  if (!mongoDb) {
    pushLog("warning", "MongoDB কানেক্টেড না — GitHub auto-import নিরাপদে স্কিপ করা হলো (duplicate-import ট্র্যাক করা যাবে না)।");
    return { ok: false, msg: "MongoDB কানেক্টেড না" };
  }
  const existingCount = await mongoDb.collection("bot_files").countDocuments().catch(() => 0);
  if (existingCount > 0 && !force) {
    pushLog("info", "MongoDB-তে আগে থেকেই বট ফাইল ব্যাকআপ আছে — GitHub স্কিপ, MongoDB থেকেই ডিস্কে রিস্টোর করা হচ্ছে (Mongo-ই সোর্স অফ ট্রুথ, ডিস্ক ephemeral)।");
    await mongoRestoreFiles();
    // পুরনো ব্যাকআপ যদি ভুল/ডাবল-নেস্টেড স্ট্রাকচারে সেভ হয়ে থাকে, এখানে ঠিক করে আবার Mongo-তেও ফিক্স করে ফেলা হয়
    const fixed = await flattenSingleRootFolder(CONFIG.BOT_DIR);
    if (fixed) {
      pushLog("warning", "🔧 আগের ব্যাকআপে ডাবল-নেস্টেড ফোল্ডার সমস্যা পাওয়া গেছে — ঠিক করে MongoDB-তে আবার সেভ করা হচ্ছে।");
      await mongoBackupFiles();
    }
    await logBotDirStructureIfEntryMissing();
    return { ok: true, restored: true, fixed };
  }
  if (!CONFIG.GITHUB_REPO) {
    pushLog("info", "GITHUB_REPO সেট নেই এবং MongoDB-তেও কোনো ফাইল নেই — বট শুরু থেকে খালি থাকবে।");
    return { ok: false, msg: "GITHUB_REPO সেট নেই — আগে ⚙️ সেটিংস থেকে বসান" };
  }
  try {
    const axios = require("axios");
    const url = `https://api.github.com/repos/${CONFIG.GITHUB_REPO}/contents/${CONFIG.GITHUB_ZIP_PATH}`;
    const headers = { Accept: "application/vnd.github.raw" };
    if (CONFIG.GITHUB_TOKEN) headers.Authorization = `token ${CONFIG.GITHUB_TOKEN}`;
    pushLog("info", `📥 GitHub থেকে ${CONFIG.GITHUB_ZIP_PATH} (${CONFIG.GITHUB_REPO}@${CONFIG.GITHUB_BRANCH}) ডাউনলোড হচ্ছে...`);
    const res = await axios.get(url, {
      headers, params: { ref: CONFIG.GITHUB_BRANCH },
      responseType: "arraybuffer", timeout: 30000, maxContentLength: 200 * 1024 * 1024,
    });
    const tmpZip = path.join(os.tmpdir(), "github_bot_import.zip");
    await fsp.writeFile(tmpZip, res.data);

    const AdmZip = require("adm-zip");
    const zip = new AdmZip(tmpZip);
    await fsp.mkdir(CONFIG.BOT_DIR, { recursive: true });
    zip.extractAllTo(CONFIG.BOT_DIR, true);
    await fsp.unlink(tmpZip).catch(() => {});
    await flattenSingleRootFolder(CONFIG.BOT_DIR);
    pushLog("success", "✅ GitHub zip এক্সট্র্যাক্ট হয়েছে বট ফোল্ডারে।");
    await logBotDirStructureIfEntryMissing();

    const backup = await mongoBackupFiles(); // এখন থেকে MongoDB-ই সোর্স অফ ট্রুথ
    await mongoDb.collection("state").updateOne(
      { _id: "github_import" },
      { $set: { importedAt: Date.now(), path: CONFIG.GITHUB_ZIP_PATH, branch: CONFIG.GITHUB_BRANCH } },
      { upsert: true }
    );
    pushNotification("github-import", `GitHub zip ইম্পোর্ট ও MongoDB ব্যাকআপ সম্পন্ন (${backup.count || 0} ফাইল)। এখন GitHub থেকে zip মুছে ফেললেও সমস্যা নেই।`);
    return { ok: true, count: backup.count || 0 };
  } catch (e) {
    const msg = e.response ? `HTTP ${e.response.status}` : e.message;
    pushLog("error", `❌ GitHub zip ইম্পোর্ট ব্যর্থ: ${msg} (repo/path/token ঠিক আছে কিনা চেক করুন)`);
    return { ok: false, msg };
  }
}

// ---------------------------------------------------------------------------
// ৩. লগ + নোটিফিকেশন হেল্পার
// ---------------------------------------------------------------------------
const NOISE_PATTERNS = [/mqtt/i, /fca-unofficial.*stack/i, /at Socket\./, /at process\.processTicksAndRejections/];
const SEVERITY_OVERRIDE = [
  { re: /\[সতর্ক\]|\bwarn(ing)?\b/i, type: "warning" },
  { re: /\[এরর\]|\berror\b|❌/i, type: "error" },
  { re: /\[সফল\]|success|✅/i, type: "success" },
];

function pushLog(type, text) {
  if (NOISE_PATTERNS.some(re => re.test(text))) return; // internal noise বাদ
  let finalType = type;
  for (const rule of SEVERITY_OVERRIDE) {
    if (rule.re.test(text)) { finalType = rule.type; break; }
  }
  const entry = { ts: Date.now(), type: finalType, text: String(text) };
  STATE.logs.push(entry);
  if (STATE.logs.length > STATE.MAX_LOGS) STATE.logs.shift();
  broadcast({ kind: "log", entry });
}

function pushNotification(type, text) {
  const COOLDOWN_MS = 5 * 60 * 1000;
  const last = STATE.notifCooldowns[type] || 0;
  if (Date.now() - last < COOLDOWN_MS) return; // স্প্যাম বন্ধ
  STATE.notifCooldowns[type] = Date.now();
  const entry = { ts: Date.now(), type, text, read: false };
  STATE.notifications.push(entry);
  if (STATE.notifications.length > 500) STATE.notifications.shift();
  broadcast({ kind: "notification", entry });
  mongoSaveState();
}

// ---------------------------------------------------------------------------
// ৪. বট প্রসেস ম্যানেজার (fork + IPC)
// ---------------------------------------------------------------------------
function getBotEntryPath() {
  return path.resolve(CONFIG.BOT_DIR, CONFIG.BOT_ENTRY);
}

function startBot(reason = "manual") {
  if (STATE.botProcess) {
    pushLog("warning", "বট আগে থেকেই চলছে।");
    return;
  }
  if (!fs.existsSync(getBotEntryPath())) {
    pushLog("error", `❌ বট এন্ট্রি ফাইল পাওয়া যায়নি: ${CONFIG.BOT_ENTRY} (পাথ: ${getBotEntryPath()})`);
    logBotDirStructureIfEntryMissing();
    STATE.wantBotRunning = false; // এন্ট্রি ফাইল না থাকলে বারবার ব্যর্থ auto-restart loop বন্ধ রাখা হলো
    STATE.botStatus = "offline";
    broadcast({ kind: "status", status: STATE.botStatus });
    return;
  }
  STATE.botStatus = "booting";
  STATE.wantBotRunning = true;
  pushLog("info", `▶ বট চালু হচ্ছে... (কারণ: ${reason})`);
  broadcast({ kind: "status", status: STATE.botStatus });

  const child = fork(getBotEntryPath(), [], {
    cwd: CONFIG.BOT_DIR,
    silent: true,
    env: { ...process.env },
  });

  STATE.botProcess = child;
  STATE.botPid = child.pid;
  STATE.startedAt = Date.now();
  STATE.lifetime.totalStarts += 1;

  child.stdout && child.stdout.on("data", (d) => pushLog("info", d.toString().trim()));
  child.stderr && child.stderr.on("data", (d) => pushLog("error", d.toString().trim()));

  child.on("message", (msg) => handleBotIpcMessage(msg));

  child.on("exit", (code, signal) => {
    const uptimeMs = STATE.startedAt ? Date.now() - STATE.startedAt : 0;
    STATE.lifetime.totalUptimeMs += uptimeMs;
    const wasCrash = code !== 0 && code !== null;
    if (wasCrash) STATE.lifetime.totalCrashes += 1;

    STATE.restartHistory.push({ ts: Date.now(), uptimeMs, exitCode: code, signal, reason: wasCrash ? "crash" : "stopped" });
    if (STATE.restartHistory.length > 200) STATE.restartHistory.shift();

    STATE.botProcess = null;
    STATE.botPid = null;
    STATE.botStatus = "offline";
    broadcast({ kind: "status", status: STATE.botStatus });
    mongoSaveState();

    pushLog(wasCrash ? "error" : "warning", `⏹ বট বন্ধ হলো (exit code: ${code}, uptime: ${formatDuration(uptimeMs)})`);

    if (wasCrash) {
      pushNotification("crash", `বট ক্র্যাশ করেছে (exit ${code}), ${formatDuration(uptimeMs)} চালু ছিল। Auto-restart হচ্ছে...`);
    }

    if (STATE.autoRestartEnabled && STATE.wantBotRunning) {
      scheduleAutoRestart(wasCrash);
    }
  });

  child.on("error", (err) => pushLog("error", "বট প্রসেস এরর: " + err.message));
}

function scheduleAutoRestart(wasCrash) {
  // Exponential backoff: ১০সে থেকে সর্বোচ্চ ৫মিনিট
  STATE.backoffAttempt = wasCrash ? STATE.backoffAttempt + 1 : 0;
  const delaySec = Math.min(10 * Math.pow(2, STATE.backoffAttempt), 300);
  pushLog("info", `🔁 ${delaySec} সেকেন্ড পরে বট আবার চালু হবে (backoff attempt #${STATE.backoffAttempt})।`);
  setTimeout(() => {
    if (STATE.wantBotRunning && !STATE.botProcess) startBot("auto-restart");
  }, delaySec * 1000);
}

function stopBot(reason = "manual") {
  STATE.wantBotRunning = false;
  if (!STATE.botProcess) {
    pushLog("warning", "বট এমনিতেই বন্ধ আছে।");
    return;
  }
  pushLog("info", `⏹ বট বন্ধ করা হচ্ছে... (কারণ: ${reason})`);
  STATE.botProcess.kill("SIGTERM");
}

function restartBot(reason = "manual") {
  pushLog("info", `🔄 বট রিস্টার্ট শুরু... (কারণ: ${reason})`);
  if (STATE.botProcess) {
    STATE.wantBotRunning = true;
    STATE.botProcess.once("exit", () => setTimeout(() => startBot("restart:" + reason), 1000));
    STATE.botProcess.kill("SIGTERM");
  } else {
    startBot("restart:" + reason);
  }
}

function handleBotIpcMessage(msg) {
  if (!msg || typeof msg !== "object") return;
  switch (msg.type) {
    case "ready":
    case "bot_ready":
      STATE.botStatus = "online";
      broadcast({ kind: "status", status: STATE.botStatus });
      pushLog("success", `✅ বট সম্পূর্ণ প্রস্তুত — কমান্ড: ${msg.commands ?? "?"}, ব্যর্থ: ${msg.failed ?? 0}`);
      break;
    case "log":
      pushLog(msg.level || "info", msg.text || "");
      break;
    case "heavy-start":
      STATE.heavyDownloadCount++;
      break;
    case "heavy-end":
      STATE.heavyDownloadCount = Math.max(0, STATE.heavyDownloadCount - 1);
      break;
    case "crash-notice":
      pushNotification("crash", msg.text || "আগের সেশনে বট ক্র্যাশ করেছিল।");
      break;
    case "file-live-ack":
      pushLog("info", `⚡ কমান্ড ফাইল লাইভ হয়েছে: ${msg.file}`);
      break;
    default:
      break;
  }
}

// ফাইল-ম্যানেজার থেকে সরাসরি বটকে IPC দিয়ে জানানো (fs.watch এর অপেক্ষা না করে)
// বট সাইড হ্যান্ডলার { type: "panel_file_change", relPath, action } আশা করে (শুধু Script/commands/*.js প্রযোজ্য)
function notifyBotFileChange(action, relPath) {
  if (STATE.botProcess && STATE.botProcess.connected) {
    STATE.botProcess.send({ type: "panel_file_change", action, relPath });
  }
}

// ---------------------------------------------------------------------------
// ৫. স্বয়ংক্রিয় স্থিতিশীলতা সিস্টেম
// ---------------------------------------------------------------------------
function getProcRamMB(pid) {
  try {
    const status = fs.readFileSync(`/proc/${pid}/status`, "utf8");
    const m = status.match(/VmRSS:\s+(\d+)\s+kB/);
    if (m) return Math.round(Number(m[1]) / 1024);
  } catch { /* /proc না থাকলে (নন-লিনাক্স) fallback */ }
  return null;
}

// প্রতিরোধমূলক RAM guard — পরপর কয়েকবার থ্রেশহোল্ড পার হলে নিরাপদ রিস্টার্ট
let ramOverCount = 0;
setInterval(() => {
  const panelRamMB = Math.round(process.memoryUsage().rss / 1024 / 1024);
  STATE.panelRamMax = Math.max(STATE.panelRamMax, panelRamMB);

  let botRamMB = null;
  if (STATE.botPid) {
    botRamMB = getProcRamMB(STATE.botPid);
    if (botRamMB != null) STATE.botRamMax = Math.max(STATE.botRamMax, botRamMB);
  }

  if (botRamMB != null && botRamMB >= CONFIG.RESTART_RAM_THRESHOLD_MB) {
    ramOverCount++;
    if (ramOverCount >= 3) { // ধারাবাহিক ৩ বার (৩x১৫সে = ৪৫সে ধরে বেশি)
      pushLog("warning", `⚠️ বটের RAM ধারাবাহিকভাবে ${botRamMB}MB+ — প্রতিরোধমূলক নিরাপদ রিস্টার্ট করা হচ্ছে।`);
      pushNotification("preventive-restart", `RAM ${botRamMB}MB ছুঁয়ে যাওয়ায় প্রতিরোধমূলক রিস্টার্ট করা হয়েছে।`);
      ramOverCount = 0;
      restartBot("preventive-ram-guard");
    }
  } else {
    ramOverCount = 0;
  }

  broadcast({
    kind: "stats",
    panelRamMB, botRamMB,
    panelRamMax: STATE.panelRamMax, botRamMax: STATE.botRamMax,
    botStatus: STATE.botStatus,
    heavyDownloadCount: STATE.heavyDownloadCount,
  });
}, 15000);

// ৬-ঘণ্টা কানেকশন-রিফ্রেশ (fca-unofficial silent MQTT death প্রতিরোধ)
setInterval(() => {
  if (STATE.botStatus === "online") {
    pushLog("info", "🔄 নিয়মিত ৬-ঘণ্টা কানেকশন-রিফ্রেশ (silent MQTT death প্রতিরোধ)।");
    pushNotification("connection-refresh", "নিয়মিত ৬-ঘণ্টা কানেকশন-রিফ্রেশ সম্পন্ন হয়েছে।");
    restartBot("6h-connection-refresh");
  }
}, 6 * 60 * 60 * 1000);

// Scheduled daily restart (ঐচ্ছিক)
setInterval(() => {
  if (CONFIG.DAILY_RESTART_HOUR == null) return;
  const now = new Date();
  if (now.getHours() === CONFIG.DAILY_RESTART_HOUR && now.getMinutes() === 0) {
    restartBot("scheduled-daily");
  }
}, 60 * 1000);

// npm install — non-blocking, lock সহ, ৫ মিনিট hang-safety
function runNpmInstall() {
  return new Promise((resolve) => {
    if (STATE.npmInstallLock) {
      pushLog("warning", "npm install ইতিমধ্যে চলছে — একসাথে দুইটা চালানো যায় না।");
      return resolve({ ok: false, msg: "already running" });
    }
    STATE.npmInstallLock = true;
    STATE.npmInstallStartedAt = Date.now();
    pushLog("info", "📦 npm install শুরু হচ্ছে...");

    const child = spawn("npm", ["install", "--no-audit", "--no-fund"], { cwd: CONFIG.BOT_DIR });
    const hangTimer = setTimeout(() => {
      pushLog("error", "❌ npm install ৫ মিনিটের বেশি সময় নিচ্ছে — বাতিল করা হলো।");
      pushNotification("npm-fail", "npm install টাইমআউট হয়েছে (৫ মিনিট)।");
      child.kill("SIGKILL");
    }, 5 * 60 * 1000);

    child.stdout.on("data", (d) => pushLog("info", d.toString().trim()));
    child.stderr.on("data", (d) => pushLog("warning", d.toString().trim()));

    child.on("exit", (code) => {
      clearTimeout(hangTimer);
      STATE.npmInstallLock = false;
      STATE.npmInstallStartedAt = null;
      if (code === 0) {
        pushLog("success", "✅ npm install সম্পন্ন হয়েছে।");
        resolve({ ok: true });
      } else {
        pushLog("error", `❌ npm install ব্যর্থ (exit ${code})।`);
        pushNotification("npm-fail", `npm install ব্যর্থ হয়েছে (exit code ${code})।`);
        resolve({ ok: false, msg: "exit " + code });
      }
    });
  });
}

// ---------------------------------------------------------------------------
// ৬. Express + সেশন + অথ
// ---------------------------------------------------------------------------
const app = express();
const server = http.createServer(app);
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

// no-cache হেডার — পুরনো cache করা পেজ যেন না দেখায়
app.use((req, res, next) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
  next();
});

app.use(session({
  secret: CONFIG.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 24 * 60 * 60 * 1000 }, // session cookie ১ দিন
}));

// দীর্ঘমেয়াদী auth টোকেন (৩০ দিন) — session হারালেও persistent auth থাকে
const LONG_TOKEN_COOKIE = "panel_auth_token";
const validLongTokens = new Set();

function issueLongToken(res) {
  const token = crypto.randomBytes(32).toString("hex");
  validLongTokens.add(token);
  res.cookie(LONG_TOKEN_COOKIE, token, {
    maxAge: 30 * 24 * 60 * 60 * 1000,
    httpOnly: true,
    sameSite: "lax",
  });
}

function requireAuth(req, res, next) {
  if (req.session && req.session.authed) return next();
  const token = req.cookies[LONG_TOKEN_COOKIE];
  if (token && validLongTokens.has(token)) {
    req.session.authed = true;
    return next();
  }
  if (req.path.startsWith("/api/")) return res.status(401).json({ ok: false, msg: "লগইন প্রয়োজন" });
  return res.redirect("/login");
}

app.get("/login", (req, res) => {
  if (req.session.authed) return res.redirect("/");
  res.send(renderLogin());
});

app.post("/login", (req, res) => {
  const { password } = req.body;
  if (password === CONFIG.PANEL_PASSWORD) {
    req.session.authed = true;
    issueLongToken(res);
    return res.redirect("/");
  }
  res.send(renderLogin("❌ ভুল পাসওয়ার্ড!"));
});

app.get("/logout", (req, res) => {
  const token = req.cookies[LONG_TOKEN_COOKIE];
  if (token) validLongTokens.delete(token);
  req.session.destroy(() => {
    res.clearCookie(LONG_TOKEN_COOKIE);
    res.redirect("/login");
  });
});

app.use(requireAuth);

// ---------------------------------------------------------------------------
// ৭. হোম পেজ + বট কন্ট্রোল API
// ---------------------------------------------------------------------------
app.get("/", (req, res) => res.send(renderHome()));

app.post("/api/bot/start", (req, res) => { startBot("panel-button"); res.json({ ok: true }); });
app.post("/api/bot/stop", (req, res) => { stopBot("panel-button"); res.json({ ok: true }); });
app.post("/api/bot/restart", (req, res) => { restartBot("panel-button"); res.json({ ok: true }); });
app.post("/api/bot/npm-install", async (req, res) => { const r = await runNpmInstall(); res.json(r); });
app.post("/api/bot/backup", async (req, res) => { const r = await mongoBackupFiles(); res.json(r); });
app.post("/api/bot/mongo-sync", async (req, res) => { await mongoRestoreState(); res.json({ ok: true }); });
app.post("/api/bot/restore", async (req, res) => {
  const r = await mongoRestoreFiles();
  const fixed = await flattenSingleRootFolder(CONFIG.BOT_DIR);
  if (fixed) await mongoBackupFiles();
  await logBotDirStructureIfEntryMissing();
  res.json({ ...r, fixed });
});

// পেস্ট করা cookie/appstate টেক্সটকে fca-unofficial এর appstate.json ফরম্যাটে রূপান্তর করে
function parseAppstateInput(raw) {
  const text = String(raw || "").trim();
  if (!text) throw new Error("খালি ইনপুট");
  // ১) ইতিমধ্যে valid JSON appstate array হলে সরাসরি ব্যবহার
  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) return parsed;
  } catch { /* JSON না — নিচে plain cookie হিসেবে পার্স করা হবে */ }
  // ২) plain cookie string ("name1=value1; name2=value2;") থেকে appstate array বানানো
  const pairs = text.split(";").map((s) => s.trim()).filter(Boolean);
  if (!pairs.length) throw new Error("চেনা যায়নি এমন ফরম্যাট — JSON array অথবা 'name=value; ...' ফরম্যাটে দিন");
  const now = Date.now();
  return pairs.map((p) => {
    const idx = p.indexOf("=");
    if (idx === -1) throw new Error(`অবৈধ কুকি অংশ: ${p}`);
    return {
      key: p.slice(0, idx).trim(),
      value: p.slice(idx + 1).trim(),
      domain: ".facebook.com",
      path: "/",
      hostOnly: false,
      creation: new Date(now).toISOString(),
      lastAccessed: new Date(now).toISOString(),
    };
  });
}

app.post("/api/bot/set-appstate", async (req, res) => {
  try {
    const appstate = parseAppstateInput(req.body.text);
    await fsp.mkdir(CONFIG.BOT_DIR, { recursive: true });
    await fsp.writeFile(path.join(CONFIG.BOT_DIR, "appstate.json"), JSON.stringify(appstate, null, 2), "utf8");
    pushLog("success", "🍪 Facebook cookie/appstate সেভ হয়েছে।");
    const fixed = await flattenSingleRootFolder(CONFIG.BOT_DIR); // পুরনো ডাবল-নেস্টেড সমস্যা থাকলে এখানেই ঠিক হয়ে যাবে
    if (fixed) pushLog("warning", "🔧 ফোল্ডার স্ট্রাকচার ঠিক করা হলো (ডাবল-নেস্টেড ফোল্ডার সরানো হয়েছে)।");
    await logBotDirStructureIfEntryMissing();
    if (mongoDb) {
      await mongoBackupFiles().catch(() => {});
    }
    pushLog("info", "▶ বট চালু করা হচ্ছে...");
    restartBot("appstate-updated");
    res.json({ ok: true, fixed });
  } catch (e) {
    res.json({ ok: false, msg: e.message });
  }
});

// সরাসরি ফোন থেকে bot.zip আপলোড (GitHub ছাড়াই)
const uploadMem = multer({ storage: multer.memoryStorage(), limits: { fileSize: 150 * 1024 * 1024 } });
app.post("/api/bot/upload-zip", uploadMem.single("zipfile"), async (req, res) => {
  try {
    if (!req.file) return res.json({ ok: false, msg: "কোনো ফাইল পাওয়া যায়নি" });
    pushLog("info", `📤 ফোন থেকে zip আপলোড হচ্ছে (${Math.round(req.file.size / 1024)} KB)...`);
    const AdmZip = require("adm-zip");
    const zip = new AdmZip(req.file.buffer);
    await fsp.mkdir(CONFIG.BOT_DIR, { recursive: true });
    zip.extractAllTo(CONFIG.BOT_DIR, true);
    await flattenSingleRootFolder(CONFIG.BOT_DIR);
    pushLog("success", "✅ আপলোড করা zip এক্সট্র্যাক্ট হয়েছে।");
    await logBotDirStructureIfEntryMissing();
    const backup = await mongoBackupFiles();
    pushNotification("manual-upload", `ফোন থেকে সরাসরি আপলোড করা bot.zip ইম্পোর্ট হয়েছে (${backup.count || 0} ফাইল)।`);
    res.json({ ok: true, count: backup.count || 0 });
  } catch (e) {
    pushLog("error", "❌ আপলোড ব্যর্থ: " + e.message);
    res.json({ ok: false, msg: e.message });
  }
});

app.get("/api/bot/status", (req, res) => {
  res.json({
    status: STATE.botStatus,
    pid: STATE.botPid,
    startedAt: STATE.startedAt,
    panelRamMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
    botRamMB: STATE.botPid ? getProcRamMB(STATE.botPid) : null,
    serverUptimeMs: Date.now() - STATE.serverStartedAt,
    botFileCount: countBotFilesSync(),
    lifetime: STATE.lifetime,
    restartHistory: STATE.restartHistory.slice(-15).reverse(),
    nodeVersion: process.version,
  });
});

function countBotFilesSync() {
  try {
    let n = 0;
    const walk = (d) => {
      for (const e of fs.readdirSync(d, { withFileTypes: true })) {
        if (e.name === "node_modules" || e.name === ".git") continue;
        const full = path.join(d, e.name);
        if (e.isDirectory()) walk(full); else n++;
      }
    };
    walk(CONFIG.BOT_DIR);
    return n;
  } catch { return 0; }
}

// ---------------------------------------------------------------------------
// ৮. ফাইল ম্যানেজার
// ---------------------------------------------------------------------------
const HIDDEN_FILES = [".crash_flag.json", ".github_import_marker.json", ".DS_Store"];
// GitHub-এর ভাষা-রঙের ধাঁচে — এক্সটেনশন অনুযায়ী রঙিন ব্যাজ (label + background + text color)
const EXT_BADGES = {
  ".js":   { label: "JS",   bg: "#f1e05a", fg: "#222" },
  ".jsx":  { label: "JSX",  bg: "#f1e05a", fg: "#222" },
  ".mjs":  { label: "JS",   bg: "#f1e05a", fg: "#222" },
  ".ts":   { label: "TS",   bg: "#3178c6", fg: "#fff" },
  ".tsx":  { label: "TSX",  bg: "#3178c6", fg: "#fff" },
  ".json": { label: "{ }",  bg: "#cbcb41", fg: "#222" },
  ".py":   { label: "PY",   bg: "#3572A5", fg: "#fff" },
  ".md":   { label: "MD",   bg: "#083fa1", fg: "#fff" },
  ".html": { label: "HTML", bg: "#e34c26", fg: "#fff" },
  ".css":  { label: "CSS",  bg: "#563d7c", fg: "#fff" },
  ".sh":   { label: "SH",   bg: "#89e051", fg: "#222" },
  ".env":  { label: "ENV",  bg: "#6e7681", fg: "#fff" },
  ".yml":  { label: "YML",  bg: "#cb171e", fg: "#fff" },
  ".yaml": { label: "YML",  bg: "#cb171e", fg: "#fff" },
  ".zip":  { label: "ZIP",  bg: "#6e7681", fg: "#fff" },
  ".rar":  { label: "RAR",  bg: "#6e7681", fg: "#fff" },
  ".png":  { label: "IMG",  bg: "#a855f7", fg: "#fff" },
  ".jpg":  { label: "IMG",  bg: "#a855f7", fg: "#fff" },
  ".jpeg": { label: "IMG",  bg: "#a855f7", fg: "#fff" },
  ".gif":  { label: "IMG",  bg: "#a855f7", fg: "#fff" },
  ".webp": { label: "IMG",  bg: "#a855f7", fg: "#fff" },
  ".txt":  { label: "TXT",  bg: "#89929b", fg: "#fff" },
  ".db":   { label: "DB",   bg: "#81c784", fg: "#222" },
  ".sqlite": { label: "DB", bg: "#81c784", fg: "#222" },
  ".csv":  { label: "CSV",  bg: "#81c784", fg: "#222" },
};
function fileBadge(name) {
  const ext = path.extname(name).toLowerCase();
  if (EXT_BADGES[ext]) return EXT_BADGES[ext];
  return { label: ext ? ext.slice(1, 4).toUpperCase() : "•", bg: "#475569", fg: "#e2e8f0" };
}

function safeResolve(relPath) {
  const full = path.resolve(CONFIG.BOT_DIR, "." + path.sep + relPath);
  if (!full.startsWith(path.resolve(CONFIG.BOT_DIR))) throw new Error("অবৈধ পাথ");
  return full;
}

app.get("/files", (req, res) => res.send(renderFiles()));

app.get("/api/files/list", async (req, res) => {
  const rel = req.query.dir || ".";
  try {
    await fsp.mkdir(CONFIG.BOT_DIR, { recursive: true }).catch(() => {});
    const full = safeResolve(rel);
    let entries;
    try {
      entries = await fsp.readdir(full, { withFileTypes: true });
    } catch (e) {
      if (e.code === "ENOENT") {
        // বট এখনো ইম্পোর্ট হয়নি — এরর না দেখিয়ে সহায়ক বার্তা দেওয়া হচ্ছে
        return res.json({ ok: true, items: [], summary: { folders: 0, files: 0 }, empty: true });
      }
      throw e;
    }
    const items = [];
    for (const e of entries) {
      if (HIDDEN_FILES.includes(e.name)) continue;
      const stat = await fsp.stat(path.join(full, e.name)).catch(() => null);
      items.push({
        name: e.name,
        isDir: e.isDirectory(),
        size: stat ? stat.size : 0,
        mtime: stat ? stat.mtimeMs : 0,
        badge: e.isDirectory() ? null : fileBadge(e.name),
      });
    }
    items.sort((a, b) => (b.isDir - a.isDir) || a.name.localeCompare(b.name));
    res.json({
      ok: true, items,
      summary: { folders: items.filter(i => i.isDir).length, files: items.filter(i => !i.isDir).length },
    });
  } catch (e) { res.json({ ok: false, msg: e.message }); }
});

app.get("/api/files/read", async (req, res) => {
  try {
    const full = safeResolve(req.query.path);
    const content = await fsp.readFile(full, "utf8");
    res.json({ ok: true, content });
  } catch (e) { res.json({ ok: false, msg: e.message }); }
});

app.post("/api/files/write", async (req, res) => {
  try {
    const { path: relPath, content, isNew } = req.body;
    const full = safeResolve(relPath);
    await fsp.mkdir(path.dirname(full), { recursive: true });
    await fsp.writeFile(full, content ?? "", "utf8");
    notifyBotFileChange(isNew ? "add" : "edit", relPath);
    pushLog("success", `📝 ফাইল ${isNew ? "তৈরি" : "এডিট"} হয়েছে: ${relPath}`);
    if (mongoDb) mongoDb.collection("bot_files").updateOne(
      { _id: relPath }, { $set: { content, mtime: Date.now() } }, { upsert: true }
    ).catch(() => {});
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, msg: e.message }); }
});

app.post("/api/files/delete", async (req, res) => {
  try {
    const { path: relPath } = req.body;
    const full = safeResolve(relPath);
    const stat = await fsp.stat(full);
    if (stat.isDirectory()) await fsp.rm(full, { recursive: true, force: true });
    else await fsp.unlink(full);
    notifyBotFileChange("delete", relPath);
    pushLog("warning", `🗑 মুছে ফেলা হয়েছে: ${relPath}`);
    if (mongoDb) mongoDb.collection("bot_files").deleteOne({ _id: relPath }).catch(() => {});
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, msg: e.message }); }
});

app.post("/api/files/rename", async (req, res) => {
  try {
    const { from, to } = req.body;
    await fsp.rename(safeResolve(from), safeResolve(to));
    pushLog("info", `✏️ নাম পরিবর্তন: ${from} → ${to}`);
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, msg: e.message }); }
});

app.post("/api/files/move", async (req, res) => {
  try {
    const { from, to } = req.body;
    await fsp.rename(safeResolve(from), safeResolve(to));
    pushLog("info", `📦 সরানো হয়েছে: ${from} → ${to}`);
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, msg: e.message }); }
});

app.post("/api/files/copy", async (req, res) => {
  try {
    const { from, to } = req.body;
    await fsp.cp(safeResolve(from), safeResolve(to), { recursive: true });
    pushLog("info", `📋 কপি হয়েছে: ${from} → ${to}`);
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, msg: e.message }); }
});

app.post("/api/files/mkdir", async (req, res) => {
  try {
    const { path: relPath } = req.body;
    await fsp.mkdir(safeResolve(relPath), { recursive: true });
    res.json({ ok: true });
  } catch (e) { res.json({ ok: false, msg: e.message }); }
});

app.get("/api/files/search", async (req, res) => {
  const q = (req.query.q || "").toLowerCase();
  const results = [];
  async function walk(dir, rel) {
    let entries;
    try { entries = await fsp.readdir(dir, { withFileTypes: true }); } catch { return; }
    for (const e of entries) {
      if (e.name === "node_modules" || e.name === ".git" || HIDDEN_FILES.includes(e.name)) continue;
      const r = rel ? rel + "/" + e.name : e.name;
      if (e.name.toLowerCase().includes(q)) results.push({ path: r, isDir: e.isDirectory() });
      if (e.isDirectory() && results.length < 200) await walk(path.join(dir, e.name), r);
    }
  }
  await walk(CONFIG.BOT_DIR, "");
  res.json({ ok: true, results: results.slice(0, 200) });
});

app.get("/api/files/download-zip", (req, res) => {
  const rel = req.query.dir || ".";
  try {
    const full = safeResolve(rel);
    res.attachment((rel === "." ? "bot" : path.basename(rel)) + ".zip");
    const archive = archiver("zip", { zlib: { level: 9 } });
    archive.on("error", (err) => res.status(500).end(err.message));
    archive.pipe(res);
    archive.directory(full, false);
    archive.finalize();
  } catch (e) { res.status(400).json({ ok: false, msg: e.message }); }
});

// ---------------------------------------------------------------------------
// ৯. কমান্ড টেস্টার
// ---------------------------------------------------------------------------
app.get("/tester", (req, res) => res.send(renderTester()));

app.post("/api/test-command", async (req, res) => {
  const relPath = req.body.path;
  const full = safeResolve(relPath);
  const result = { path: relPath, syntax: null, structure: null, dependencies: null, apiChecks: [], verdict: "unknown" };

  // ১) সিনট্যাক্স
  await new Promise((resolve) => {
    const p = spawn(process.execPath, ["--check", full]);
    let err = "";
    p.stderr.on("data", (d) => (err += d));
    p.on("exit", (code) => {
      result.syntax = code === 0 ? { ok: true } : { ok: false, msg: err.trim() };
      resolve();
    });
  });

  // ২) স্ট্রাকচার — isolated subprocess-এ require করে চেক (মূল প্যানেল ক্র্যাশ না করে)
  if (result.syntax.ok) {
    result.structure = await new Promise((resolve) => {
      const checkerCode = `
        try {
          const mod = require(${JSON.stringify(full)});
          const cfg = mod.config || {};
          const hasRun = typeof mod.run === "function" || typeof mod.onStart === "function" || typeof mod.onCall === "function";
          console.log(JSON.stringify({ ok: !!cfg.name && hasRun, hasName: !!cfg.name, hasRun, name: cfg.name || null, dependencies: cfg.dependencies || {} }));
        } catch (e) {
          console.log(JSON.stringify({ ok: false, error: e.message }));
        }
      `;
      const p = fork(path.join(os.tmpdir(), "checker.js"), [], { silent: true, execArgv: [] });
      // eval-style isolated child: write temp file then run
      const tmpFile = path.join(os.tmpdir(), `checker_${Date.now()}.js`);
      fs.writeFileSync(tmpFile, checkerCode);
      const runner = fork(tmpFile, [], { silent: true, cwd: CONFIG.BOT_DIR, timeout: 8000 });
      let out = "";
      runner.stdout.on("data", (d) => (out += d));
      runner.on("exit", () => {
        fs.unlink(tmpFile, () => {});
        try { resolve(JSON.parse(out.trim())); }
        catch { resolve({ ok: false, error: "স্ট্রাকচার চেক ব্যর্থ (parse error)" }); }
      });
      setTimeout(() => { try { runner.kill(); } catch {} }, 8500);
    });
  }

  // ৩) Dependency ইনস্টলড কিনা
  if (result.structure && result.structure.dependencies) {
    const deps = Object.keys(result.structure.dependencies);
    result.dependencies = deps.map((d) => {
      try { require.resolve(d, { paths: [CONFIG.BOT_DIR] }); return { name: d, installed: true }; }
      catch { return { name: d, installed: false }; }
    });
  } else {
    result.dependencies = [];
  }

  // ৪) ফাইলের ভেতরের API URL লাইভ টেস্ট
  try {
    const src = await fsp.readFile(full, "utf8");
    const urls = [...src.matchAll(/https?:\/\/[^\s"'`)]+/g)].map((m) => m[0]).slice(0, 5);
    const axios = require("axios");
    for (const url of urls) {
      try {
        const r = await axios.head(url, { timeout: 5000, validateStatus: () => true });
        result.apiChecks.push({ url, status: r.status, ok: r.status < 400 });
      } catch (e) {
        result.apiChecks.push({ url, status: null, ok: false, error: e.message });
      }
    }
  } catch { /* ignore */ }

  const structOk = result.structure ? result.structure.ok : false;
  const depsOk = result.dependencies.every((d) => d.installed);
  const apiOk = result.apiChecks.every((a) => a.ok);
  result.verdict = (result.syntax.ok && structOk && depsOk && apiOk) ? "ok" : "issue";

  res.json({ ok: true, result });
});

// ---------------------------------------------------------------------------
// ১০. লাইভ মনিটর + টার্মিনাল ট্যাব ডেটা
// ---------------------------------------------------------------------------
app.get("/monitor", (req, res) => res.send(renderMonitor()));
app.get("/terminal", (req, res) => res.send(renderTerminal()));
app.get("/logs", (req, res) => res.send(renderLogs()));

app.get("/api/monitor/data", async (req, res) => {
  let mongoStorageMB = null, mongoEntries = null;
  if (mongoDb) {
    try {
      const stats = await mongoDb.stats();
      mongoStorageMB = Math.round((stats.dataSize + stats.indexSize) / 1024 / 1024 * 100) / 100;
      STATE.mongoStorageMax = Math.max(STATE.mongoStorageMax, mongoStorageMB);
      mongoEntries = await mongoDb.collection("bot_files").countDocuments().catch(() => null);
      if (mongoStorageMB && (mongoStorageMB / 512) > 0.85) {
        pushNotification("mongo-storage", `⚠️ MongoDB স্টোরেজ ${mongoStorageMB}MB (৮৫%+ ব্যবহৃত)।`);
      }
    } catch {}
  }

  let renderBandwidth = null;
  if (CONFIG.RENDER_API_KEY && CONFIG.RENDER_SERVICE_ID) {
    try {
      const axios = require("axios");
      const r = await axios.get(
        `https://api.render.com/v1/services/${CONFIG.RENDER_SERVICE_ID}/bandwidth`,
        { headers: { Authorization: `Bearer ${CONFIG.RENDER_API_KEY}` }, timeout: 6000 }
      );
      renderBandwidth = r.data;
    } catch { /* ঐচ্ছিক ফিচার, ব্যর্থ হলে চুপচাপ স্কিপ */ }
  }

  res.json({
    ok: true,
    panelRamMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
    panelRamMax: STATE.panelRamMax,
    botRamMB: STATE.botPid ? getProcRamMB(STATE.botPid) : null,
    botRamMax: STATE.botRamMax,
    memLimitMB: CONFIG.MEM_LIMIT_MB,
    mongoStorageMB, mongoStorageMax: STATE.mongoStorageMax, mongoEntries,
    lifetime: STATE.lifetime,
    renderBandwidth,
  });
});

app.get("/api/terminal/data", (req, res) => {
  const net = readNetSpeed();
  const load = os.loadavg()[0];
  const cpuPct = Math.min(100, Math.round((load / os.cpus().length) * 100));
  const memPct = Math.round((1 - os.freemem() / os.totalmem()) * 100);
  res.json({
    ok: true,
    net, cpuPct, memPct,
    heavyDownloadCount: STATE.heavyDownloadCount,
    maxHeavy: STATE.MAX_HEAVY,
    botStatus: STATE.botStatus,
    uptimeMs: STATE.startedAt ? Date.now() - STATE.startedAt : 0,
    recentLogs: STATE.logs.slice(-6),
  });
});

function readNetSpeed() {
  try {
    const data = fs.readFileSync("/proc/net/dev", "utf8");
    let rx = 0, tx = 0;
    data.split("\n").slice(2).forEach((line) => {
      const parts = line.trim().split(/\s+/);
      if (parts.length < 10) return;
      if (parts[0].startsWith("lo:") || parts[0] === "lo") return;
      rx += Number(parts[1]) || 0;
      tx += Number(parts[9]) || 0;
    });
    const now = Date.now();
    let rxKBs = 0, txKBs = 0;
    if (STATE.netPrev) {
      const dt = (now - STATE.netPrev.t) / 1000;
      if (dt > 0) {
        rxKBs = Math.max(0, Math.round((rx - STATE.netPrev.rx) / 1024 / dt));
        txKBs = Math.max(0, Math.round((tx - STATE.netPrev.tx) / 1024 / dt));
      }
    }
    STATE.netPrev = { rx, tx, t: now };
    return { downKBs: rxKBs, upKBs: txKBs };
  } catch {
    return { downKBs: null, upKBs: null }; // /proc/net/dev না থাকলে (নন-লিনাক্স)
  }
}

// ---------------------------------------------------------------------------
// ১১. নোটিফিকেশন API + রিসেট + "আরো" পেজ
// ---------------------------------------------------------------------------
app.get("/api/notifications", (req, res) => res.json({ ok: true, items: STATE.notifications.slice(-100).reverse() }));
app.post("/api/notifications/read-all", (req, res) => {
  STATE.notifications.forEach((n) => (n.read = true));
  res.json({ ok: true });
});

app.post("/api/reset", (req, res) => {
  STATE.logs = [];
  STATE.notifications = [];
  pushLog("info", "🔄 প্যানেল রিসেট করা হয়েছে (লগ + নোটিফিকেশন ফাঁকা)।");
  res.json({ ok: true });
});

app.post("/api/settings/reset-lifetime", async (req, res) => {
  STATE.lifetime = { totalStarts: 0, totalCrashes: 0, totalUptimeMs: 0, trackingSince: Date.now() };
  STATE.restartHistory = [];
  STATE.panelRamMax = 0;
  STATE.botRamMax = 0;
  STATE.mongoStorageMax = 0;
  if (mongoDb) await mongoSaveState().catch(() => {});
  pushLog("info", "🔄 লাইফটাইম পরিসংখ্যান (Start/Crash/RAM max/রিস্টার্ট ইতিহাস) রিসেট করা হলো।");
  res.json({ ok: true });
});

app.post("/api/settings/reset-notifications", (req, res) => {
  STATE.notifications = [];
  STATE.notifCooldowns = {};
  if (mongoDb) mongoSaveState().catch(() => {});
  res.json({ ok: true });
});

app.get("/more", (req, res) => res.send(renderMore()));

app.get("/settings", (req, res) => res.send(renderSettings()));

app.get("/upload", (req, res) => res.send(renderUpload()));

app.get("/api/settings", (req, res) => {
  res.json({
    ok: true,
    mongoConnected: !!mongoDb,
    GITHUB_REPO: CONFIG.GITHUB_REPO,
    GITHUB_BRANCH: CONFIG.GITHUB_BRANCH,
    GITHUB_ZIP_PATH: CONFIG.GITHUB_ZIP_PATH,
    GITHUB_TOKEN_SET: !!CONFIG.GITHUB_TOKEN, // নিরাপত্তার জন্য আসল টোকেন ফেরত পাঠানো হয় না
    DAILY_RESTART_HOUR: CONFIG.DAILY_RESTART_HOUR,
    DEV_INFO: DEV_INFO,
    lockedByEnv: MONGO_OVERRIDABLE_KEYS.filter((k) => !!process.env[k]),
  });
});

app.post("/api/settings", async (req, res) => {
  const r = await saveSettingsToMongo(req.body || {});
  res.json(r);
});

app.post("/api/settings/import-now", async (req, res) => {
  const r = await githubImportZipIfNeeded(true); // force=true, সাথে সাথে ইম্পোর্ট, Render রিস্টার্টের অপেক্ষা করতে হবে না
  res.json(r);
});

// ভুল/ডাবল-নেস্টেড ইম্পোর্ট হয়ে গেলে MongoDB + ডিস্ক থেকে বট ফাইল সম্পূর্ণ মুছে ফেলে —
// এরপর আবার ইম্পোর্ট/আপলোড করলে একদম ফ্রেশ শুরু হবে
app.post("/api/settings/test-mongo", async (req, res) => {
  const r = await mongoConnect(); // re-attempt connection right now, without needing a redeploy
  if (r.ok) {
    await loadSettingsFromMongo();
    await githubImportZipIfNeeded(); // এখন কানেক্ট হয়েছে তো — MongoDB-তে ডেটা থাকলে রিস্টোর করে ফেলুক
  }
  res.json(r);
});

app.get("/api/status-badges", (req, res) => {
  res.json({ ok: true, botStatus: STATE.botStatus, dbConnected: !!mongoDb });
});

app.post("/api/settings/wipe-bot-files", async (req, res) => {
  try {
    if (mongoDb) {
      await mongoDb.collection("bot_files").deleteMany({});
      await mongoDb.collection("state").deleteOne({ _id: "github_import" });
    }
    await fsp.rm(CONFIG.BOT_DIR, { recursive: true, force: true }).catch(() => {});
    await fsp.mkdir(CONFIG.BOT_DIR, { recursive: true });
    pushLog("warning", "🗑 MongoDB + ডিস্ক থেকে সব বট ফাইল মুছে ফেলা হলো — পরের ইম্পোর্ট/আপলোড একদম ফ্রেশ শুরু হবে।");
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false, msg: e.message });
  }
});

// ---------------------------------------------------------------------------
// ১২. WebSocket (লাইভ লগ / নোটিফিকেশন / স্ট্যাটাস ব্রডকাস্ট)
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server, path: "/ws" });
function broadcast(payload) {
  const data = JSON.stringify(payload);
  wss.clients.forEach((c) => { if (c.readyState === 1) c.send(data); });
}
wss.on("connection", (ws) => {
  ws.send(JSON.stringify({ kind: "init", status: STATE.botStatus, logs: STATE.logs.slice(-200) }));
});

// ---------------------------------------------------------------------------
// ১৩. UI — শেয়ার্ড লেআউট, CSS, ক্লায়েন্ট JS
// ---------------------------------------------------------------------------
function baseCss() {
  return `
  :root{--bg:#0b0f14;--panel:#121821;--panel2:#1a2230;--border:#243044;--text:#e6edf3;--muted:#8b98a9;
        --accent:#3b82f6;--green:#22c55e;--yellow:#eab308;--red:#ef4444;--purple:#a855f7;}
  *{box-sizing:border-box;} body{margin:0;background:var(--bg);color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;padding-bottom:74px;}
  a{color:inherit;text-decoration:none;}
  .nav{display:flex;position:fixed;bottom:0;left:0;right:0;background:var(--panel);
       border-top:1px solid var(--border);z-index:20;padding-top:3px;}
  .nav a{flex:1;display:flex;flex-direction:column;align-items:center;gap:3px;
       padding:6px 2px 8px;font-size:10.5px;color:var(--muted);position:relative;}
  .nav a .navicon{width:32px;height:32px;border-radius:10px;display:flex;align-items:center;
       justify-content:center;font-size:16px;background:var(--panel2);}
  .nav a.active{color:var(--text);}
  .nav a.active .navicon{background:linear-gradient(135deg,var(--accent),var(--purple));}
  .nav a.active::before{content:'';position:absolute;top:0;left:22%;right:22%;height:3px;
       background:linear-gradient(90deg,var(--accent),var(--purple));border-radius:0 0 3px 3px;}
  .wrap{max-width:900px;margin:0 auto;padding:14px;}
  .card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:14px;margin-bottom:12px;}
  .grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;}
  @media(min-width:600px){.grid{grid-template-columns:repeat(3,1fr);}}
  .stat{background:var(--panel2);border-radius:10px;padding:10px;text-align:center;}
  .stat .v{font-size:20px;font-weight:700;} .stat .l{font-size:11px;color:var(--muted);margin-top:2px;}
  .btn{display:inline-flex;align-items:center;gap:6px;background:var(--panel2);border:1px solid var(--border);
       color:var(--text);padding:10px 14px;border-radius:10px;font-size:14px;cursor:pointer;margin:4px 4px 0 0;}
  .btn:active{transform:scale(0.97);}
  .btn.primary{background:var(--accent);border-color:var(--accent);}
  .btn.danger{background:var(--red);border-color:var(--red);}
  .btn.success{background:var(--green);border-color:var(--green);color:#04220f;}
  .badge{display:inline-block;padding:3px 9px;border-radius:20px;font-size:12px;font-weight:600;}
  .badge.offline{background:#3a1414;color:#f87171;} .badge.booting{background:#3a2f0f;color:#facc15;}
  .badge.online{background:#0f3a1e;color:#4ade80;}
  input,textarea,select{background:var(--panel2);border:1px solid var(--border);color:var(--text);
       border-radius:8px;padding:9px;width:100%;font-family:inherit;font-size:14px;}
  .log-line{padding:6px 8px;border-left:3px solid var(--muted);font-size:12.5px;font-family:monospace;
       white-space:pre-wrap;word-break:break-word;margin-bottom:2px;border-radius:4px;background:rgba(255,255,255,.02);}
  .log-info{border-color:var(--accent);} .log-success{border-color:var(--green);}
  .log-warning{border-color:var(--yellow);} .log-error{border-color:var(--red);}
  .bell{position:relative;cursor:pointer;font-size:20px;} 
  .bell .cnt{position:absolute;top:-6px;right:-10px;background:var(--red);border-radius:50%;
       font-size:10px;padding:1px 5px;}
  #errbox{display:none;position:fixed;bottom:0;left:0;right:0;background:#3a0f0f;color:#fca5a5;
       font-family:monospace;font-size:12px;padding:10px;z-index:999;max-height:40vh;overflow:auto;}
  .topbar{display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--panel);}
  .file-row{display:flex;align-items:center;justify-content:space-between;padding:9px 6px;border-bottom:1px solid var(--border);}
  .dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:6px;}
  .run-badge{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:6px;font-size:12px;font-weight:600;}
  .run-badge.running{background:#3a2f0f;color:#facc15;}
  .run-badge.pass{background:#0f3a1e;color:#4ade80;}
  .run-badge.fail{background:#3a1414;color:#f87171;}
  .spin{display:inline-block;animation:spin 0.8s linear infinite;}
  @keyframes spin{from{transform:rotate(0deg);}to{transform:rotate(360deg);}}
  `;
}

function clientErrorCatcher() {
  // পুরনো ব্রাউজার সাপোর্টের জন্য ?? এবং ?. এড়ানো হয়েছে
  return `
  window.onerror = function(msg, src, line, col, err){
    var box = document.getElementById('errbox');
    if (box) {
      box.style.display = 'block';
      box.innerHTML += '<div>[JS Error] ' + msg + ' (' + src + ':' + line + ':' + col + ')</div>';
    }
    return false;
  };
  window.addEventListener('unhandledrejection', function(ev){
    var box = document.getElementById('errbox');
    if (box) {
      box.style.display = 'block';
      var reason = (ev && ev.reason && ev.reason.message) ? ev.reason.message : String(ev.reason);
      box.innerHTML += '<div>[Promise Rejection] ' + reason + '</div>';
    }
  });
  `;
}

function navBar(active) {
  const items = [
    ["/", "🏠", "হোম"], ["/monitor", "📊", "মনিটর"], ["/terminal", "⚡", "টার্মিনাল"],
    ["/logs", "📋", "লগ"], ["/files", "📁", "ফাইল"], ["/upload", "📤", "আপলোড"], ["/more", "⚙️", "আরো"],
  ];
  return `<div class="nav">${items.map(([href, icon, label]) =>
    `<a href="${href}" class="${active === href ? "active" : ""}"><span class="navicon">${icon}</span>${label}</a>`).join("")}</div>`;
}

function bellWidget() {
  return `
  <span class="bell" onclick="toggleNotifPanel()">🔔<span class="cnt" id="notifCount">0</span></span>
  <div id="notifPanel" style="display:none;position:fixed;top:50px;right:10px;left:10px;max-width:400px;
       margin-left:auto;background:var(--panel);border:1px solid var(--border);border-radius:12px;
       padding:10px;z-index:50;max-height:70vh;overflow:auto;"></div>
  <div id="notifBanner" style="display:none;position:fixed;top:0;left:0;right:0;background:var(--accent);
       color:#fff;padding:10px 14px;font-size:14px;z-index:60;"></div>
  `;
}

function sharedScript() {
  return `
  var ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws');
  var unread = 0;
  ws.onmessage = function(ev){
    var msg = JSON.parse(ev.data);
    if (msg.kind === 'log') appendLog(msg.entry);
    if (msg.kind === 'init') { (msg.logs || []).forEach(appendLog); setStatus(msg.status); }
    if (msg.kind === 'status') setStatus(msg.status);
    if (msg.kind === 'notification') { showBanner(msg.entry); unread++; updateNotifCount(); }
    if (msg.kind === 'stats' && window.onStatsUpdate) window.onStatsUpdate(msg);
  };
  function appendLog(entry){
    var el = document.getElementById('logBox');
    if (!el) return;
    var div = document.createElement('div');
    div.className = 'log-line log-' + entry.type;
    var t = new Date(entry.ts).toLocaleTimeString();
    div.textContent = '[' + t + '] ' + entry.text;
    el.appendChild(div);
    while (el.children.length > 500) el.removeChild(el.firstChild);
    el.scrollTop = el.scrollHeight;
  }
  function setStatus(status){
    var el = document.getElementById('statusBadge');
    if (!el) return;
    var map = { offline: ['🔴 বন্ধ','offline'], booting: ['🟡 চালু হচ্ছে','booting'], online: ['✅ সম্পূর্ণ প্রস্তুত','online'] };
    var pair = map[status] || map.offline;
    el.textContent = pair[0];
    el.className = 'badge ' + pair[1];
  }
  function showBanner(entry){
    var b = document.getElementById('notifBanner');
    if (!b) return;
    b.textContent = '🔔 ' + entry.text;
    b.style.display = 'block';
    setTimeout(function(){ b.style.display = 'none'; }, 8000);
  }
  function updateNotifCount(){
    var el = document.getElementById('notifCount');
    if (el) el.textContent = unread > 0 ? unread : '';
  }
  function toggleNotifPanel(){
    var p = document.getElementById('notifPanel');
    if (!p) return;
    if (p.style.display === 'none' || !p.style.display) {
      fetch('/api/notifications').then(function(r){ return r.json(); }).then(function(data){
        var items = data.items || [];
        p.innerHTML = items.length ? items.map(function(n){
          return '<div style="padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;">' +
                 '<div style="color:var(--muted);font-size:11px;">' + new Date(n.ts).toLocaleString() + '</div>' +
                 n.text + '</div>';
        }).join('') : '<div style="color:var(--muted);padding:10px;">কোনো নোটিফিকেশন নেই</div>';
        p.style.display = 'block';
        unread = 0; updateNotifCount();
        fetch('/api/notifications/read-all', { method: 'POST' });
      });
    } else {
      p.style.display = 'none';
    }
  }
  function callApi(url){
    fetch(url, { method: 'POST' }).then(function(r){ return r.json(); }).then(function(d){
      if (!d.ok) alert('ব্যর্থ: ' + (d.msg || 'অজানা এরর'));
    });
  }
  `;
}

function page(title, active, body, extraScript) {
  return `<!DOCTYPE html>
<html lang="bn"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<title>${title} — Bot Panel</title>
<style>${baseCss()}</style>
<script>${clientErrorCatcher()}</script>
</head><body>
<div id="errbox"></div>
${bellWidget()}
${navBar(active)}
<div class="wrap">${body}</div>
<script>${sharedScript()}${extraScript || ""}</script>
</body></html>`;
}

function renderLogin(err) {
  return `<!DOCTYPE html><html lang="bn"><head><meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <title>লগইন — Bot Panel</title><style>${baseCss()}
  body{display:flex;align-items:center;justify-content:center;min-height:100vh;
       background:radial-gradient(circle at 30% 20%,#1e1145 0%,#0b0f14 55%),
                  radial-gradient(circle at 80% 80%,#3b0764 0%,transparent 45%);
       overflow:hidden;padding-bottom:0;}
  .box{width:100%;max-width:340px;padding:32px 26px;position:relative;z-index:2;
       background:rgba(18,24,33,.85);backdrop-filter:blur(6px);
       border:1px solid rgba(168,85,247,.35);box-shadow:0 0 40px rgba(99,102,241,.25);}
  .robotwrap{width:74px;height:74px;margin:0 auto 14px;border-radius:20px;
       background:linear-gradient(135deg,#6366f1,#a855f7,#ec4899);
       display:flex;align-items:center;justify-content:center;font-size:34px;
       box-shadow:0 0 30px rgba(168,85,247,.6);animation:floatIcon 3s ease-in-out infinite;}
  @keyframes floatIcon{0%,100%{transform:translateY(0);}50%{transform:translateY(-6px);}}
  .brandtitle{text-align:center;font-size:24px;font-weight:800;margin:0 0 2px;
       background:linear-gradient(90deg,#a78bfa,#f0abfc,#f472b6);
       -webkit-background-clip:text;background-clip:text;color:transparent;}
  .brandsub{text-align:center;color:var(--muted);font-size:12.5px;margin-bottom:18px;}
  .glow-btn{background:linear-gradient(90deg,#7c3aed,#db2777);border:none;
       width:100%;padding:13px;border-radius:12px;color:#fff;font-weight:700;font-size:15px;
       margin-top:12px;cursor:pointer;box-shadow:0 6px 20px rgba(124,58,237,.4);}
  .glow-btn:active{transform:scale(0.98);}
  .particle{position:absolute;border-radius:50%;background:rgba(168,85,247,.5);
       filter:blur(1px);animation:floatUp linear infinite;}
  @keyframes floatUp{from{transform:translateY(0);opacity:.7;}to{transform:translateY(-110vh);opacity:0;}}
  </style>
  <script>${clientErrorCatcher()}</script></head><body>
  <div id="errbox"></div>
  <div id="particles"></div>
  <div class="box">
    <div class="robotwrap">🤖</div>
    <div class="brandtitle">Bot Panel</div>
    <div class="brandsub">তোমার বট কন্ট্রোল সেন্টার</div>
    ${err ? `<p style="color:#f87171;text-align:center;font-size:13.5px;">❌ ${err}</p>` : ""}
    <form method="POST" action="/login">
      <input type="password" name="password" placeholder="🔐 পাসওয়ার্ড দিন" autofocus required
        style="text-align:center;border-color:rgba(168,85,247,.4);">
      <button class="glow-btn" type="submit">প্রবেশ করুন →</button>
    </form>
  </div>
  <script>
    var box = document.getElementById('particles');
    for (var i = 0; i < 18; i++) {
      var p = document.createElement('div');
      var size = 2 + Math.random() * 4;
      p.className = 'particle';
      p.style.width = size + 'px';
      p.style.height = size + 'px';
      p.style.left = (Math.random() * 100) + 'vw';
      p.style.top = (50 + Math.random() * 50) + 'vh';
      p.style.animationDuration = (6 + Math.random() * 8) + 's';
      p.style.animationDelay = (Math.random() * 6) + 's';
      box.appendChild(p);
    }
  </script>
  </body></html>`;
}

function formatDuration(ms) {
  if (!ms || ms < 0) return "0সে";
  const s = Math.floor(ms / 1000);
  const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  let out = "";
  if (d) out += d + "দিন ";
  if (h) out += h + "ঘ ";
  if (m) out += m + "মি ";
  if (!d && !h) out += sec + "সে";
  return out.trim();
}

function renderHome() {
  return page("হোম", "/", `
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <h2 style="margin:0;">বট কন্ট্রোল</h2>
      <span id="statusBadge" class="badge offline">লোড হচ্ছে...</span>
    </div>
    <div style="margin-top:12px;">
      <button class="btn success" onclick="callApi('/api/bot/start')">▶ চালু</button>
      <button class="btn danger" onclick="callApi('/api/bot/stop')">⏹ বন্ধ</button>
      <button class="btn primary" onclick="callApi('/api/bot/restart')">🔄 রিস্টার্ট</button>
      <button class="btn" onclick="callApi('/api/bot/npm-install')">📦 npm install</button>
      <button class="btn" onclick="callApi('/api/bot/backup')">💾 Backup</button>
      <button class="btn" onclick="callApi('/api/bot/mongo-sync')">☁️ MongoDB Sync</button>
      <button class="btn" onclick="callApi('/api/bot/restore')">🔄 Restore</button>
    </div>
    <p style="color:var(--muted);font-size:12.5px;margin-top:8px;">
      Auto-Restart: সবসময় ✅ ON (নিরাপত্তার জন্য বন্ধ করার অপশন রাখা হয়নি)
    </p>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">🍪 Facebook Cookie / Appstate</h3>
    <p style="color:var(--muted);font-size:12.5px;">Cookie বা appstate.json পেস্ট করুন → সেভ হয়ে বট চালু হয়ে যাবে</p>
    <textarea id="appstateInput" rows="4" placeholder='[{"key":"c_user","value":"..."}] অথবা plain cookie string (name=value; name2=value2;)'></textarea>
    <button class="btn success" style="width:100%;justify-content:center;margin-top:8px;" onclick="saveAppstate()">✅ Cookie সেভ ও বট চালু করুন</button>
    <p id="appstateResult" style="font-size:12px;color:var(--muted);margin-top:6px;"></p>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">স্ট্যাট</h3>
    <div class="grid" id="statGrid">
      <div class="stat"><div class="v" id="s-mem">-</div><div class="l">Memory MB</div></div>
      <div class="stat"><div class="v" id="s-uptime">-</div><div class="l">Server Uptime</div></div>
      <div class="stat"><div class="v" id="s-files">-</div><div class="l">বট ফাইল সংখ্যা</div></div>
      <div class="stat"><div class="v" id="s-starts">-</div><div class="l">মোট Start</div></div>
      <div class="stat"><div class="v" id="s-crashes">-</div><div class="l">মোট Crash</div></div>
      <div class="stat"><div class="v" id="s-node">-</div><div class="l">Node.js ভার্সন</div></div>
    </div>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">রিস্টার্ট ইতিহাস</h3>
    <div id="restartHistory" style="font-size:13px;color:var(--muted);">লোড হচ্ছে...</div>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">লাইভ লগ (সংক্ষিপ্ত)</h3>
    <div id="logBox" style="max-height:260px;overflow:auto;"></div>
  </div>
  `, `
  function refreshStatus(){
    fetch('/api/bot/status').then(function(r){ return r.json(); }).then(function(d){
      setStatus(d.status);
      document.getElementById('s-mem').textContent = d.panelRamMB + (d.botRamMB ? ' / ' + d.botRamMB : '') + ' MB';
      document.getElementById('s-uptime').textContent = formatDurationClient(d.serverUptimeMs);
      document.getElementById('s-files').textContent = d.botFileCount;
      document.getElementById('s-starts').textContent = d.lifetime.totalStarts;
      document.getElementById('s-crashes').textContent = d.lifetime.totalCrashes;
      document.getElementById('s-node').textContent = d.nodeVersion;
      var hist = d.restartHistory || [];
      document.getElementById('restartHistory').innerHTML = hist.length ? hist.map(function(h){
        return '<div style="padding:4px 0;border-bottom:1px solid var(--border);">' +
          new Date(h.ts).toLocaleString() + ' — exit ' + h.exitCode + ', uptime ' + formatDurationClient(h.uptimeMs) + '</div>';
      }).join('') : 'কোনো ইতিহাস নেই';
    });
  }
  function formatDurationClient(ms){
    if (!ms || ms < 0) return '0সে';
    var s = Math.floor(ms/1000), d = Math.floor(s/86400), h = Math.floor((s%86400)/3600), m = Math.floor((s%3600)/60);
    if (d) return d + 'দিন ' + h + 'ঘ';
    if (h) return h + 'ঘ ' + m + 'মি';
    return m + 'মি';
  }
  function saveAppstate(){
    var text = document.getElementById('appstateInput').value.trim();
    if (!text) { alert('আগে cookie/appstate পেস্ট করুন'); return; }
    document.getElementById('appstateResult').textContent = '⏳ সেভ হচ্ছে...';
    fetch('/api/bot/set-appstate', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ text: text }) })
      .then(function(r){ return r.json(); }).then(function(d){
        document.getElementById('appstateResult').textContent = d.ok
          ? '✅ সেভ হয়েছে — বট রিস্টার্ট হচ্ছে, নিচের লগে দেখুন।'
          : '❌ ব্যর্থ: ' + d.msg;
      });
  }
  refreshStatus();
  setInterval(refreshStatus, 8000);
  `);
}

function renderFiles() {
  return page("ফাইল ম্যানেজার", "/files", `
  <div class="card">
    <input id="searchBox" placeholder="🔍 ফাইল সার্চ..." oninput="onSearch()">
    <div style="margin-top:8px;color:var(--muted);font-size:13px;" id="pathBar">📁 /</div>
    <div style="margin-top:6px;" id="summary"></div>
  </div>
  <div class="card">
    <button class="btn" onclick="mkNew(true)">📁+ নতুন ফোল্ডার</button>
    <button class="btn" onclick="mkNew(false)">📄+ নতুন ফাইল</button>
    <button class="btn" onclick="downloadZip()">⬇️ Zip ডাউনলোড</button>
  </div>
  <div class="card" id="fileList">লোড হচ্ছে...</div>
  <div id="editorModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.8);z-index:80;padding:10px;">
    <div class="card" style="height:90vh;display:flex;flex-direction:column;">
      <div style="display:flex;justify-content:space-between;">
        <b id="editorTitle">ফাইল</b>
        <span style="cursor:pointer;" onclick="closeEditor()">✕</span>
      </div>
      <textarea id="editorArea" style="flex:1;margin-top:8px;font-family:monospace;font-size:12.5px;"></textarea>
      <div style="margin-top:8px;">
        <button class="btn primary" onclick="saveFile()">💾 সেভ</button>
        <button class="btn" onclick="testFromEditor()">🧪 টেস্ট করুন</button>
      </div>
      <div id="editorResult" style="font-size:12px;color:var(--muted);margin-top:6px;"></div>
    </div>
  </div>
  `, `
  var curDir = '.';
  var curFile = null;
  function load(dir){
    curDir = dir || '.';
    document.getElementById('pathBar').textContent = '📁 /' + (curDir === '.' ? '' : curDir);
    fetch('/api/files/list?dir=' + encodeURIComponent(curDir)).then(function(r){ return r.json(); }).then(function(d){
      if (!d.ok) { document.getElementById('fileList').textContent = 'এরর: ' + d.msg; return; }
      document.getElementById('summary').textContent = d.summary.folders + ' ফোল্ডার, ' + d.summary.files + ' ফাইল';
      if (d.empty && curDir === '.') {
        document.getElementById('fileList').innerHTML =
          '<p style="color:var(--muted);text-align:center;padding:20px 10px;">📭 এখনো কোনো বট ফাইল নেই।<br>' +
          '⚙️ সেটিংস ট্যাব থেকে GitHub রিপো বসিয়ে ইম্পোর্ট করুন, অথবা<br>' +
          '📤 আপলোড ট্যাব থেকে সরাসরি bot.zip আপলোড করুন।</p>';
        return;
      }
      var html = '';
      if (curDir !== '.') html += '<div class="file-row" style="cursor:pointer;" onclick="load(parentDir())">⬅️ ফিরে যান</div>';
      d.items.forEach(function(it, i){
        var full = (curDir === '.' ? '' : curDir + '/') + it.name;
        var badge = it.isDir
          ? '<span style="width:22px;height:22px;border-radius:5px;background:#37415177;display:inline-flex;align-items:center;justify-content:center;font-size:13px;margin-right:6px;">📁</span>'
          : '<span style="min-width:30px;height:20px;border-radius:5px;background:' + it.badge.bg + ';color:' + it.badge.fg +
            ';display:inline-flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;padding:0 4px;margin-right:6px;">' + it.badge.label + '</span>';
        html += '<div class="file-row">' +
          '<span style="cursor:pointer;flex:1;display:flex;align-items:center;" onclick="' + (it.isDir ? "load('" + full + "')" : "openFile('" + full + "')") + '">' +
          badge + it.name + '</span>' +
          '<span>' +
          (it.isDir ? '' : '<span id="runbadge-' + i + '"><button class="btn" style="padding:4px 8px;" onclick="testFile(\\'' + full + '\\',' + i + ',false)">🧪</button></span>') +
          '<button class="btn" style="padding:4px 8px;" onclick="renameItem(\\'' + full + '\\')">✏️</button>' +
          '<button class="btn danger" style="padding:4px 8px;" onclick="deleteItem(\\'' + full + '\\')">🗑</button>' +
          '</span></div>';
      });
      document.getElementById('fileList').innerHTML = html || '<i style="color:var(--muted);">খালি</i>';
    });
  }
  function parentDir(){
    var parts = curDir.split('/'); parts.pop();
    return parts.length ? parts.join('/') : '.';
  }
  function openFile(full){
    curFile = full;
    fetch('/api/files/read?path=' + encodeURIComponent(full)).then(function(r){ return r.json(); }).then(function(d){
      if (!d.ok) { alert(d.msg); return; }
      document.getElementById('editorTitle').textContent = full;
      document.getElementById('editorArea').value = d.content;
      document.getElementById('editorResult').textContent = '';
      document.getElementById('editorModal').style.display = 'block';
    });
  }
  function closeEditor(){ document.getElementById('editorModal').style.display = 'none'; }
  function saveFile(){
    fetch('/api/files/write', { method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ path: curFile, content: document.getElementById('editorArea').value, isNew: false }) })
      .then(function(r){ return r.json(); }).then(function(d){
        if (d.ok) { closeEditor(); load(curDir); } else alert(d.msg);
      });
  }
  function testFromEditor(){ testFile(curFile, null, true); }
  function testFile(full, rowIndex, inEditor){
    var slot = (rowIndex != null) ? document.getElementById('runbadge-' + rowIndex) : null;
    if (slot) slot.innerHTML = '<span class="run-badge running"><span class="spin">⟳</span> চলছে</span>';
    if (inEditor) document.getElementById('editorResult').textContent = '⏳ টেস্ট চলছে...';
    fetch('/api/test-command', { method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ path: full }) })
      .then(function(r){ return r.json(); }).then(function(d){
        var r = d.result;
        var pass = r.verdict === 'ok';
        var msg = (pass ? '✅ ঠিক আছে' : '⚠️ সমস্যা আছে') + ' — সিনট্যাক্স: ' + (r.syntax.ok ? 'ঠিক' : r.syntax.msg);
        if (slot) {
          slot.innerHTML = '<span class="run-badge ' + (pass ? 'pass' : 'fail') + '" style="cursor:pointer;" onclick="testFile(\\'' + full + '\\',' + rowIndex + ',false)">' +
            (pass ? '✅' : '❌') + '</span>';
        }
        if (inEditor) document.getElementById('editorResult').textContent = msg;
        if (!slot && !inEditor) alert(msg);
      })
      .catch(function(){
        if (slot) slot.innerHTML = '<span class="run-badge fail">❌</span>';
      });
  }
  function mkNew(isDir){
    var name = prompt(isDir ? 'ফোল্ডারের নাম:' : 'ফাইলের নাম:');
    if (!name) return;
    var full = (curDir === '.' ? '' : curDir + '/') + name;
    if (isDir) {
      fetch('/api/files/mkdir', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ path: full }) })
        .then(function(){ load(curDir); });
    } else {
      fetch('/api/files/write', { method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ path: full, content: '', isNew: true }) }).then(function(){ load(curDir); });
    }
  }
  function renameItem(full){
    var name = prompt('নতুন নাম/পাথ:', full);
    if (!name || name === full) return;
    fetch('/api/files/rename', { method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ from: full, to: name }) }).then(function(){ load(curDir); });
  }
  function deleteItem(full){
    if (!confirm('মুছে ফেলবেন? ' + full)) return;
    fetch('/api/files/delete', { method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ path: full }) }).then(function(){ load(curDir); });
  }
  function downloadZip(){ location.href = '/api/files/download-zip?dir=' + encodeURIComponent(curDir); }
  function onSearch(){
    var q = document.getElementById('searchBox').value;
    if (!q) { load(curDir); return; }
    fetch('/api/files/search?q=' + encodeURIComponent(q)).then(function(r){ return r.json(); }).then(function(d){
      document.getElementById('fileList').innerHTML = d.results.map(function(r){
        return '<div class="file-row" style="cursor:pointer;" onclick="' +
          (r.isDir ? "load('" + r.path + "')" : "openFile('" + r.path + "')") + '">' +
          (r.isDir ? '📁' : '📄') + ' ' + r.path + '</div>';
      }).join('') || '<i style="color:var(--muted);">কিছু পাওয়া যায়নি</i>';
    });
  }
  load('.');
  `);
}

function renderTester() {
  return page("কমান্ড টেস্টার", "/tester", `
  <div class="card">
    <h3 style="margin-top:0;">🧪 যেকোনো ফাইল টেস্ট করুন</h3>
    <input id="testPath" placeholder="যেমন: Script/commands/ping.js">
    <button class="btn primary" style="margin-top:8px;" onclick="runTest()">টেস্ট চালান</button>
  </div>
  <div class="card" id="verdict" style="display:none;"></div>
  <div class="card" id="testResult" style="display:none;"></div>
  `, `
  function runTest(){
    var p = document.getElementById('testPath').value.trim();
    if (!p) return;
    document.getElementById('verdict').style.display = 'block';
    document.getElementById('verdict').innerHTML = '<span class="run-badge running"><span class="spin">⟳</span> চলছে...</span>';
    fetch('/api/test-command', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ path: p }) })
      .then(function(r){ return r.json(); }).then(function(d){
        var r = d.result;
        var v = document.getElementById('verdict');
        var pass = r.verdict === 'ok';
        v.innerHTML = '<span class="run-badge ' + (pass ? 'pass' : 'fail') + '" style="font-size:15px;">' +
          (pass ? '✅ ঠিক আছে' : '⚠️ সমস্যা আছে') + '</span>';
        var out = document.getElementById('testResult');
        out.style.display = 'block';
        out.innerHTML =
          '<p><b>১. সিনট্যাক্স:</b> ' + (r.syntax.ok ? '✅ ঠিক' : '❌ ' + r.syntax.msg) + '</p>' +
          '<p><b>২. স্ট্রাকচার:</b> ' + (r.structure && r.structure.ok ? '✅ config.name + run/onStart/onCall আছে (' + r.structure.name + ')' : '❌ ' + (r.structure ? (r.structure.error || 'অসম্পূর্ণ') : 'চেক হয়নি')) + '</p>' +
          '<p><b>৩. Dependency:</b><br>' + (r.dependencies.length ? r.dependencies.map(function(dp){
            return (dp.installed ? '✅ ' : '❌ ') + dp.name;
          }).join('<br>') : 'কোনো dependency ঘোষিত নেই') + '</p>' +
          '<p><b>৪. API URL টেস্ট:</b><br>' + (r.apiChecks.length ? r.apiChecks.map(function(a){
            return (a.ok ? '✅ ' : '❌ ') + a.url + ' (status: ' + a.status + ')';
          }).join('<br>') : 'কোনো URL পাওয়া যায়নি') + '</p>';
      });
  }
  `);
}

function renderMonitor() {
  return page("লাইভ মনিটর", "/monitor", `
  <div class="card">
    <h3 style="margin-top:0;">RAM</h3>
    <div class="grid">
      <div class="stat"><div class="v" id="m-panel">-</div><div class="l">প্যানেল RAM</div></div>
      <div class="stat"><div class="v" id="m-bot">-</div><div class="l">বট RAM</div></div>
      <div class="stat"><div class="v" id="m-total">-</div><div class="l">মোট</div></div>
    </div>
    <div style="margin-top:8px;color:var(--muted);font-size:12px;" id="m-max"></div>
  </div>
  <div class="card">
    <h3 style="margin-top:0;">MongoDB Atlas স্টোরেজ</h3>
    <div id="m-mongo">লোড হচ্ছে...</div>
  </div>
  <div class="card">
    <h3 style="margin-top:0;">লাইফটাইম সামারি</h3>
    <div id="m-lifetime">লোড হচ্ছে...</div>
  </div>
  `, `
  function loadMonitor(){
    fetch('/api/monitor/data').then(function(r){ return r.json(); }).then(function(d){
      document.getElementById('m-panel').textContent = d.panelRamMB + ' MB';
      document.getElementById('m-bot').textContent = (d.botRamMB != null ? d.botRamMB : '-') + ' MB';
      document.getElementById('m-total').textContent = (d.panelRamMB + (d.botRamMB || 0)) + ' / ' + d.memLimitMB + ' MB';
      document.getElementById('m-max').textContent = 'লাইফটাইম সর্বোচ্চ — প্যানেল: ' + d.panelRamMax + 'MB, বট: ' + d.botRamMax + 'MB';
      document.getElementById('m-mongo').innerHTML = d.mongoStorageMB != null
        ? ('ব্যবহৃত: ' + d.mongoStorageMB + ' / 512 MB<br>মোট এন্ট্রি: ' + d.mongoEntries + '<br>লাইফটাইম সর্বোচ্চ: ' + d.mongoStorageMax + ' MB')
        : 'MongoDB কনফিগার করা নেই';
      document.getElementById('m-lifetime').innerHTML =
        'মোট Start: ' + d.lifetime.totalStarts + '<br>মোট Crash: ' + d.lifetime.totalCrashes +
        '<br>ট্র্যাকিং শুরু: ' + new Date(d.lifetime.trackingSince).toLocaleDateString();
    });
  }
  loadMonitor();
  setInterval(loadMonitor, 10000);
  `);
}

function renderTerminal() {
  return page("টার্মিনাল", "/terminal", `
  <div class="card" style="background:#000;color:#22c55e;font-family:monospace;">
    <div style="text-align:center;font-size:18px;letter-spacing:2px;" id="glitchTitle">⚡ SYSTEM TERMINAL ⚡</div>
    <pre id="termBody" style="white-space:pre-wrap;font-size:12.5px;margin-top:10px;">লোড হচ্ছে...</pre>
  </div>
  `, `
  function glitch(){
    var el = document.getElementById('glitchTitle');
    var chars = '!<>-_\\\\/[]{}—=+*^?#________';
    var orig = '⚡ SYSTEM TERMINAL ⚡';
    if (Math.random() < 0.15) {
      var arr = orig.split('');
      var i = Math.floor(Math.random()*arr.length);
      arr[i] = chars[Math.floor(Math.random()*chars.length)];
      el.textContent = arr.join('');
      setTimeout(function(){ el.textContent = orig; }, 120);
    }
  }
  setInterval(glitch, 900);
  function bar(pct, color){
    var filled = Math.round(pct/5);
    return '[' + Array(filled+1).join('#') + Array(20-filled+1).join('.') + '] ' + pct + '%';
  }
  function loadTerm(){
    fetch('/api/terminal/data').then(function(r){ return r.json(); }).then(function(d){
      var statusMap = { offline: 'OFFLINE', booting: 'BOOTING', online: 'ONLINE' };
      var lines = [];
      lines.push('STATUS      : ' + statusMap[d.botStatus]);
      lines.push('UPTIME      : ' + Math.floor(d.uptimeMs/1000) + 's');
      lines.push('CPU LOAD    : ' + bar(d.cpuPct));
      lines.push('MEMORY      : ' + bar(d.memPct));
      lines.push('NET DOWN    : ' + (d.net.downKBs != null ? d.net.downKBs + ' KB/s' : 'N/A'));
      lines.push('NET UP      : ' + (d.net.upKBs != null ? d.net.upKBs + ' KB/s' : 'N/A'));
      lines.push('HEAVY JOBS  : ' + d.heavyDownloadCount + ' / ' + d.maxHeavy);
      lines.push('');
      lines.push('--- সাম্প্রতিক লগ ---');
      d.recentLogs.forEach(function(l){ lines.push('[' + new Date(l.ts).toLocaleTimeString() + '] ' + l.text); });
      document.getElementById('termBody').textContent = lines.join('\\n');
    });
  }
  loadTerm();
  setInterval(loadTerm, 4000);
  `);
}

function renderLogs() {
  return page("লগ", "/logs", `
  <div class="card" style="background:#000;border-color:#1a2e1a;padding:0;overflow:hidden;">
    <div style="display:flex;align-items:center;gap:8px;padding:10px 14px;background:#0a120a;border-bottom:1px solid #1a2e1a;">
      <span style="width:9px;height:9px;border-radius:50%;background:#ef4444;display:inline-block;"></span>
      <span style="width:9px;height:9px;border-radius:50%;background:#eab308;display:inline-block;"></span>
      <span style="width:9px;height:9px;border-radius:50%;background:#22c55e;display:inline-block;"></span>
      <span style="color:#22c55e;font-family:monospace;font-size:12px;margin-left:8px;">live.log</span>
      <span id="liveDot" style="margin-left:auto;color:#22c55e;font-family:monospace;font-size:11px;">● LIVE</span>
    </div>
    <div id="logBox" style="height:74vh;overflow-y:auto;padding:10px 12px;background:#000;font-family:'Courier New',monospace;"></div>
  </div>
  `, `
  // এই পেজে লগ-বক্সের স্টাইল হ্যাকার-টার্মিনাল ধাঁচে ওভাররাইড করা হচ্ছে
  var style = document.createElement('style');
  style.textContent =
    '#logBox .log-line{background:transparent;border-left:none;padding:2px 0;font-size:12px;color:#4ade80;}' +
    '#logBox .log-error{color:#f87171;}' +
    '#logBox .log-warning{color:#facc15;}' +
    '#logBox .log-success{color:#4ade80;text-shadow:0 0 4px rgba(74,222,128,.35);}' +
    '#logBox .log-info{color:#7dd3fc;}' +
    '#logBox .log-line::before{content:"$ ";color:#22c55e;opacity:.6;}';
  document.head.appendChild(style);
  var blink = true;
  setInterval(function(){
    var dot = document.getElementById('liveDot');
    if (dot) dot.style.opacity = blink ? '1' : '0.25';
    blink = !blink;
  }, 600);
  `);
}

function renderMore() {
  return page("আরো", "/more", `
  <div class="card">
    <h3 style="margin-top:0;">🔗 আরো টুলস</h3>
    <a href="/tester" class="btn" style="width:100%;justify-content:center;box-sizing:border-box;">🧪 কমান্ড টেস্টার</a>
    <a href="/settings" class="btn" style="width:100%;justify-content:center;box-sizing:border-box;margin-top:8px;">⚙️ সেটিংস (GitHub, ডেভেলপার তথ্য ইত্যাদি)</a>
  </div>
  <div class="card">
    <h3 style="margin-top:0;">👤 ডেভেলপার তথ্য</h3>
    <p><b>নাম:</b> ${DEV_INFO.name}<br>
       <b>পেশা:</b> ${DEV_INFO.role}<br>
       <b>যোগাযোগ:</b> ${DEV_INFO.contact}<br>
       <b>ব্র্যান্ড:</b> ${DEV_INFO.brand}</p>
  </div>
  <div class="card">
    <h3 style="margin-top:0;">🔄 রিসেট</h3>
    <p style="color:var(--muted);font-size:13px;">পুরনো লগ + নোটিফিকেশন মুছে ফ্রেশ ভিউ দেখাবে।</p>
    <button class="btn danger" onclick="if(confirm('রিসেট করবেন?')) callApi('/api/reset')">🔄 রিসেট করুন</button>
  </div>
  <div class="card">
    <p style="color:var(--muted);font-size:12px;">Build: ${CONFIG.BUILD_VERSION}</p>
    <a href="/logout" class="btn">লগ আউট</a>
  </div>
  `);
}

function renderUpload() {
  return page("আপলোড", "/upload", `
  <div class="card">
    <h3 style="margin-top:0;">📤 সরাসরি ফোন থেকে bot.zip আপলোড</h3>
    <p style="color:var(--muted);font-size:12.5px;">GitHub এর দরকার নেই — এখান থেকে সরাসরি zip বসিয়ে দিলে সেটা বট ফোল্ডারে এক্সট্র্যাক্ট হয়ে সাথে সাথে MongoDB-তে ব্যাকআপ হয়ে যাবে।</p>
    <p style="color:var(--yellow);font-size:12px;">⚠️ এটা বর্তমান বট ফোল্ডারের উপর দিয়ে লিখে দেবে (existing ফাইল ওভাররাইট হবে)।</p>
    <input type="file" id="zipInput" accept=".zip" style="padding:10px;">
    <button class="btn primary" style="width:100%;justify-content:center;margin-top:10px;" onclick="doUpload()">📤 আপলোড শুরু করুন</button>
    <div id="uploadProgress" style="margin-top:10px;font-size:13px;color:var(--muted);"></div>
  </div>
  `, `
  function doUpload(){
    var input = document.getElementById('zipInput');
    if (!input.files || !input.files[0]) { alert('একটা zip ফাইল বেছে নিন'); return; }
    var fd = new FormData();
    fd.append('zipfile', input.files[0]);
    document.getElementById('uploadProgress').textContent = '⏳ আপলোড হচ্ছে... ফাইল বড় হলে কিছুক্ষণ সময় লাগতে পারে।';
    fetch('/api/bot/upload-zip', { method: 'POST', body: fd })
      .then(function(r){ return r.json(); }).then(function(d){
        document.getElementById('uploadProgress').textContent = d.ok
          ? '✅ সফল! ' + d.count + ' টা ফাইল ইম্পোর্ট হয়েছে। এখন 📁 ফাইল ট্যাব বা 🏠 হোম থেকে বট চালু করুন।'
          : '❌ ব্যর্থ: ' + d.msg;
      })
      .catch(function(e){ document.getElementById('uploadProgress').textContent = '❌ এরর: ' + e.message; });
  }
  `);
}

function renderSettings() {
  return page("সেটিংস", "/settings", `
  <div class="card" id="mongoCard" style="border-color:#3a2f0f;">
    <h3 style="margin-top:0;">🗄️ MongoDB স্ট্যাটাস</h3>
    <p id="mongoStatusLine" style="font-size:14px;">⏳ চেক করা হচ্ছে...</p>
    <button class="btn primary" style="width:100%;justify-content:center;" onclick="testMongo()">🔌 এখনই টেস্ট / রিকানেক্ট করুন</button>
    <p id="mongoHint" style="font-size:12.5px;color:var(--muted);margin-top:8px;"></p>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">🐙 GitHub অটো-ইম্পোর্ট</h3>
    <p style="color:var(--muted);font-size:12.5px;">শুধু প্রথমবার (MongoDB খালি থাকলে) এই রিপো থেকে zip ইম্পোর্ট হবে। এরপর MongoDB-ই সোর্স অফ ট্রুথ।</p>
    <label style="font-size:12.5px;color:var(--muted);">GitHub রিপো লিংক বা owner/repo</label>
    <input id="s-repo" placeholder="https://github.com/username/repo">
    <label style="font-size:12.5px;color:var(--muted);margin-top:8px;display:block;">ব্রাঞ্চ</label>
    <input id="s-branch" placeholder="main">
    <label style="font-size:12.5px;color:var(--muted);margin-top:8px;display:block;">zip ফাইলের নাম (রিপোর রুটে)</label>
    <input id="s-zippath" placeholder="bot.zip">
    <label style="font-size:12.5px;color:var(--muted);margin-top:8px;display:block;">GitHub Token (শুধু Private রিপো হলে লাগবে)</label>
    <input id="s-token" placeholder="ghp_xxxxxxxx (আগে সেট থাকলে ফাঁকা রাখুন)">
    <button class="btn primary" style="width:100%;justify-content:center;margin-top:10px;" onclick="importNow()">📥 এখনই GitHub থেকে ইম্পোর্ট করুন</button>
    <p id="importResult" style="font-size:12.5px;color:var(--muted);margin-top:6px;"></p>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">📤 অথবা সরাসরি ফোন থেকে zip আপলোড</h3>
    <p style="color:var(--muted);font-size:12.5px;">GitHub ছাড়াই — <a href="/upload" style="color:var(--accent);">📤 আপলোড ট্যাবে</a> গিয়ে সরাসরি bot.zip বসিয়ে দিন।</p>
  </div>

  <div class="card" style="border-color:#3a1414;">
    <h3 style="margin-top:0;color:var(--red);">🗑 বট ফাইল সম্পূর্ণ মুছে ফ্রেশ শুরু করুন</h3>
    <p style="color:var(--muted);font-size:12.5px;">যদি ভুল/ডাবল-নেস্টেড zip ইম্পোর্ট হয়ে যায় (যেমন <code>bot/bot/index.js</code>), এই বাটন MongoDB + ডিস্ক থেকে সব বট ফাইল মুছে দেবে — এরপর আবার GitHub ইম্পোর্ট বা আপলোড করলে একদম ফ্রেশ শুরু হবে।</p>
    <button class="btn danger" style="width:100%;justify-content:center;" onclick="wipeBotFiles()">🗑 সব বট ফাইল মুছে ফেলুন</button>
    <p id="wipeResult" style="font-size:12.5px;color:var(--muted);margin-top:6px;"></p>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">🔄 রিসেট সেন্টার</h3>
    <p style="color:var(--muted);font-size:12.5px;">যেটা দরকার শুধু সেটাই রিসেট করুন — একেবারে সব একসাথে মোছার দরকার নেই।</p>
    <button class="btn" style="width:100%;justify-content:center;margin-top:6px;" onclick="doReset('/api/reset','লগ ও নোটিফিকেশন')">🧹 লগ + নোটিফিকেশন খালি করুন</button>
    <button class="btn" style="width:100%;justify-content:center;margin-top:6px;" onclick="doReset('/api/settings/reset-notifications','নোটিফিকেশন')">🔔 শুধু নোটিফিকেশন খালি করুন</button>
    <button class="btn" style="width:100%;justify-content:center;margin-top:6px;" onclick="doReset('/api/settings/reset-lifetime','লাইফটাইম Start/Crash/RAM পরিসংখ্যান')">📊 লাইফটাইম পরিসংখ্যান রিসেট করুন</button>
    <p id="resetCenterResult" style="font-size:12.5px;color:var(--muted);margin-top:6px;"></p>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">⏰ শিডিউল</h3>
    <label style="font-size:12.5px;color:var(--muted);">রোজ কোন ঘণ্টায় (0-23) বট অটো-রিস্টার্ট হবে (ঐচ্ছিক, ফাঁকা রাখলে বন্ধ)</label>
    <input id="s-restarthour" placeholder="যেমন: 4 (রাত ৪টা)">
  </div>

  <div class="card">
    <h3 style="margin-top:0;">👤 ডেভেলপার তথ্য ("আরো" পেজে দেখাবে)</h3>
    <input id="s-devname" placeholder="নাম" style="margin-bottom:8px;">
    <input id="s-devrole" placeholder="পেশা" style="margin-bottom:8px;">
    <input id="s-devcontact" placeholder="যোগাযোগ" style="margin-bottom:8px;">
    <input id="s-devbrand" placeholder="ব্র্যান্ড নাম">
  </div>

  <button class="btn primary" style="width:100%;justify-content:center;padding:12px;" onclick="saveSettings()">💾 সব সেভ করুন</button>
  <p id="lockedNote" style="color:var(--muted);font-size:12px;margin-top:10px;"></p>
  `, `
  function loadSettings(){
    fetch('/api/settings').then(function(r){ return r.json(); }).then(function(d){
      renderMongoStatus(d.mongoConnected, null);
      document.getElementById('s-repo').value = d.GITHUB_REPO || '';
      document.getElementById('s-branch').value = d.GITHUB_BRANCH || 'main';
      document.getElementById('s-zippath').value = d.GITHUB_ZIP_PATH || 'bot.zip';
      document.getElementById('s-token').placeholder = d.GITHUB_TOKEN_SET ? 'আগে সেট করা আছে (পরিবর্তন না করলে ফাঁকা রাখুন)' : 'ghp_xxxxxxxx';
      document.getElementById('s-restarthour').value = d.DAILY_RESTART_HOUR != null ? d.DAILY_RESTART_HOUR : '';
      document.getElementById('s-devname').value = d.DEV_INFO.name || '';
      document.getElementById('s-devrole').value = d.DEV_INFO.role || '';
      document.getElementById('s-devcontact').value = d.DEV_INFO.contact || '';
      document.getElementById('s-devbrand').value = d.DEV_INFO.brand || '';
      if (d.lockedByEnv && d.lockedByEnv.length) {
        document.getElementById('lockedNote').textContent =
          'নোট: এই ফিল্ডগুলো Render Environment Variable থেকে সেট করা আছে, তাই এখান থেকে পরিবর্তন কাজ করবে না যতক্ষণ না Render থেকে মুছবেন: ' + d.lockedByEnv.join(', ');
      }
    });
  }
  function renderMongoStatus(connected, extra){
    var line = document.getElementById('mongoStatusLine');
    var card = document.getElementById('mongoCard');
    var hint = document.getElementById('mongoHint');
    if (connected) {
      line.innerHTML = '<span style="color:var(--green);">✅ সংযুক্ত আছে</span> — ফাইল ও সেটিংস স্থায়ীভাবে সেভ হচ্ছে';
      card.style.borderColor = '#0f3a1e';
      hint.textContent = '';
    } else {
      line.innerHTML = '<span style="color:var(--red);">❌ সংযুক্ত নেই</span> — ফাইল/সেটিংস রিস্টার্টে হারিয়ে যাবে';
      card.style.borderColor = '#3a1414';
      hint.textContent = extra || 'নিচের বাটনে চেপে টেস্ট করুন — সঠিক কারণ দেখাবে।';
    }
  }
  function testMongo(){
    document.getElementById('mongoStatusLine').innerHTML = '⏳ টেস্ট করা হচ্ছে...';
    fetch('/api/settings/test-mongo', { method: 'POST' }).then(function(r){ return r.json(); }).then(function(d){
      renderMongoStatus(d.ok, d.ok ? null : (d.msg + (d.hint ? ' — ' + d.hint : '')));
    });
  }
  function doReset(url, label){
    if (!confirm(label + ' রিসেট করবেন?')) return;
    fetch(url, { method: 'POST' }).then(function(r){ return r.json(); }).then(function(d){
      document.getElementById('resetCenterResult').textContent = d.ok ? '✅ ' + label + ' রিসেট হয়েছে।' : '❌ ব্যর্থ: ' + d.msg;
    });
  }
  function saveSettings(){
    var body = {
      GITHUB_REPO: document.getElementById('s-repo').value,
      GITHUB_BRANCH: document.getElementById('s-branch').value,
      GITHUB_ZIP_PATH: document.getElementById('s-zippath').value,
      DAILY_RESTART_HOUR: document.getElementById('s-restarthour').value,
      DEV_INFO: {
        name: document.getElementById('s-devname').value,
        role: document.getElementById('s-devrole').value,
        contact: document.getElementById('s-devcontact').value,
        brand: document.getElementById('s-devbrand').value,
      }
    };
    var tokenVal = document.getElementById('s-token').value;
    if (tokenVal) body.GITHUB_TOKEN = tokenVal;
    fetch('/api/settings', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) })
      .then(function(r){ return r.json(); }).then(function(d){
        alert(d.ok ? '✅ সেভ হয়েছে' : '❌ ব্যর্থ: ' + d.msg);
      });
  }
  function importNow(){
    document.getElementById('importResult').textContent = '⏳ প্রথমে সেটিংস সেভ হচ্ছে, তারপর ইম্পোর্ট শুরু হবে...';
    var body = {
      GITHUB_REPO: document.getElementById('s-repo').value,
      GITHUB_BRANCH: document.getElementById('s-branch').value,
      GITHUB_ZIP_PATH: document.getElementById('s-zippath').value,
    };
    var tokenVal = document.getElementById('s-token').value;
    if (tokenVal) body.GITHUB_TOKEN = tokenVal;
    fetch('/api/settings', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) })
      .then(function(){
        document.getElementById('importResult').textContent = '📥 GitHub থেকে ইম্পোর্ট চলছে... হোম ট্যাবের লগে অগ্রগতি দেখতে পারবেন।';
        return fetch('/api/settings/import-now', { method: 'POST' });
      })
      .then(function(r){ return r.json(); }).then(function(d){
        document.getElementById('importResult').textContent = d.ok
          ? '✅ ইম্পোর্ট সম্পন্ন! এখন 📁 ফাইল ট্যাবে গিয়ে দেখুন।'
          : '❌ ব্যর্থ: ' + d.msg;
      });
  }
  function wipeBotFiles(){
    if (!confirm('নিশ্চিত? এতে MongoDB + ডিস্কের সব বট ফাইল মুছে যাবে (ফেরত আনা যাবে না)।')) return;
    document.getElementById('wipeResult').textContent = '⏳ মুছে ফেলা হচ্ছে...';
    fetch('/api/settings/wipe-bot-files', { method: 'POST' })
      .then(function(r){ return r.json(); }).then(function(d){
        document.getElementById('wipeResult').textContent = d.ok
          ? '✅ সব মুছে ফেলা হয়েছে — এখন উপরের "এখনই GitHub থেকে ইম্পোর্ট করুন" বাটন বা 📤 আপলোড ট্যাব থেকে ফ্রেশ ইম্পোর্ট করুন।'
          : '❌ ব্যর্থ: ' + d.msg;
      });
  }
  loadSettings();
  `);
}

// ---------------------------------------------------------------------------
// ১৪. বুট সিকোয়েন্স
// ---------------------------------------------------------------------------
async function boot() {
  try {
    await mongoConnect();
    await mongoRestoreState();       // লাইফটাইম স্ট্যাট/নোটিফিকেশন রিস্টোর
    await loadSettingsFromMongo();   // ⚙️ সেটিংস পেজ থেকে সেভ করা GitHub/অন্য কনফিগ লোড
    await githubImportZipIfNeeded(); // MongoDB-তে ফাইল না থাকলেই শুধু GitHub থেকে zip ইম্পোর্ট হবে, নাহলে skip
  } catch (e) {
    // বুট সিকোয়েন্সে কোনো একটা ধাপ ব্যর্থ হলেও প্যানেল যেন বন্ধ না হয়ে যায় — সবসময় লিসেন করবে
    console.error("Boot sequence-এ সমস্যা (প্যানেল তবুও চালু হবে):", e.message);
  }

  server.listen(CONFIG.PORT, () => {
    pushLog("success", `🚀 প্যানেল চালু হয়েছে — পোর্ট ${CONFIG.PORT}`);
    console.log(`Panel running on http://localhost:${CONFIG.PORT}`);
  });

  setInterval(mongoSaveState, 60 * 1000); // প্রতি মিনিটে লাইফটাইম ডেটা ব্যাকআপ

  // আগের সেশন চালু ছিল কিনা — এখানে চাইলে auto-start যোগ করা যায়:
  // startBot("boot-auto-start");
}

process.on("SIGTERM", () => { if (STATE.botProcess) STATE.botProcess.kill(); process.exit(0); });
process.on("uncaughtException", (e) => { try { pushLog("error", "প্যানেল uncaughtException: " + e.message); } catch {} });
process.on("unhandledRejection", (e) => { try { pushLog("error", "প্যানেল unhandledRejection: " + (e && e.message ? e.message : e)); } catch {} });

boot();
