#!/usr/bin/env node
/**
 * Leads Automation - Runs the Leads Finder actor with automatic deduplication.
 *
 * Usage:
 *   node --env-file=.env leads-automation.js                        # interactive profile picker
 *   node --env-file=.env leads-automation.js --profile 0            # run profile by index
 *   node --env-file=.env leads-automation.js --profile "Corporate"  # run profile by name match
 *   node --env-file=.env leads-automation.js --list-seen            # show all seen lead counts per run
 */

import { parseArgs } from 'node:util';
import { writeFileSync, readFileSync, existsSync } from 'node:fs';
import { createInterface } from 'node:readline';

const TOKEN = process.env.APIFY_TOKEN;
const ACTOR_ID = 'code_crafter~leads-finder';
const API_BASE = 'https://api.apify.com/v2';
const CONFIG_FILE = './leads-config.json';

// ─── Helpers ────────────────────────────────────────────────────────────────

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

async function apiFetch(path, opts = {}) {
    const sep = path.includes('?') ? '&' : '?';
    const url = `${API_BASE}${path}${sep}token=${encodeURIComponent(TOKEN)}`;
    const res = await fetch(url, opts);
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`API ${res.status} at ${path}: ${text}`);
    }
    return res.json();
}

function toKey(lead) {
    // Deduplication key: prefer email, fall back to linkedin
    const email = (lead.email || '').toLowerCase().trim();
    const linkedin = (lead.linkedin || '').toLowerCase().trim().replace(/\/$/, '');
    return email || linkedin || null;
}

function formatCsv(rows) {
    if (!rows.length) return '';
    const fields = Object.keys(rows[0]);
    const escape = (v) => {
        if (v === null || v === undefined) return '';
        let s = typeof v === 'object' ? JSON.stringify(v) : String(v);
        if (s.includes(',') || s.includes('"') || s.includes('\n')) {
            s = `"${s.replace(/"/g, '""')}"`;
        }
        return s;
    };
    const lines = [fields.join(',')];
    for (const row of rows) {
        lines.push(fields.map((f) => escape(row[f])).join(','));
    }
    return lines.join('\n');
}

// ─── Apify API calls ─────────────────────────────────────────────────────────

async function getAllRuns() {
    const data = await apiFetch(`/acts/${ACTOR_ID.replace('/', '~')}/runs?limit=100&desc=true`);
    return data.data.items;
}

async function getDatasetItems(datasetId, fields = null) {
    let path = `/datasets/${datasetId}/items?limit=50000&format=json`;
    if (fields) path += `&fields=${fields}`;
    const res = await fetch(
        `${API_BASE}${path}&token=${encodeURIComponent(TOKEN)}`
    );
    if (!res.ok) return []; // dataset may be empty or deleted
    return res.json();
}

async function getRunInput(kvStoreId) {
    try {
        const res = await fetch(
            `${API_BASE}/key-value-stores/${kvStoreId}/records/INPUT?token=${encodeURIComponent(TOKEN)}`
        );
        if (!res.ok) return null;
        return res.json();
    } catch {
        return null;
    }
}

// ─── Build seen-keys set from all past runs ───────────────────────────────────

async function buildSeenKeys() {
    console.log('\nFetching all past runs to build deduplication index...');
    const runs = await getAllRuns();
    const succeededOrPartial = runs.filter((r) =>
        ['SUCCEEDED', 'TIMED-OUT', 'ABORTED'].includes(r.status)
    );

    console.log(`  Found ${succeededOrPartial.length} runs with data.`);

    const seen = new Set();
    for (const run of succeededOrPartial) {
        try {
            const items = await getDatasetItems(run.defaultDatasetId, 'email,linkedin');
            let count = 0;
            for (const item of items) {
                const key = toKey(item);
                if (key) {
                    seen.add(key);
                    count++;
                }
            }
            console.log(`  Run ${run.id} (${run.status}): ${count} leads indexed`);
        } catch {
            console.log(`  Run ${run.id}: skipped (empty or inaccessible)`);
        }
    }

    console.log(`\nTotal unique leads already scraped: ${seen.size}`);
    return seen;
}

// ─── Start actor run ──────────────────────────────────────────────────────────

async function startRun(input) {
    const data = await apiFetch(`/acts/${ACTOR_ID.replace('/', '~')}/runs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(input),
    });
    return { runId: data.data.id, datasetId: data.data.defaultDatasetId };
}

async function pollRun(runId, timeoutSecs = 3600) {
    const start = Date.now();
    let lastStatus = null;
    while (true) {
        const data = await apiFetch(`/actor-runs/${runId}`);
        const status = data.data.status;
        if (status !== lastStatus) {
            console.log(`  Status: ${status}`);
            lastStatus = status;
        }
        if (['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT'].includes(status)) {
            return status;
        }
        if ((Date.now() - start) / 1000 > timeoutSecs) {
            console.warn('  Warning: polling timeout reached, run may still be active.');
            return 'POLL-TIMEOUT';
        }
        await sleep(10_000);
    }
}

// ─── List seen mode ───────────────────────────────────────────────────────────

async function listSeenMode() {
    const runs = await getAllRuns();
    console.log('\nRun ID                    | Status     | Leads | Input name');
    console.log('─'.repeat(80));
    for (const run of runs) {
        const input = await getRunInput(run.defaultKeyValueStoreId);
        const name = input?.file_name || '(unknown)';
        let count = 0;
        try {
            const items = await getDatasetItems(run.defaultDatasetId, 'email');
            count = items.length;
        } catch {}
        const statusPad = run.status.padEnd(10);
        console.log(`${run.id} | ${statusPad} | ${String(count).padStart(5)} | ${name}`);
    }
}

// ─── Profile picker ───────────────────────────────────────────────────────────

async function pickProfile(profiles, profileArg) {
    if (profileArg !== undefined) {
        const byIndex = parseInt(profileArg, 10);
        if (!isNaN(byIndex) && profiles[byIndex]) return profiles[byIndex];
        // match by name substring
        const match = profiles.find((p) =>
            p.name.toLowerCase().includes(profileArg.toLowerCase())
        );
        if (match) return match;
        console.error(`No profile found matching: "${profileArg}"`);
        process.exit(1);
    }

    // Interactive picker
    console.log('\nAvailable profiles:');
    profiles.forEach((p, i) => console.log(`  [${i}] ${p.name}`));
    const rl = createInterface({ input: process.stdin, output: process.stdout });
    const answer = await new Promise((resolve) => {
        rl.question('\nEnter profile number: ', resolve);
    });
    rl.close();
    const idx = parseInt(answer.trim(), 10);
    if (isNaN(idx) || !profiles[idx]) {
        console.error('Invalid selection.');
        process.exit(1);
    }
    return profiles[idx];
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
    if (!TOKEN) {
        console.error('Error: APIFY_TOKEN not set. Add it to your .env file.');
        process.exit(1);
    }

    const { values } = parseArgs({
        options: {
            profile: { type: 'string', short: 'p' },
            'list-seen': { type: 'boolean' },
            'skip-dedup': { type: 'boolean' },
            output: { type: 'string', short: 'o' },
        },
        allowPositionals: false,
    });

    if (values['list-seen']) {
        await listSeenMode();
        return;
    }

    if (!existsSync(CONFIG_FILE)) {
        console.error(`Config file not found: ${CONFIG_FILE}`);
        process.exit(1);
    }

    const config = JSON.parse(readFileSync(CONFIG_FILE, 'utf-8'));
    const profile = await pickProfile(config.profiles, values.profile);

    console.log(`\nProfile: ${profile.name}`);
    console.log(`Fetch count: ${profile.fetch_count}`);

    // Build deduplication index unless skipped
    let seenKeys = new Set();
    if (!values['skip-dedup']) {
        seenKeys = await buildSeenKeys();
    }

    // Build actor input from profile — spread all fields except 'name' (config-only)
    const { name: _name, ...profileFields } = profile;
    const input = {
        file_name: profile.name,
        ...profileFields,
    };

    // Start the run
    console.log('\nStarting actor run...');
    const { runId, datasetId } = await startRun(input);
    console.log(`Run ID:     ${runId}`);
    console.log(`Dataset ID: ${datasetId}`);
    console.log(`Monitor:    https://console.apify.com/actors/runs/${runId}`);

    // Poll to completion
    console.log('\nWaiting for run to complete (polling every 10s)...');
    const finalStatus = await pollRun(runId);

    if (finalStatus === 'FAILED') {
        console.error(`\nRun failed. Check: https://console.apify.com/actors/runs/${runId}`);
        process.exit(1);
    }

    // Download results
    console.log('\nDownloading results...');
    const results = await getDatasetItems(datasetId);
    console.log(`Total fetched: ${results.length}`);

    // Deduplicate
    const newLeads = [];
    let dupeCount = 0;
    for (const lead of results) {
        const key = toKey(lead);
        if (!key || !seenKeys.has(key)) {
            newLeads.push(lead);
            if (key) seenKeys.add(key);
        } else {
            dupeCount++;
        }
    }

    console.log(`Duplicates removed: ${dupeCount}`);
    console.log(`New unique leads:   ${newLeads.length}`);

    if (newLeads.length === 0) {
        console.log('\nNo new leads to save — all results were already scraped.');
        return;
    }

    // Save to CSV
    const date = new Date().toISOString().slice(0, 10);
    const safeName = profile.name.replace(/[^a-z0-9]+/gi, '_').toLowerCase();
    const outputFile = values.output || `${date}_${safeName}_new_leads.csv`;

    writeFileSync(outputFile, formatCsv(newLeads));
    console.log(`\nSaved to: ${outputFile}`);
    console.log(`Dataset:  https://console.apify.com/storage/datasets/${datasetId}`);
}

main().catch((err) => {
    console.error(`\nError: ${err.message}`);
    process.exit(1);
});
