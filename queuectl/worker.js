import { parentPort, isMainThread, threadId } from 'worker_threads';
import { initDb, getDb } from './db.js';
import { sleep, exec } from './utils.js';

if (isMainThread) {
  throw new Error('This file must be run as a worker thread.');
}

const workerId = `worker-${threadId}`;
let db;
let isRunning = true;

async function runWorker() {
  console.log(`[${workerId}] Starting...`);
  db = await initDb();

  parentPort.on('message', (msg) => {
    if (msg === 'stop') {
      console.log(`[${workerId}] Received stop signal. Shutting down gracefully...`);
      isRunning = false;
    }
  });

  while (isRunning) {
    let job = null;
    try {
      job = await pollAndLockJob();

      if (job) {
        await executeJob(job);
      } else {
        await sleep(1000);
      }
    } catch (err) {
      console.error(`[${workerId}] Unhandled error in worker loop:`, err);
      if (job) {
        await setJobState(job.id, 'failed', { attempts: job.attempts });
      }
      await sleep(1000);
    }
  }

  console.log(`[${workerId}] Exited.`);
  parentPort.close();
}

// Tries to find a pending job and lock it for processing.

async function pollAndLockJob() {
  const now = new Date().toISOString();
  const tenSecondsAgo = new Date(Date.now() - 10000).toISOString(); 

  try {
    await db.run('BEGIN IMMEDIATE TRANSACTION;');

    const job = await db.get(
      `SELECT * FROM jobs
       WHERE
         (
           (state = 'pending' AND run_at <= ? AND locked_by IS NULL) OR
           (state = 'failed' AND run_at <= ? AND locked_by IS NULL) OR
           (state = 'processing' AND locked_at < ?)
         )
       ORDER BY priority DESC, created_at ASC
       LIMIT 1`,
      [now, now, tenSecondsAgo]
    );

    if (job) {
      if (job.state === 'processing') {
        console.warn(`[${workerId}] Rescuing stale job: ${job.id} (was locked by ${job.locked_by})`);
      }

      //set 'started_at' - when the job is locked.
      await db.run(
        `UPDATE jobs
         SET
           state = 'processing',
           locked_by = ?,
           locked_at = ?,
           updated_at = ?,
           started_at = ?  -- <-- ADDED THIS
         WHERE id = ?`,
        [workerId, now, now, now, job.id]
      );
      await db.run('COMMIT;');
      
      return { ...job, state: 'processing', locked_by: workerId, started_at: now };
    } else {
      await db.run('COMMIT;');
      return null;
    }
  } catch (err) {
    if (err.code === 'SQLITE_BUSY') {
      return null;
    }
    console.error(`[${workerId}] Error in pollAndLockJob:`, err.message);
    try {
      await db.run('ROLLBACK;');
    } catch (rollbackErr) {
      console.warn(`[${workerId}] Rollback attempt failed:`, rollbackErr.message);
    }
    return null;
  }
}

// Executes the job command and handles success or failure.

async function executeJob(job) {
  const currentAttempt = job.attempts + 1;

  console.log(`\n[${workerId}] --- STARTING Job: ${job.id} (Attempt ${currentAttempt} of ${job.max_retries}) ---`);
  console.log(`[${workerId}]   -> Command: ${job.command}`);
  
  try {
    //Execute the command
    const { stdout, stderr } = await exec(job.command, { timeout: 30000 }); 

    const out = stdout ? stdout.trim() : '';
    const err = stderr ? stderr.trim() : '';
    
    
console.log(`[${workerId}]   -> Status: SUCCESS`);
if (out) console.log(`[${workerId}]   -> stdout: ${out}`);
if (err) console.warn(`[${workerId}]   -> stderr: ${err}`);

await setJobState(job.id, 'completed', { 
  stdout: out, 
  stderr: err,
  attempts: currentAttempt
});

  } catch (err) {
    // On failure
    const out = err.stdout ? err.stdout.trim() : '';
    const errText = err.stderr ? err.stderr.trim() : err.message;

    console.warn(`[${workerId}]   -> Status: FAILED`);
    if (out) console.log(`[${workerId}]   -> stdout: ${out}`);
    if (errText) console.warn(`[${workerId}]   -> stderr: ${errText}`);
    
    const newAttempts = currentAttempt;

    if (newAttempts >= job.max_retries) {
      console.error(`[${workerId}]   -> ACTION: Moving to DLQ (Max retries reached).`);
      // Save output on move to DLQ
      await setJobState(job.id, 'dead', { 
        attempts: newAttempts,
        stdout: out,
        stderr: errText
      });

    } else {
      
      const { value: base } = await db.get("SELECT value FROM config WHERE key = 'backoff_base'");
      const delayInSeconds = Math.pow(parseInt(base, 10), newAttempts);
      const newRunAt = new Date(Date.now() + delayInSeconds * 1000).toISOString();

      console.warn(`[${workerId}]   -> ACTION: Retrying. Next run at ${newRunAt}`);

      // Save output on retry
      await setJobState(job.id, 'failed', {
        attempts: newAttempts,
        run_at: newRunAt,
        stdout: out,
        stderr: errText
      });
    }
  } finally {
    console.log(`[${workerId}] --- FINISHED Job: ${job.id} ---`);
  }
}

 // Helper to update a job's state and release its lock.

async function setJobState(id, state, overrides = {}) {
  const now = new Date().toISOString();
  
  const fields = {
    state,
    locked_by: null,
    locked_at: null,
    updated_at: now,
    ...overrides,
  };

  // If the job is 'completed' or 'dead', set its completion time.
  if (state === 'completed' || state === 'dead') {
    fields.completed_at = now;
  }

  const setClauses = Object.keys(fields).map(key => `${key} = ?`).join(', ');
  const values = Object.values(fields);

  await db.run(
    `UPDATE jobs SET ${setClauses} WHERE id = ?`,
    [...values, id]
  );
}
// Start the worker
runWorker().catch(err => console.error(`[${workerId}] Fatal error:`, err));