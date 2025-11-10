#!/usr/bin/env node

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { initDb, getDb } from './db.js';
import { Worker } from 'worker_threads';
import path from 'path';
import { fileURLToPath } from 'url';

import Table from 'cli-table3';
import chalk from 'chalk';

// __dirname replacement in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const activeWorkers = [];

async function main() {
  // Initialize DB first
  await initDb(); 

  yargs(hideBin(process.argv))
    
    // 1. Enqueue Command (with Priority and Delay)
    
    .command(
      'enqueue',
      'Add a new job to the queue',
      (yargs) => {
        return yargs
          .option('id', { describe: 'Unique Job ID', type: 'string', demandOption: true })
          .option('command', { describe: 'The shell command to run', type: 'string', demandOption: true })
          .option('max_retries', { describe: 'Optional: Max retries for this job', type: 'number' })
          .option('priority', {
            describe: 'Job priority (higher number = higher priority)',
            type: 'number',
            default: 0
          })
          
          .option('delay', {
            describe: 'Delay job execution by N seconds',
            type: 'number',
            default: 0 
          });
      },
      async (argv) => {
        const db = getDb();
        try {
          const jobData = { id: argv.id, command: argv.command };
          const { value: default_max_retries } = await db.get("SELECT value FROM config WHERE key = 'max_retries'");
          
          const runAtTime = new Date(Date.now() + (argv.delay * 1000)).toISOString();

          const job = {
            max_retries: argv.max_retries || parseInt(default_max_retries, 10),
            ...jobData,
            state: 'pending',
            attempts: 0,
            priority: argv.priority,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            run_at: runAtTime,
          };

          await db.run(
            `INSERT INTO jobs (id, command, state, attempts, max_retries, priority, created_at, updated_at, run_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            job.id, job.command, job.state, job.attempts, job.max_retries,
            job.priority, job.created_at, job.updated_at, job.run_at
          );
          
          if (argv.delay > 0) {
            console.log(`[OK] Job enqueued: ${job.id} (priority: ${job.priority}). Will run after ${argv.delay} seconds.`);
          } else {
            console.log(`[OK] Job enqueued: ${job.id} (priority: ${job.priority}).`);
          }

        } catch (err) {
          if (err.code === 'SQLITE_CONSTRAINT') {
             console.error(`[Error] A job with ID "${argv.id}" already exists.`);
          } else {
             console.error(`[Error] ${err.message}`);
          }
        }
      }
    )
    
    // 2. Worker Command Group
  
    .command(
      'worker <action>',
      'Manage worker processes',
      (yargs) => {
        yargs
          .command(
            'start',
            'Start worker processes',
            (yargs) => {
              return yargs.option('count', {
                alias: 'c',
                type: 'number',
                default: 1,
                describe: 'Number of workers to start',
              });
            },
            (argv) => {
              console.log(`Starting ${argv.count} worker(s)...`);
              for (let i = 0; i < argv.count; i++) {
                const worker = new Worker(path.resolve(__dirname, 'worker.js'));
                activeWorkers.push(worker);
                worker.on('exit', () => console.log(`Worker ${worker.threadId} exited.`));
                worker.on('error', (err) => console.error(`Worker ${worker.threadId} error:`, err));
              }
              console.log(`${argv.count} worker(s) started.`);
              setInterval(() => {}, 1 << 30);
            }
          )
          .command(
            'stop',
            'Stop running workers gracefully (Not implemented)',
            () => {
              console.log('Sending stop signal to workers...');
              activeWorkers.forEach(worker => worker.postMessage('stop'));
              console.log("Stop signal sent. Workers will exit after their current job.");
              setTimeout(() => process.exit(0), 2000);
            }
          );
      }
    )
    
    // 3. Status Command
    
    .command(
      'status',
      'Show summary of all job states',
      async () => {
        const db = getDb();
        try {
          const results = await db.all("SELECT state, COUNT(*) as count FROM jobs GROUP BY state");
          console.log('Job Status Summary:');
          console.table(results);
        } catch (err) {
          console.error(`[Error] ${err.message}`);
        }
      }
    )
    
    // 4. List Command
    
    .command(
      'list',
      'List jobs. By default, lists ALL jobs. Use --state to filter.',
      (yargs) => {
        return yargs.option('state', {
          alias: 's',
          type: 'string',
          describe: 'Optional: Filter jobs by state (pending, completed, dead, etc.)',
        });
      },
      async (argv) => {
        const db = getDb();
        try {
          let query = 'SELECT id, state, attempts, command, created_at, updated_at FROM jobs';
          const params = [];
          
          if (argv.state) {
            query += ' WHERE state = ?';
            params.push(argv.state);
          }
          
          query += ' ORDER BY updated_at DESC';

          const rows = await db.all(query, params);
          
          if (rows.length === 0) {
            if (argv.state) {
              console.log(chalk.yellow(`\nNo jobs found with state: ${argv.state}\n`));
            } else {
              console.log(chalk.yellow('\nNo jobs found in the queue.\n'));
            }
            return;
          }

          
          const table = new Table({
            head: [
              chalk.cyan('ID'),
              chalk.cyan('State'),
              chalk.cyan('Attempts'),
              chalk.cyan('Command'),
              chalk.cyan('Updated At')
            ],
            colWidths: [20, 15, 10, 40, 30],
            wordWrap: true, // Wrap long commands
          });

          //function to color-code the state
          const colorState = (state) => {
            switch (state) {
              case 'pending':
                return chalk.yellow(state);
              case 'completed':
                return chalk.green(state);
              case 'failed':
                return chalk.magenta(state);
              case 'dead':
                return chalk.red(state);
              case 'processing':
                return chalk.blue(state);
              default:
                return state;
            }
          };

          rows.forEach(job => {
            table.push([
              job.id,
              colorState(job.state),
              job.attempts,
              job.command,
              new Date(job.created_at).toLocaleString()
            ]);
          });

          console.log(table.toString());
          

        } catch (err) {
          console.error(`[Error] ${err.message}`);
        }
      }
    )
    
    // 5. DLQ Command Group
    
    .command(
      'dlq <action> [jobId]',
      'Manage the Dead Letter Queue',
      (yargs) => {
        yargs
          .command(
            'list',
            'List all jobs in the DLQ',
            async () => {
              const db = getDb();
              try {
                const rows = await db.all(
                  "SELECT id, command, state, attempts, updated_at FROM jobs WHERE state = 'dead' ORDER BY updated_at DESC"
                );
                if (rows.length === 0) {
                  console.log('DLQ is empty.');
                  return;
                }
                console.log('Dead Letter Queue:');
                console.table(rows);
              } catch (err) {
                console.error(`[Error] ${err.message}`);
              }
            }
          )
          .command(
            'retry <jobId>',
            'Retry a specific job from the DLQ',
            (yargs) => {
              return yargs.positional('jobId', {
                describe: 'The ID of the job to retry',
                type: 'string',
              });
            },
            async (argv) => {
              const db = getDb();
              try {
                const result = await db.run(
                  `UPDATE jobs
                   SET
                     state = 'pending',
                     attempts = 0,
                     run_at = CURRENT_TIMESTAMP,
                     updated_at = CURRENT_TIMESTAMP,
                     locked_by = NULL
                   WHERE id = ? AND state = 'dead'`,
                  argv.jobId
                );
                if (result.changes === 0) {
                  console.log(`[Warning] No job found in DLQ with ID: ${argv.jobId}`);
                } else {
                  console.log(`[OK] Job ${argv.jobId} moved back to queue for retry.`);
                }
              } catch (err) {
                console.error(`[Error] ${err.message}`);
              }
            }
          );
      }
    )

    // 6. Config Command Group

    .command(
      'config <action>', // Define the group
      'Manage configuration',
      (yargs) => {
        // Subcommand 1: list
        yargs.command(
          'list', // The action
          'List all config values',
          () => {}, 
          async () => {
            const db = getDb(); 
            try {
              const results = await db.all("SELECT * FROM config");
              console.table(results);
            } catch (err) {
              console.error(`[Error] ${err.message}`);
            }
          }
        )
        // Subcommand 2: set
        .command(
          'set <key> <value>', // The action + args
          'Set a config value',
          (yargs) => { // Builder for set
            return yargs
              .positional('key', { type: 'string', describe: 'The config key (e.g., max_retries)' })
              .positional('value', { type: 'string', describe: 'The new value' });
          },
          async (argv) => { // Handler for set
            const db = getDb(); 
            try {
              const result = await db.run(
                'UPDATE config SET value = ? WHERE key = ?',
                argv.value, argv.key
              );
              if (result.changes === 0) {
                console.log(`[Warning] Config key "${argv.key}" not found.`);
              } else {
                console.log(`[OK] Config updated: ${argv.key} = ${argv.value}`);
              }
            } catch (err) {
              console.error(`[Error] ${err.message}`);
            }
          }
        );
      }
    )
    
    // 7. Log Command (NEW)
    
    .command(
      'log <jobId>', 
      'View the saved stdout/stderr for a job',
      (yargs) => { // Builder
        return yargs
          .positional('jobId', { type: 'string', describe: 'The ID of the job to view' });
      },
      async (argv) => { // Handler
        const db = getDb();
        try {
          const job = await db.get("SELECT id, state, stdout, stderr FROM jobs WHERE id = ?", argv.jobId);
          if (!job) {
            console.error(`[Error] No job found with ID: ${argv.jobId}`);
            return;
          }

          console.log(chalk.cyan('---------------------------------'));
          console.log(chalk.cyan.bold(`Logs for Job: ${job.id}`));
          console.log(chalk.cyan.bold(`State: ${job.state}`));
          console.log(chalk.cyan('---------------------------------'));
          
          console.log(chalk.green.bold('\n--- STDOUT ---'));
          console.log(job.stdout || '(empty)');
          
          console.log(chalk.red.bold('\n--- STDERR ---'));
          console.log(job.stderr || '(empty)');
          console.log(chalk.cyan('\n---------------------------------'));

        } catch (err) {
          console.error(`[Error] ${err.message}`);
        }
      }
    )
    // 8. Stats Command (NEW)
    .command(
      'stats',
      'Show execution stats and metrics',
      async () => {
        const db = getDb();
        try {
          console.log(chalk.cyan.bold('\n--- QueueCTL Metrics ---'));
          // Job Counts
          const counts = await db.all("SELECT state, COUNT(*) as count FROM jobs GROUP BY state");
          console.log(chalk.cyan('\nJob Counts:'));
          if (counts.length === 0) {
            console.log('  No jobs in queue.');
          } else {
            counts.forEach(c => {
              console.log(`  - ${c.state}: ${c.count}`);
            });
          }

          // Get Avg Execution Time
          // used julianday to calculate the difference in seconds
          const result = await db.get(
            `SELECT AVG(julianday(completed_at) - julianday(started_at)) * 86400.0 as avg_time
             FROM jobs 
             WHERE state = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL`
          );
          
          console.log(chalk.cyan('\nExecution Stats:'));
          if (result && result.avg_time !== null) {
            console.log(`  - Avg. Job Duration (Completed): ${result.avg_time.toFixed(2)} seconds`);
          } else {
            console.log('  - Avg. Job Duration: N/A (No completed jobs with full stats).');
          }

        } catch (err) {
          console.error(`[Error] ${err.message}`);
        }
      }
    )
    
    .demandCommand(1, 'You must provide a command.\nRun --help to see all commands.')
    .help()
    .strict() // Catches unknown commands
    .argv;
}

main().catch(console.error);