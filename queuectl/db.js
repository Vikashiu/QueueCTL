import { open } from 'sqlite';
import sqlite3 from 'sqlite3';

// This is the heart of your persistence.
const DB_FILE = './queue.db';

let db = null;

/**
 * Opens a connection to the SQLite database.
 * If the database/tables don't exist, it creates them.
 */
export async function initDb() {
  if (db) {
    return db;
  }

  try {
    db = await open({
      filename: DB_FILE,
      driver: sqlite3.Database,
    });

    // These SQL commands are the "schema" for your system.
    // The "locked_by" and "run_at" fields are CRITICAL for
    // the worker and backoff logic.
    // ... in db.js
    await db.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        priority INTEGER NOT NULL DEFAULT 0,
        stdout TEXT,
        stderr TEXT,

        -- THESE ARE THE NEW COLUMNS --
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        started_at DATETIME,
        completed_at DATETIME,

        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        run_at DATETIME DEFAULT CURRENT_TIMESTAMP,  
        locked_by TEXT,                           
        locked_at DATETIME
      );
    `);


    // A simple key-value store for configuration
    await db.exec(`
      CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
      );
    `);

    // Insert default config if not present
    await db.run(
      "INSERT OR IGNORE INTO config (key, value) VALUES ('max_retries', '3')"
    );
    await db.run(
      "INSERT OR IGNORE INTO config (key, value) VALUES ('backoff_base', '2')"
    );

    // console.log('Database initialized successfully.');
    return db;
  } catch (err) {
    console.error('Failed to initialize database:', err);
    process.exit(1);
  }
}

/**
 * Returns the database instance.
 * Throws an error if initDb() hasn't been called.
 */
export function getDb() {
  if (!db) {
    throw new Error('Database not initialized! Call initDb() first.');
  }
  return db;
}