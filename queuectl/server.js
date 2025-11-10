import express from 'express';
import { initDb, getDb } from './db.js';
import cors from 'cors';

const app = express();
const PORT = 4000;

// 1. Await the database initialization at the top level
console.log('[Server] Initializing database...');
await initDb();
console.log('[Server] Database initialized.');



// 2. Define the API endpoint

app.use(cors());
app.get('/api/jobs', async (req, res) => {
  let db;
  try {
    db = getDb();
  } catch (err) {
    console.error('[Server] CRITICAL: getDb() failed.', err.message);
    return res.status(500).json({ error: 'Failed to get DB connection.', message: err.message });
  }

  try {
    // Get all jobs, ordered by most recently updated
    const jobs = await db.all('SELECT * FROM jobs ORDER BY updated_at DESC');
    
    // Get the status summary
    const summary = await db.all('SELECT state, COUNT(*) as count FROM jobs GROUP BY state');
    
    // Send all data as a JSON object
    res.json({
      summary,
      jobs,
    });
  } catch (err) {
    console.error('[Server] Error during /api/jobs database query:', err.message);
    res.status(500).json({ error: 'Database query failed.', message: err.message });
  }
});

// 3. Start the server
app.listen(PORT, () => {
  console.log(`Dashboard server running at http://localhost:${PORT}`);
});