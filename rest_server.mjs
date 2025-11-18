import * as path from 'node:path';
import * as url from 'node:url';

import { default as express } from 'express';
import { default as sqlite3 } from 'sqlite3';

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const db_filename = path.join(__dirname, 'db', 'stpaul_crime.sqlite3');

const port = 8000;

let app = express();
app.use(express.json());

/********************************************************************
 ***   DATABASE FUNCTIONS                                         *** 
 ********************************************************************/
// Open SQLite3 database (in read-write mode)
let db = new sqlite3.Database(db_filename, sqlite3.OPEN_READWRITE, (err) => {
    if (err) {
        console.log('Error opening ' + path.basename(db_filename));
    }
    else {
        console.log('Now connected to ' + path.basename(db_filename));
    }
});

// Create Promise for SQLite3 database SELECT query 
function dbSelect(query, params) {
    return new Promise((resolve, reject) => {
        db.all(query, params, (err, rows) => {
            if (err) {
                reject(err);
            }
            else {
                resolve(rows);
            }
        });
    });
}

// Create Promise for SQLite3 database INSERT or DELETE query
function dbRun(query, params) {
    return new Promise((resolve, reject) => {
        db.run(query, params, (err) => {
            if (err) {
                reject(err);
            }
            else {
                resolve();
            }
        });
    });
}

/********************************************************************
 ***   REST REQUEST HANDLERS                                      *** 
 ********************************************************************/
// GET request handler for crime codes
app.get('/codes', async (req, res) => {
    console.log(req.query);
    
    try {
        let query = 'SELECT code, incident_type FROM Codes';
        let params = [];
        
        // Check if code filter is provided
        if (req.query.code) {
            let codes = req.query.code.split(',').map(c => c.trim());
            let placeholders = codes.map(() => '?').join(',');
            query += ` WHERE code IN (${placeholders})`;
            params = codes;
        }
        
        query += ' ORDER BY code';
        
        let rows = await dbSelect(query, params);
        
        let result = rows.map(row => ({
            code: row.code,
            type: row.incident_type
        }));
        
        res.status(200).type('json').send(result);
    } catch (error) {
        console.error(error);
        res.status(500).type('txt').send('Error retrieving codes');
    }
});

// GET request handler for neighborhoods
app.get('/neighborhoods', async (req, res) => {
    console.log(req.query);
    
    try {
        let query = 'SELECT neighborhood_number, neighborhood_name FROM Neighborhoods';
        let params = [];
        
        // Check if id filter is provided
        if (req.query.id) {
            let ids = req.query.id.split(',').map(id => id.trim());
            let placeholders = ids.map(() => '?').join(',');
            query += ` WHERE neighborhood_number IN (${placeholders})`;
            params = ids;
        }
        
        query += ' ORDER BY neighborhood_number';
        
        let rows = await dbSelect(query, params);
        
        let result = rows.map(row => ({
            id: row.neighborhood_number,
            name: row.neighborhood_name
        }));
        
        res.status(200).type('json').send(result);
    } catch (error) {
        console.error(error);
        res.status(500).type('txt').send('Error retrieving neighborhoods');
    }
});

// GET request handler for crime incidents
app.get('/incidents', async (req, res) => {
    console.log(req.query);
    
    try {
        let query = `
            SELECT case_number, 
                   DATE(date_time) as date, 
                   TIME(date_time) as time,
                   code, 
                   incident, 
                   police_grid, 
                   neighborhood_number, 
                   block
            FROM Incidents
        `;
        
        let whereClauses = [];
        let params = [];
        
        // Filter by start_date
        if (req.query.start_date) {
            whereClauses.push('date(date_time) >= ?');
            params.push(req.query.start_date);
        }
        
        // Filter by end_date
        if (req.query.end_date) {
            whereClauses.push('date(date_time) <= ?');
            params.push(req.query.end_date);
        }
        
        // Filter by code
        if (req.query.code) {
            let codes = req.query.code.split(',').map(c => c.trim());
            let placeholders = codes.map(() => '?').join(',');
            whereClauses.push(`code IN (${placeholders})`);
            params.push(...codes);
        }
        
        // Filter by grid
        if (req.query.grid) {
            let grids = req.query.grid.split(',').map(g => g.trim());
            let placeholders = grids.map(() => '?').join(',');
            whereClauses.push(`police_grid IN (${placeholders})`);
            params.push(...grids);
        }
        
        // Filter by neighborhood
        if (req.query.neighborhood) {
            let neighborhoods = req.query.neighborhood.split(',').map(n => n.trim());
            let placeholders = neighborhoods.map(() => '?').join(',');
            whereClauses.push(`neighborhood_number IN (${placeholders})`);
            params.push(...neighborhoods);
        }
        
        // Add WHERE clauses if any filters were provided
        if (whereClauses.length > 0) {
            query += ' WHERE ' + whereClauses.join(' AND ');
        }
        
        // Always order by date_time descending
        query += ' ORDER BY date_time DESC';
        
        // Apply limit (default 1000)
        let limit = req.query.limit ? parseInt(req.query.limit) : 1000;
        query += ` LIMIT ${limit}`;
        
        let rows = await dbSelect(query, params);
        
        let result = rows.map(row => ({
            case_number: row.case_number,
            date: row.date,
            time: row.time,
            code: row.code,
            incident: row.incident,
            police_grid: row.police_grid,
            neighborhood_number: row.neighborhood_number,
            block: row.block
        }));
        
        res.status(200).type('json').send(result);
    } catch (error) {
        console.error(error);
        res.status(500).type('txt').send('Error retrieving incidents');
    }
});

// PUT request handler for new crime incident
app.put('/new-incident', async (req, res) => {
    console.log(req.body);
    
    try {
        // First check if case_number already exists
        let checkQuery = 'SELECT case_number FROM Incidents WHERE case_number = ?';
        let existing = await dbSelect(checkQuery, [req.body.case_number]);
        
        if (existing.length > 0) {
            res.status(500).type('txt').send('Case number already exists');
            return;
        }
        
        // Insert new incident
        let insertQuery = `
            INSERT INTO Incidents (case_number, date_time, code, incident, police_grid, neighborhood_number, block)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `;
        let dateTime = req.body.date + 'T' + req.body.time;
        let params = [
            req.body.case_number,
            dateTime,
            req.body.code,
            req.body.incident,
            req.body.police_grid,
            req.body.neighborhood_number,
            req.body.block
        ];
        
        await dbRun(insertQuery, params);
        res.status(200).type('txt').send('OK');
    } catch (error) {
        console.error(error);
        res.status(500).type('txt').send('Error inserting incident');
    }
});

// DELETE request handler for crime incident
app.delete('/remove-incident', async (req, res) => {
    console.log(req.body);
    
    try {
        // First check if case_number exists
        let checkQuery = 'SELECT case_number FROM Incidents WHERE case_number = ?';
        let existing = await dbSelect(checkQuery, [req.body.case_number]);
        
        if (existing.length === 0) {
            res.status(500).type('txt').send('Case number does not exist');
            return;
        }
        
        // Delete incident
        let deleteQuery = 'DELETE FROM Incidents WHERE case_number = ?';
        await dbRun(deleteQuery, [req.body.case_number]);
        
        res.status(200).type('txt').send('OK');
    } catch (error) {
        console.error(error);
        res.status(500).type('txt').send('Error deleting incident');
    }
});

/********************************************************************
 ***   START SERVER                                               *** 
 ********************************************************************/
// Start server - listen for client connections
app.listen(port, () => {
    console.log('Now listening on port ' + port);
});