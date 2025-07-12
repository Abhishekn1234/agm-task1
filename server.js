const fs = require('fs');
const path = require('path');
const { Parser } = require('sql-ddl-to-json-schema');
const mongoose = require('mongoose');
require('dotenv').config();


const config = {
  mongodbUri:'mongodb://localhost:27017/db',
  sqlFilePath: path.join(__dirname, 'shops.sql'),
  clearCollections: true,       
  batchSize: 500,              
  reportInterval: 1000,       
  logLevel: 'verbose',        
  timeout: 30000           
};


const logger = {
  verbose: (...args) => config.logLevel === 'verbose' && console.log('[VERBOSE]', ...args),
  info: (...args) => (config.logLevel === 'verbose' || config.logLevel === 'info') && console.log(...args),
  error: (...args) => console.error('❌', ...args)
};

// Enhanced type mapping
function getMongooseType(sqlType, columnDef) {
  if (!sqlType) return mongoose.Schema.Types.Mixed;

  sqlType = sqlType.toString().toLowerCase().split('(')[0].trim();

  const typeMap = {
   
    int: { type: Number },
    integer: { type: Number },
    smallint: { type: Number },
    mediumint: { type: Number },
    bigint: { type: Number },
    
   
    float: { type: Number },
    double: { type: Number },
    real: { type: Number },
    

    decimal: { type: mongoose.Types.Decimal128 },
    numeric: { type: mongoose.Types.Decimal128 },
   
    varchar: { type: String },
    char: { type: String },
    text: { type: String },
    tinytext: { type: String },
    mediumtext: { type: String },
    longtext: { type: String },
    
   
    blob: { type: Buffer },
    tinyblob: { type: Buffer },
    mediumblob: { type: Buffer },
    longblob: { type: Buffer },
    binary: { type: Buffer },
    varbinary: { type: Buffer },
    
 
    date: { type: Date },
    datetime: { type: Date },
    timestamp: { type: Date },
    time: { type: Date },
    year: { type: Date },
    
    
    tinyint: {
      type: (columnDef.column?.def?.length?.value === '1') ? Boolean : Number
    },
    
    
    json: { type: mongoose.Schema.Types.Mixed },
    
  
    enum: { type: String, enum: columnDef.column?.def?.values?.map(v => v.value.replace(/'/g, '')) },
    set: { type: [String] }
  };

  return typeMap[sqlType] || { type: mongoose.Schema.Types.Mixed };
}

async function connectToMongoDB() {
  try {
    mongoose.set('strictQuery', false);
    await mongoose.connect(config.mongodbUri, {
      connectTimeoutMS: config.timeout,
      socketTimeoutMS: config.timeout,
      serverSelectionTimeoutMS: config.timeout
    });
    logger.info('✅ Connected to MongoDB');
  } catch (err) {
    logger.error('MongoDB connection error:', err.message);
    process.exit(1);
  }
}

function cleanSQL(sqlContent) {
  return sqlContent
    .split('\n')
    .filter(line => {
      const l = line.trim().toLowerCase();
      return !l.startsWith('--') &&
             !l.startsWith('/*!') &&
             !l.startsWith('drop') &&
             !l.startsWith('set') &&
             !l.startsWith('use') &&
             !l.startsWith('lock') &&
             !l.startsWith('unlock') &&
             l !== '';
    })
    .join('\n');
}

async function processTables(ddlSQL) {
  const parser = new Parser('mysql');
  try {
    parser.feed(ddlSQL);
  } catch (err) {
    logger.error('SQL Parse Error:', err.message);
    throw err;
  }

  const tables = parser.results.def || [];
  const models = {};
  const indexOperations = [];

  for (const table of tables) {
    try {
      const tableDef = table.def?.def?.def;
      if (!tableDef?.table) continue;

      const tableName = tableDef.table;
      const columnsDef = tableDef.columnsDef?.def || [];
      const indexes = tableDef.indexes || [];

    const schemaDefinition = {};
      for (const col of columnsDef) {
        const colDef = col.def;
        if (!colDef?.column?.name) continue;

        const name = colDef.column.name;
        const typeDef = colDef.column.def?.datatype?.def || colDef.column.def?.datatype;
        
        schemaDefinition[name] = getMongooseType(typeDef, colDef);
        
       
        if (colDef.column?.def?.options) {
          const opts = colDef.column.def.options;
          if (opts.required) schemaDefinition[name].required = true;
          if (opts.default !== undefined) schemaDefinition[name].default = opts.default.value;
          if (opts.unique) schemaDefinition[name].unique = true;
        }
      }

      
      const schema = new mongoose.Schema(schemaDefinition, {
        versionKey: false,
        timestamps: false,
        strict: false
      });

      
      for (const idx of indexes) {
        const idxDef = idx.def;
        if (idxDef?.type?.toLowerCase() === 'primary') continue;
        
        const fields = {};
        idxDef.columns.forEach(col => {
          fields[col.def.column.name] = 1;
        });
        
        indexOperations.push({
          modelName: tableName,
          index: {
            fields,
            options: {
              unique: idxDef.type?.toLowerCase() === 'unique',
              name: idxDef.name
            }
          }
        });
      }

      const modelName = tableName.charAt(0).toUpperCase() + tableName.slice(1);
      models[tableName] = mongoose.model(modelName, schema);
      logger.info(`🔹 Created model for table: ${tableName}`);

    } catch (err) {
      logger.error(`Error processing table:`, err.message);
    }
  }

  
  for (const op of indexOperations) {
    try {
      await models[op.modelName].createIndexes([op.index]);
      logger.verbose(`  ↳ Created index ${op.index.options.name} on ${op.modelName}`);
    } catch (err) {
      logger.error(`Index creation failed for ${op.modelName}:`, err.message);
    }
  }

  return models;
}

async function processInserts(insertSQL, models) {
  const insertRegex = /INSERT\s+INTO\s+`?([\w_]+)`?\s*(?:\(([^)]+)\))?\s*VALUES\s*((?:\([^)]*\),?\s*)+)/gi;
  let match;
  let total = 0;
  let errors = 0;
  let lastReport = 0;

  const processValue = (val) => {
    val = val.trim();
    if (val.toLowerCase() === 'null') return null;
    if (val.startsWith("'") && val.endsWith("'")) return val.slice(1, -1).replace(/''/g, "'");
    if (val.startsWith("0x")) return Buffer.from(val.slice(2), 'hex');
    if (!isNaN(val)) return Number(val);
    return val;
  };

 
  if (config.clearCollections) {
    logger.info('🧹 Clearing existing collections...');
    for (const table in models) {
      try {
        await models[table].deleteMany({});
        logger.verbose(`  ↳ Cleared collection ${table}`);
      } catch (err) {
        logger.error(`Failed to clear ${table}:`, err.message);
      }
    }
  }

  
  while ((match = insertRegex.exec(insertSQL)) !== null) {
    const [, table, columnsStr = '', valuesStr] = match;
    if (!models[table]) {
      logger.verbose(`⚠️ No model found for table: ${table}`);
      continue;
    }

    const columns = columnsStr 
      ? columnsStr.split(',').map(c => c.trim().replace(/`/g, ''))
      : null;
    
    const valueGroups = valuesStr.match(/\([^)]+\)/g) || [];
    const batch = [];

    for (const group of valueGroups) {
      try {
        const rawValues = group
          .slice(1, -1)
          .split(/(?<!\\),/)
          .map(v => v.trim());

        const values = rawValues.map(processValue);
        const doc = {};

        if (columns) {
          columns.forEach((col, i) => {
            if (i < values.length) doc[col] = values[i];
          });
        } else {
          // Handle INSERTs without column list (assume all columns in order)
          Object.keys(models[table].schema.paths).forEach((key, i) => {
            if (i < values.length) doc[key] = values[i];
          });
        }

        batch.push(doc);
      } catch (err) {
        logger.verbose(`Value parsing error: ${err.message}`);
        errors++;
      }
    }


    for (let i = 0; i < batch.length; i += config.batchSize) {
      const batchChunk = batch.slice(i, i + config.batchSize);
      try {
        await models[table].insertMany(batchChunk, { ordered: false });
        total += batchChunk.length;
        
   
        if (Date.now() - lastReport > config.reportInterval) {
          logger.info(`📊 Progress: ${total} documents inserted (${errors} errors)`);
          lastReport = Date.now();
        }
      } catch (err) {
        errors += err.writeErrors?.length || 1;
        logger.verbose(`Insert error (${table}):`, err.message);
      }
    }
  }

  return { total, errors };
}

async function main() {
  try {
    logger.info('🚀 Starting SQL to MongoDB migration');
    logger.verbose('Configuration:', JSON.stringify(config, null, 2));

   
    logger.info(`📂 Loading SQL file: ${config.sqlFilePath}`);
    const sql = fs.readFileSync(config.sqlFilePath, 'utf8');
    const cleaned = cleanSQL(sql);
    
   const statements = cleaned.split(';').filter(s => s.trim());
    const ddl = statements.filter(s => !s.trim().toLowerCase().startsWith('insert into')).join(';');
    const inserts = statements.filter(s => s.trim().toLowerCase().startsWith('insert into')).join(';');

   
    await connectToMongoDB();

    logger.info('🛠️  Processing database schema...');
    const models = await processTables(ddl);
    
    logger.info('💾 Importing data...');
    const { total, errors } = await processInserts(inserts, models);

 
    logger.info('\n🎉 Migration completed!');
    logger.info(`   Documents inserted: ${total}`);
    logger.info(`   Errors encountered: ${errors}`);

  } catch (err) {
    logger.error('Migration failed:', err);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
    process.exit();
  }
}
if (process.argv[2]) {
  config.sqlFilePath = path.resolve(process.argv[2]);
}
if (process.argv[3]) {
  config.mongodbUri = process.argv[3];
}

main();