require("dotenv").config();

const { MongoClient } = require("mongodb");
const sql = require("mssql");

const BATCH_SIZE = 100;

async function run() {
  const mongo = new MongoClient(process.env.MONGO_URI);
  await mongo.connect();

  const db = mongo.db(process.env.MONGO_DB);

  const start = new Date("2026-01-01T00:00:00Z");
  const end = new Date("2027-01-01T00:00:00Z");

  const cursor = db.collection("user-sessions").find({
    createdAt: { $gte: start, $lt: end },
  });

  console.log("Mongo cursor opened");

  const sqlConfig = {
    server: process.env.SQL_SERVER,
    port: parseInt(process.env.SQL_PORT),
    database: process.env.SQL_DATABASE,
    user: process.env.SQL_USER,
    password: process.env.SQL_PASSWORD,
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
  };

  const pool = await sql.connect(sqlConfig);

  let processed = 0;

  let sessionRows = [];
  let answerRows = [];
  let benefitRows = [];
  let logRows = [];

  async function flushBatch() {
    if (sessionRows.length === 0) return;

    console.log(`Flushing batch: ${sessionRows.length} sessions`);

    const sessionTable = new sql.Table("raw_sessions");
    sessionTable.create = false;

    sessionTable.columns.add("mongo_id", sql.NVarChar(50));
    sessionTable.columns.add("created_at", sql.DateTime);
    sessionTable.columns.add("locale", sql.NVarChar(10));
    sessionTable.columns.add("city", sql.NVarChar(100));
    sessionTable.columns.add("province", sql.NVarChar(10));
    sessionTable.columns.add("referrer", sql.NVarChar(300));
    sessionTable.columns.add("questionnaire_count", sql.Int);
    sessionTable.columns.add("email_count", sql.Int);
    sessionTable.columns.add("print_count", sql.Int);

    sessionRows.forEach((r) => sessionTable.rows.add(...r));

    // const answersTable = new sql.Table("raw_session_answers");
    // answersTable.create = false;

    // answersTable.columns.add("session_mongo_id", sql.NVarChar(50));
    // answersTable.columns.add("question_key", sql.NVarChar(50));
    // answersTable.columns.add("answer_value", sql.NVarChar(50));

    const answersTable = new sql.Table("dbo.raw_session_answers");
    answersTable.create = false;

    answersTable.columns.add("session_mongo_id", sql.NVarChar(50), {
      nullable: false,
    });
    answersTable.columns.add("question_key", sql.NVarChar(50), {
      nullable: false,
    });
    answersTable.columns.add("answer_value", sql.NVarChar(50), {
      nullable: true,
    });

    answerRows.forEach((r) => answersTable.rows.add(...r));

    const benefitsTable = new sql.Table("raw_session_benefits");
    benefitsTable.create = false;

    benefitsTable.columns.add("session_mongo_id", sql.NVarChar(50));
    benefitsTable.columns.add("benefit_id", sql.NVarChar(30));
    benefitsTable.columns.add("is_recommended", sql.Bit);
    benefitsTable.columns.add("priority", sql.Int);
    benefitsTable.columns.add("select_status", sql.Int);

    benefitRows.forEach((r) => benefitsTable.rows.add(...r));

    const logTable = new sql.Table("raw_session_activity_log");
    logTable.create = false;

    logTable.columns.add("session_mongo_id", sql.NVarChar(50));
    logTable.columns.add("type", sql.Int);
    logTable.columns.add("value", sql.NVarChar(200));
    logTable.columns.add("description", sql.NVarChar(300));
    logTable.columns.add("created_date", sql.DateTime);

    logRows.forEach((r) => logTable.rows.add(...r));

    const transaction = new sql.Transaction(pool);

    try {
      await transaction.begin();
      const request = new sql.Request(transaction);

      await request.bulk(sessionTable);

      if (answerRows.length) await request.bulk(answersTable);

      if (benefitRows.length) await request.bulk(benefitsTable);

      if (logRows.length) await request.bulk(logTable);

      await transaction.commit();
    } catch (err) {
      console.error("SQL BULK ERROR:");
      console.error(err);
      await transaction.rollback();
      throw err;
    }

    sessionRows = [];
    answerRows = [];
    benefitRows = [];
    logRows = [];
  }

  try {
    for await (const s of cursor) {
      const sessionId = s._id.toString();

      sessionRows.push([
        String(sessionId),
        s.createdAt ? new Date(s.createdAt) : null,
        s.locale || null,
        s.city || null,
        s.province || null,
        s.referrer || null,
        Number(s.questionnaireCount || 0),
        Number(s.emailCount || 0),
        Number(s.printCount || 0),
      ]);

      const answers = s.answers || {};

      for (const key of Object.keys(answers)) {
        if (key === "_id") continue;

        const value = answers[key];

        if (Array.isArray(value)) {
          for (const v of value) {
            answerRows.push([
              String(sessionId),
              key,
              v != null ? String(v) : null,
            ]);
          }
        } else {
          answerRows.push([
            String(sessionId),
            key,
            value != null ? String(value) : null,
          ]);
        }
      }

      for (const b of s.benefits || []) {
        benefitRows.push([
          String(sessionId),
          b.benefitId ? String(b.benefitId) : null,
          b.isRecommended ? 1 : 0,
          Number(b.priority || 0),
          Number(b.selectStatus || 0),
        ]);
      }

      for (const log of s.activityLog || []) {
        logRows.push([
          String(sessionId),
          Number(log.type || 0),
          log.value ? String(log.value) : null,
          log.description ? String(log.description) : null,
          log.createdDate ? new Date(log.createdDate) : null,
        ]);
      }

      processed++;

      if (processed % BATCH_SIZE === 0) {
        await flushBatch();
        console.log(`Processed ${processed} sessions`);
      }
    }

    await flushBatch();

    console.log(`Migration complete. Total sessions: ${processed}`);
  } finally {
    await pool.close();
    await mongo.close();
  }
}

run().catch((err) => {
  console.error("ETL failed:", err);
});
