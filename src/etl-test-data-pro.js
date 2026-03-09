require("dotenv").config();

const { MongoClient } = require("mongodb");
const sql = require("mssql");

async function run() {
  const mongo = new MongoClient(process.env.MONGO_URI);
  await mongo.connect();

  const db = mongo.db(process.env.MONGO_DB);

  const start = new Date("2026-01-01T00:00:00Z");
  const end = new Date("2027-01-01T00:00:00Z");

  const sessions = await db
    .collection("user-sessions")
    .find({
      createdAt: { $gte: start, $lt: end },
    })
    .limit(50)
    .toArray();

  console.log("Mongo documents fetched:", sessions.length);

  const config = {
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

  const pool = await sql.connect(config);

  // -------------------------
  // Prepare Bulk Tables
  // -------------------------

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

  const benefitsTable = new sql.Table("raw_session_benefits");
  benefitsTable.create = false;

  benefitsTable.columns.add("session_mongo_id", sql.NVarChar(50));
  benefitsTable.columns.add("benefit_id", sql.NVarChar(30));
  benefitsTable.columns.add("is_recommended", sql.Bit);
  benefitsTable.columns.add("priority", sql.Int);
  benefitsTable.columns.add("select_status", sql.Int);

  const logTable = new sql.Table("raw_session_activity_log");
  logTable.create = false;

  logTable.columns.add("session_mongo_id", sql.NVarChar(50));
  logTable.columns.add("type", sql.Int);
  logTable.columns.add("value", sql.NVarChar(200));
  logTable.columns.add("description", sql.NVarChar(300));
  logTable.columns.add("created_date", sql.DateTime);

  // -------------------------
  // Flatten Mongo
  // -------------------------

  for (const s of sessions) {
    const sessionId = s._id.toString();

    sessionTable.rows.add(
      String(sessionId),
      s.createdAt ? new Date(s.createdAt) : null,
      s.locale || null,
      s.city || null,
      s.province || null,
      s.referrer || null,
      Number(s.questionnaireCount || 0),
      Number(s.emailCount || 0),
      Number(s.printCount || 0),
    );
    // sessionTable.rows.add(
    //   sessionId,
    //   s.createdAt,
    //   s.locale,
    //   s.city,
    //   s.province,
    //   s.referrer,
    //   s.questionnaireCount,
    //   s.emailCount,
    //   s.printCount,
    // );

    const answers = s.answers || {};

    for (const key of Object.keys(answers)) {
      if (key === "_id") continue; // skip Mongo internal id

      const value = answers[key];

      if (Array.isArray(value)) {
        for (const v of value) {
          answersTable.rows.add(
            String(sessionId),
            String(key),
            v != null ? String(v) : null,
          );
        }
      } else {
        answersTable.rows.add(
          String(sessionId),
          String(key),
          value != null ? String(value) : null,
        );
      }
    }

    for (const b of s.benefits || []) {
      // benefitsTable.rows.add(
      //   sessionId,
      //   b.benefitId,
      //   b.isRecommended,
      //   b.priority,
      //   b.selectStatus,
      // );
      benefitsTable.rows.add(
        String(sessionId),
        b.benefitId ? String(b.benefitId) : null,
        b.isRecommended ? 1 : 0,
        Number(b.priority || 0),
        Number(b.selectStatus || 0),
      );
    }

    for (const log of s.activityLog || []) {
      logTable.rows.add(
        String(sessionId),
        Number(log.type || 0),
        log.value ? String(log.value) : null,
        log.description ? String(log.description) : null,
        log.createdDate ? new Date(log.createdDate) : null,
      );
      // logTable.rows.add(
      //   sessionId,
      //   log.type,
      //   log.value,
      //   log.description,
      //   log.createdDate,
      // );
    }
  }

  // -------------------------
  // Bulk Inserts
  // -------------------------

  console.log("Bulk inserting sessions...");
  await pool.request().bulk(sessionTable);

  console.log("Answers rows:", answersTable.rows.length);
  console.log("Example:", answersTable.rows[0]);
  if (answersTable.rows.length > 0) {
    console.log("Bulk inserting answers...");
    await pool.request().bulk(answersTable);
  }

  console.log("Bulk inserting benefits...");
  await pool.request().bulk(benefitsTable);

  console.log("Bulk inserting activity logs...");
  await pool.request().bulk(logTable);

  console.log("Bulk insert complete");

  await pool.close();
  await mongo.close();
}

run().catch(console.error);
