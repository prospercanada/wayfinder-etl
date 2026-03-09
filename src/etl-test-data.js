require("dotenv").config();

const { MongoClient } = require("mongodb");
const sql = require("mssql");

async function run() {
  // Mongo
  const mongo = new MongoClient(process.env.MONGO_URI);
  await mongo.connect();

  const db = mongo.db(process.env.MONGO_DB);
  // const sessions = await db
  //   .collection("user-sessions")
  //   .find({})
  //   .limit(10)
  //   .toArray();

  const start = new Date("2026-01-01T00:00:00Z");
  const end = new Date("2027-01-01T00:00:00Z");

  const sessions = await db
    .collection("user-sessions")
    .find({
      createdAt: {
        $gte: start,
        $lt: end,
      },
    })
    .limit(50) // limit to 50 while testing
    .toArray(); // iterable!
  console.log("Mongo documents fetched:", sessions.length);

  // SQL
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

  await sql.connect(config);

  for (const s of sessions) {
    const sessionId = s._id.toString();

    // -----------------------
    // INSERT SESSION
    // -----------------------
    await sql.query`
      INSERT INTO raw_sessions (
        mongo_id,
        created_at,
        locale,
        city,
        province,
        referrer,
        questionnaire_count,
        email_count,
        print_count
      )
      VALUES (
        ${sessionId},
        ${s.createdAt},
        ${s.locale},
        ${s.city},
        ${s.province},
        ${s.referrer},
        ${s.questionnaireCount},
        ${s.emailCount},
        ${s.printCount}
      )
    `;

    // -----------------------
    // ANSWERS
    // -----------------------

    const answers = s.answers || {};

    for (const key of Object.keys(answers)) {
      const value = answers[key];

      if (Array.isArray(value)) {
        for (const v of value) {
          await sql.query`
            INSERT INTO raw_session_answers
            (session_mongo_id, question_key, answer_value)
            VALUES (${sessionId}, ${key}, ${v.toString()})
          `;
        }
      } else {
        await sql.query`
          INSERT INTO raw_session_answers
          (session_mongo_id, question_key, answer_value)
          VALUES (${sessionId}, ${key}, ${value?.toString()})
        `;
      }
    }
    // -----------------------
    // BENEFITS
    // -----------------------

    for (const b of s.benefits || []) {
      await sql.query`
        INSERT INTO raw_session_benefits
        (
          session_mongo_id,
          benefit_id,
          is_recommended,
          priority,
          select_status
        )
        VALUES
        (
          ${sessionId},
          ${b.benefitId},
          ${b.isRecommended},
          ${b.priority},
          ${b.selectStatus}
        )
      `;
    }

    // -----------------------
    // ACTIVITY LOG
    // -----------------------

    for (const log of s.activityLog || []) {
      await sql.query`
        INSERT INTO raw_session_activity_log
        (
          session_mongo_id,
          type,
          value,
          description,
          created_date
        )
        VALUES
        (
          ${sessionId},
          ${log.type},
          ${log.value},
          ${log.description},
          ${log.createdDate}
        )
      `;
    }
  }

  console.log("Inserted records into SQL");

  await sql.close();
  await mongo.close();
}

run().catch(console.error);
