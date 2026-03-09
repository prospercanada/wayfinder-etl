require("dotenv").config();

const { MongoClient } = require("mongodb");
const sql = require("mssql");

async function testMongo() {
  try {
    console.log("process.env.MONGO_URI ", process.env.MONGO_URI);
    const client = new MongoClient(process.env.MONGO_URI);

    await client.connect();

    console.log("✅ MongoDB connected");

    const db = client.db(process.env.MONGO_DB);

    const count = await db.collection("user-sessions").countDocuments();

    console.log("Mongo documents:", count);

    await client.close();
  } catch (err) {
    console.error("❌ MongoDB connection failed");
    console.error(err);
  }
}

async function testSQL() {
  try {
    // const config = {
    //   server: "localhost",
    //   port: 1433,
    //   database: process.env.SQL_DATABASE,
    //   user: process.env.SQL_USER,
    //   password: process.env.SQL_PASSWORD,
    //   options: {
    //     encrypt: false,
    //     trustServerCertificate: true,
    //   },
    // };
    const config = {
      server: process.env.SQL_SERVER,
      database: process.env.SQL_DATABASE,
      user: process.env.SQL_USER,
      password: process.env.SQL_PASSWORD,
      options: {
        encrypt: false,
        trustServerCertificate: true,
      },
    };

    await sql.connect(config);

    console.log("✅ SQL Server connected");

    const result = await sql.query`SELECT GETDATE() as currentTime`;

    console.log("SQL Server time:", result.recordset[0].currentTime);

    await sql.close();
  } catch (err) {
    console.error("❌ SQL Server connection failed");
    console.error(err);
  }
}

async function run() {
  console.log("Testing connections...\n");

  await testMongo();

  console.log("");

  await testSQL();
}

run();
