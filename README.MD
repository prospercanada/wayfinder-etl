# Wayfinder Data ETL

This project is a simple Node.js ETL prototype used to test connections
between **MongoDB** and **SQL Server** and prepare for future data
pipelines.

The goal is to eventually move session data from MongoDB into SQL Server
so that it can be used for analytics and reporting (e.g., Power BI).

---

## Project Purpose

This repository is intended to:

- Test MongoDB connectivity from Node.js
- Test SQL Server connectivity from Node.js
- Prototype an ETL pipeline for moving Wayfinder session data from
  MongoDB to SQL Server
- Provide a local development environment that mirrors a future
  Azure-based data architecture

Future architecture may include:

MongoDB → ETL Pipeline → SQL Database → Power BI

---

## Project Structure

    wayfinder-data
    │
    ├─ src
    │   └─ etl-test.js       # Test script for MongoDB and SQL connections
    │
    ├─ .env                  # Environment variables (NOT committed to Git)
    ├─ .env.example          # Template for required environment variables
    ├─ .gitignore
    ├─ package.json
    └─ README.md

---

## Prerequisites

You should have the following installed:

- Node.js (v18 or later recommended)
- npm
- MongoDB access
- SQL Server (local or remote)

---

## Installation

Clone the repository and install dependencies:

    npm install

---

## Environment Variables

Create a `.env` file in the root folder.

Example:

    MONGO_URI=mongodb://username:password@localhost:27017
    MONGO_DB=wayfinder

    SQL_SERVER=SHINYNOSE
    SQL_DATABASE=WayfinderAnalytics
    SQL_USER=wayfinder_user
    SQL_PASSWORD=StrongPassword123!

⚠️ The `.env` file is ignored by Git to protect credentials.

---

## Running the Script

Run the test script:

    npm start

or

    npm run etl

This will:

1.  Connect to MongoDB
2.  Connect to SQL Server
3.  Print connection confirmation messages

Example output:

    Testing connections...

    MongoDB connected
    Mongo documents: 12458

    SQL Server connected
    Server time: 2026‑03‑09

---

## Future Development

Next steps for this project:

- Implement batch ETL from MongoDB to SQL Server
- Flatten nested MongoDB documents into relational tables
- Create analytics-ready SQL tables
- Integrate with Power BI

Example target schema:

- sessions
- session_benefits
- session_activity_log
- session_answers

---

## Security Notes

Do not commit the following to Git:

- `.env`
- database credentials
- exported data files

Always store credentials in environment variables.

---

## License

ISC
