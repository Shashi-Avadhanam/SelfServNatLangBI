# Self Service Natural Language BI	

In a typical enterprise, there will be multiple data requests from different stakeholders such as analysts, data scientists, developers, support staff etc.Data requests could range from simple data profiling requests to complex data reconciliation reports.

Instead of folks depending on each other, what if we could ask a bot to service these requests. This is a poc to show case a non-trivial use case of reconciliation of source table counts to target table counts. AWS Lex used to collect the User intent and details of source tables,target table and reconciliation criteria. AWS Lex events trigger AWS Lambda to do reconciliation logic. Database is Postgres and Async JS library used.

##TO DO

Build user interface to show demos.
