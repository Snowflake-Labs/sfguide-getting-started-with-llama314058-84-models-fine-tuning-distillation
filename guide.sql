-- Setup

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE DASH_S WAREHOUSE_SIZE=SMALL;
CREATE DATABASE DASH_DB;
CREATE SCHEMA DASH_SCHEMA;

USE DASH_DB.DASH_SCHEMA;
USE WAREHOUSE DASH_S;

-- Generate Synthetic Training Data

--- Define Support Ticket Categories

create or replace table support_ticket_category (
  category string
);

INSERT INTO support_ticket_category (category) VALUES 
  ('Roaming fees'), 
  ('Slow data speed'), 
  ('Lost phone'), 
  ('Add new line'), 
  ('Closing account');

--- Prompt Llama 3.1 405b to Generate Synthetic Support Tickets

create or replace table support_tickets as (
    SELECT 
      category, 
      TRY_PARSE_JSON(
        SNOWFLAKE.CORTEX.COMPLETE(
          'llama3.1-405b',
          CONCAT(
            'Please provide 25 examples of customer service calls in a telecom company for the following category:', category, '. Provide detailed and realistic scenarios that customer service representatives might encounter. Ensure the examples are diverse and cover various situations within each category. Please put the  examples into a JSON list. Each element in JSON list should include the following: {"scenario": <scenario>, "request": <detailed request from the customer, which usually is less than 3 sentences.>}. Only include JSON in output and no other words.'))) as tickets
    from support_ticket_category
);

create or replace table flatten_support_tickets as (
select 
    ticket_type, 
    abs(hash(value:request)) % 10000000 as id,
    value:request as request, 
    value:scenario as scenario
from support_tickets, lateral flatten(input => tickets) 
);

--- Rating and Filtering Synthetic Data with an LLM as a Judge.

create or replace table rate_support_tickets as (
    SELECT ticket_type, id, request, scenario, TRY_PARSE_JSON(SNOWFLAKE.CORTEX.COMPLETE('llama3.1-405b', CONCAT('You are a judge to verify if a the support ticket received in a telecom company is realistic, and valid, please give scores from 1 to 5 for each category and give your final recommendation for the given question. Support Ticket: ', request, ' Please give the score in JSON format alone following this example: "{"realistic": 5, "valid": 4}".  You can put a reason into the result JSON as "reason": <reason>. Only include JSON in the output and no other words.'))) as rating
    from flatten_support_tickets
);

create or replace table filtered_support_tickets as (
    select * from rate_support_tickets where rating['realistic'] >= 4 and rating['valid'] >= 4
);

--- Test Base Model Performance for Support Ticket Categorization using Cortex AI

CREATE OR REPLACE FUNCTION CATEGORIZE_PROMPT_TEMPLATE(request STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
CONCAT('You are an agent that helps organize requests that come to our support team. 

The request category is the reason why the customer reached out. These are the possible types of request categories:

Roaming fees
Slow data speed
Lost phone
Add new line
Closing account

Try doing it for this request and return only the request category only.

request: ', request)
$$
;

SELECT id, SNOWFLAKE.CORTEX.COMPLETE('llama3-8b', CATEGORIZE_PROMPT_TEMPLATE(request)) FROM filtered_support_tickets;

--- Prepare the Distillation Data

create or replace table training_data as (
    SELECT * from filtered_support_tickets where ID % 10 < 8 
);

create or replace table validation_data as (
    SELECT * from filtered_support_tickets where ID % 10 >= 8 
);

-- Fine-tuning

--- Fine-tuning using `FINETUNE()` SQL API

select snowflake.cortex.finetune(
'CREATE', 
'CORTEX_FINETUNING_DB.PUBLIC.SUPPORT_TICKETS_FINETUNED', 'llama3-8b', 
'SELECT request as prompt, category as completion from CORTEX_FINETUNING_DB.PUBLIC.training_data', 
'SELECT request as prompt, category as completion from CORTEX_FINETUNING_DB.PUBLIC.validation_data'
);

select snowflake.cortex.finetune('DESCRIBE', 'CortexFineTuningWorkflow_f4016e33-92ce-45d3-918a-19115c398f10');

--- Inferencing the Distilled Model

SET fine_tuned_model_name = 'SUPPORT_TICKETS_FINETUNED';

SELECT id, request,
TRIM(SNOWFLAKE.CORTEX.COMPLETE($fine_tuned_model_name, request,'\n') as fine_tuned_model_response
FROM support_tickets;
