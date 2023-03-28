-- This example demonstrates Type 2 Slowly Changing Dimensions in Hive.
-- Be sure to stage data in before starting (load_data.sh)

create database type2_test;
use type2_test;

-- Create the Hive managed table for our contacts. We track a start and end date.
create table contacts_target (id int, name varchar(100), email varchar(100), state varchar(100), valid_from date, valid_to date);
  

-- Create an external table pointing to our initial data load (1000 records)
create table contacts_initial_stage(id int, name varchar(100), email varchar(100), state varchar(100));



BULK INSERT dbo.contacts_initial_stage
FROM 'D:/data/initial_contacts.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=1
)
GO

select * from contacts_initial_stage;


 

-- Copy the initial load into the managed table. We hard code the valid_from dates to the beginning of 2017.
insert into contacts_target select *, cast('2017-01-01' as date), cast(null as date) from contacts_initial_stage;

select * from contacts_target;

-- Create an external table pointing to our refreshed data load (1100 records)
create table contacts_update_stage(id int, name varchar(100), email varchar(100), state varchar(100));



BULK INSERT dbo.contacts_update_stage
FROM 'D:/data/update_contacts.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=1
)
GO

select * from contacts_update_stage;

-- Perform the Type 2 SCD.
merge into contacts_target
using (
  -- The base staging data.
  select
    contacts_update_stage.id as join_key,
    contacts_update_stage.* from contacts_update_stage

  union all

  -- Generate an extra row for changed records.
  -- The null join_key means it will be inserted.
  select
    null, contacts_update_stage.*
  from
    contacts_update_stage join contacts_target on contacts_update_stage.id = contacts_target.id
  where
    ( contacts_update_stage.email <> contacts_target.email or contacts_update_stage.state <> contacts_target.state )
    and contacts_target.valid_to is null
    
    
) sub
on sub.join_key = contacts_target.id
when matched
  and sub.email <> contacts_target.email or sub.state <> contacts_target.state
  then update set valid_to = GETDATE()
when not matched
  then insert values (sub.id, sub.name, sub.email, sub.state, GETDATE(), null);

  select * from contacts_target ORDER BY id,valid_from;

  



-- Confirm 92 records are expired.
select count(*) from contacts_target where valid_to is not null;

-- Confirm we now have 1192 records.
select count(*) from contacts_target;

-- View one of the changed records.
select * from contacts_target where id = 12;

TRUNCATE TABLE contacts_target;


select count(*) from contacts_update_stage;