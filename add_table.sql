CREATE TABLE IF NOT EXISTs @target_database_schema.@target_cohort_table (
  cohort_definition_id int,
  subject_id bigint,
  cohort_start_date date,
  cohort_end_date date
);
