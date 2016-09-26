DROP FUNCTION IF EXISTS get_state(TEXT, TEXT, INT, INT) CASCADE;
DROP FUNCTION IF EXISTS is_ready(TEXT, TEXT, INT, INT) CASCADE;
DROP FUNCTION IF EXISTS is_completed(TEXT, TEXT, INT, INT) CASCADE;
DROP FUNCTION IF EXISTS transfer_state() CASCADE;
DROP TABLE IF EXISTS tasks, protocols, task_tree, task_log, task_queue, 
state_transfer CASCADE;
DROP TYPE IF EXISTS task_state_type;
