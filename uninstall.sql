DROP FUNCTION IF EXISTS get_state(TEXT, TEXT, INT, INT) CASCADE;
DROP FUNCTION IF EXISTS is_in_state(task_state_type, TEXT, TEXT, INT, INT) CASCADE;
DROP FUNCTION IF EXISTS is_ready(TEXT, TEXT, INT, INT) CASCADE;
DROP FUNCTION IF EXISTS is_completed(TEXT, TEXT, INT, INT) CASCADE;
DROP FUNCTION IF EXISTS tp_transfer_state() CASCADE;
DROP FUNCTION IF EXISTS tp_change() CASCADE;
DROP VIEW IF EXISTS view_protocols CASCADE;
DROP TABLE IF EXISTS tasks, protocols, task_tree, task_log, task_queue, 
state_transfer CASCADE;
DROP TYPE IF EXISTS task_state_type;
