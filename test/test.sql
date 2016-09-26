--test

\set ECHO none
\set QUIET 1
-- Turn off echo and keep things quiet.

-- Format the output for nice TAP.
\pset format unaligned
\pset tuples_only true
\pset pager

-- Revert all changes on failure.
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP 1

-- Load the TAP functions.
BEGIN;
\i test/pgtap.sql

-- Plan the tests.
SELECT plan(15);

-- Run the tests.

-- fucntions
CREATE OR REPLACE FUNCTION task1_check(in_obj TEXT, in_objid TEXT, in_task_id INT) 
RETURNS BOOL AS $BODY$
BEGIN
	IF left(in_objid, 1) = 'o' THEN
		RETURN TRUE;
	ELSE
		RAISE NOTICE 'object id does not start with o';
		RETURN FALSE;
	END IF;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION task2_check(in_obj TEXT, in_objid TEXT, in_task_id INT) 
RETURNS BOOL AS $BODY$
BEGIN
	RAISE NOTICE 'task 2 object id %', in_objid;
	RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION protocol1_check(in_obj TEXT, in_objid TEXT, in_task_id INT) 
RETURNS BOOL AS $BODY$
BEGIN
	RAISE NOTICE 'protocol 1 object %', in_obj;
	RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION state_check(in_cur task_state_type, in_task_log task_log) 
RETURNS BOOL AS $BODY$
BEGIN
	RAISE NOTICE 'current state %', in_cur;
	RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION state_check2(in_cur task_state_type, in_task_log task_log) 
RETURNS BOOL AS $BODY$
BEGIN
	RAISE NOTICE 'current state %', in_cur;
	IF (in_task_log.params->>'qc') = 'Pass' THEN
		RETURN TRUE;
	END IF;
	RETURN FALSE;
END;
$BODY$ LANGUAGE plpgsql;


INSERT INTO tasks (task_id, label, check_func) VALUES (100, 'Task1', 'task1_check');
INSERT INTO tasks (task_id, label, check_func) VALUES (101, 'Task2', 'task2_check');
INSERT INTO protocols (protocol_id, task_id, label, version, priority, check_func) 
VALUES (100, 100, 'Protocol1-1', '1.0.0', 100, NULL);
INSERT INTO protocols (protocol_id, task_id, label, version, priority, check_func) 
VALUES (101, 100, 'Protocol1-2', '1.0.0', 101, NULL);
INSERT INTO protocols (protocol_id, task_id, label, version, priority, check_func) 
VALUES (102, 101, 'Protocol2-1', '1.0.0', 100, 'protocol1_check');
INSERT INTO protocols (protocol_id, task_id, label, version, priority, check_func) 
VALUES (103, 101, 'Protocol2-2', '1.0.0', 101, NULL);
INSERT INTO task_tree (from_task_id, from_protocol_id, to_task_id, to_protocol_id) 
VALUES (100, NULL, 101, NULL);
INSERT INTO task_tree (from_task_id, from_protocol_id, to_task_id, to_protocol_id) 
VALUES (100, 100, 101, 103);
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (100, NULL, 'Queued', 'In Progress', NULL);
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (100, NULL, 'In Progress', 'Completed', 'state_check');
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (100, NULL, 'In Progress', 'Aborted', NULL);
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (100, NULL, 'In Progress', 'Canceled', NULL);
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (100, NULL, 'In Progress', 'Disregarded', NULL);
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (101, NULL, 'Queued', 'In Progress', NULL);
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (101, NULL, 'In Progress', 'Completed', NULL);
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (101, 103, 'In Progress', 'Completed', 'state_check2');
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (101, NULL, 'In Progress', 'Aborted', NULL);
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (101, NULL, 'In Progress', 'Canceled', NULL);
INSERT INTO state_transfer (task_id, protocol_id, from_state, to_state, check_func) 
VALUES (101, NULL, 'In Progress', 'Disregarded', NULL);

SELECT is(is_ready('obj', 'o1', 100, NULL), TRUE);
SELECT is(is_ready('obj', 'o1', 100, 100), TRUE);
SELECT is(is_ready('obj', 'o1', 101, NULL), FALSE);
SELECT start_task('obj', 'o1', 100, 100, 'user') INTO _tid1;
SELECT is(start_task, NULL) FROM _tid1;
SELECT start_task('obj', 'o1', 101, 102, 'user') INTO _tid2;
SELECT is(start_task, NULL) FROM _tid2;

INSERT INTO task_queue(task_id, protocol_id, object, object_id) 
VALUES (100, NULL, 'obj', 'o1');
INSERT INTO task_queue(task_id, protocol_id, object, object_id) 
VALUES (100, NULL, 'obj', 'a1');
INSERT INTO task_queue(task_id, protocol_id, object, object_id) 
VALUES (101, NULL, 'obj', 'o1');

SELECT start_task('obj', 'o1', 100, 100, 'user') INTO _tid3;
SELECT isnt(start_task, NULL) FROM _tid3;
SELECT start_task('obj', 'o1', 101, 102, 'user') INTO _tid4;
SELECT is(start_task, NULL) FROM _tid4;
SELECT is(get_state('obj', 'o1', 100), 'In Progress');
SELECT is(is_completed('obj', 'o1', 100, 100), FALSE);
INSERT INTO task_log (task_id, protocol_id, object, object_id, state, created_by) 
VALUES (100, 100, 'obj', 'o1', 'Completed', 'user');
SELECT is(get_state('obj', 'o1', 100, 100), 'Completed');
SELECT is(is_completed('obj', 'o1', 100, 100), TRUE);
SELECT is(is_ready('obj', 'o1', 101, NULL), TRUE);
SELECT start_task('obj', 'o1', 101, 103, 'user') INTO _tid5;
SELECT isnt(start_task, NULL) FROM _tid5;
INSERT INTO task_log (task_id, protocol_id, object, object_id, state, created_by) 
VALUES (101, 103, 'obj', 'o1', 'Completed', 'user');
SELECT is(is_completed('obj', 'o1', 101, 103), FALSE);
INSERT INTO task_log (task_id, protocol_id, object, object_id, state, created_by, params) 
VALUES (101, 103, 'obj', 'o1', 'Completed', 'user', '{"qc": "Pass"}');
SELECT is(is_completed('obj', 'o1', 101, 103), TRUE);


-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;