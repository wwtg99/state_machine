---------
--types--
---------

CREATE TYPE task_state_type AS 
ENUM ('Queued', 'In Progress', 'Completed', 
	'Aborted', 'Canceled', 'Disregarded');

----------
--tables--
----------

--task list
CREATE TABLE tasks (
	task_id INT PRIMARY KEY,
	label TEXT NOT NULL UNIQUE,
	descr TEXT,
	check_func TEXT, --function(object, object_id, task_id) return bool, check in start task
	params JSONB,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
	deleted_at TIMESTAMP WITH TIME ZONE
);


--protocol list
CREATE TABLE protocols (
	protocol_id INT PRIMARY KEY,
	task_id INT REFERENCES tasks (task_id) NOT NULL,
	label TEXT NOT NULL UNIQUE,
	version TEXT NOT NULL,
	priority INT NOT NULL DEFAULT 100,
	descr TEXT,
	check_func TEXT, --function(object, object_id, task_id, protocol_id) return bool, check in start task
	params JSONB,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
	deleted_at TIMESTAMP WITH TIME ZONE,
	UNIQUE (task_id, priority)
);

--task transition tree
CREATE TABLE task_tree (
	id SERIAL PRIMARY KEY,
	from_task_id INT REFERENCES tasks (task_id) NOT NULL,
	from_protocol_id INT REFERENCES protocols (protocol_id),
	to_task_id INT REFERENCES tasks (task_id) NOT NULL,
	to_protocol_id INT REFERENCES protocols (protocol_id)
);

--task log
CREATE TABLE task_log (
	id SERIAL PRIMARY KEY,
	task_id INT NOT NULL REFERENCES tasks (task_id),
	protocol_id INT REFERENCES protocols (protocol_id),
	object TEXT NOT NULL,
	object_id TEXT NOT NULL,
	state task_state_type NOT NULL,
	params JSONB,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
	created_by TEXT NOT NULL
);

--task queue list
CREATE TABLE task_queue (
	id SERIAL PRIMARY KEY,
	task_id INT NOT NULL REFERENCES tasks (task_id),
	protocol_id INT REFERENCES protocols (protocol_id),
	object TEXT NOT NULL,
	object_id TEXT NOT NULL,
	params JSONB,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

--task state transition tree
CREATE TABLE state_transfer (
	id SERIAL PRIMARY KEY,
	task_id INT NOT NULL REFERENCES tasks (task_id),
	protocol_id INT REFERENCES protocols (protocol_id),
	from_state task_state_type NOT NULL,
	to_state task_state_type NOT NULL,
	check_func TEXT -- function (current_state, task_log) return bool, check in state transition
);

-------------
--functions--
-------------

--get current state of object in task (protocol)
CREATE OR REPLACE FUNCTION get_state(in_object TEXT, in_object_id TEXT, 
	in_task_id INT, in_protocol_id INT DEFAULT NULL) 
RETURNS task_state_type AS $BODY$
DECLARE
	_state task_state_type;
BEGIN
	IF in_protocol_id IS NULL THEN
		SELECT state INTO _state FROM task_log 
		WHERE object = in_object AND object_id = in_object_id 
		AND task_id = in_task_id ORDER BY created_at DESC, id DESC LIMIT 1;
	ELSE
		SELECT state INTO _state FROM task_log 
		WHERE object = in_object AND object_id = in_object_id 
		AND task_id = in_task_id AND protocol_id = in_protocol_id 
		ORDER BY created_at DESC, id DESC LIMIT 1;
	END IF;
	IF NOT FOUND THEN
		RETURN NULL;
	END IF;
	RETURN _state;
END;
$BODY$ LANGUAGE plpgsql
SECURITY DEFINER;

--check whether object is in specific state for task (protocol)
CREATE OR REPLACE FUNCTION is_in_state(in_state task_state_type, in_object TEXT, 
	in_object_id TEXT, in_task_id INT, in_protocol_id INT DEFAULT NULL) 
RETURNS BOOL AS $BODY$
DECLARE
	_state task_state_type;
BEGIN
	_state := get_state(in_object, in_object_id, in_task_id, in_protocol_id);
	IF _state = in_state THEN
		RETURN TRUE;
	END IF;
	RETURN FALSE;
END;
$BODY$ LANGUAGE plpgsql
SECURITY DEFINER;

--check whether object is completed for task (protocol)
CREATE OR REPLACE FUNCTION is_completed(in_object TEXT, in_object_id TEXT, 
	in_task_id INT, in_protocol_id INT DEFAULT NULL) 
RETURNS BOOL AS $BODY$
DECLARE
	
BEGIN
	RETURN is_in_state('Completed'::task_state_type, in_object, in_object_id, 
		in_task_id, in_protocol_id);
END;
$BODY$ LANGUAGE plpgsql
SECURITY DEFINER;

--check whether object is ready for task (protocol)
CREATE OR REPLACE FUNCTION is_ready(in_object TEXT, in_object_id TEXT, 
	in_task_id INT, in_protocol_id INT DEFAULT NULL) 
RETURNS BOOL AS $BODY$
DECLARE
	_pre_task RECORD;
	_state task_state_type;
BEGIN
	--get pre task and protocol
	IF in_protocol_id IS NULL THEN
		SELECT * INTO _pre_task FROM task_tree 
		WHERE to_task_id = in_task_id AND to_protocol_id IS NULL;
		IF NOT FOUND THEN
			RETURN TRUE;
		END IF;
	ELSE
		SELECT * INTO _pre_task FROM task_tree 
		WHERE to_task_id = in_task_id AND to_protocol_id = in_protocol_id;
		IF NOT FOUND THEN
			SELECT * INTO _pre_task FROM task_tree 
			WHERE to_task_id = in_task_id AND to_protocol_id IS NULL;
			IF NOT FOUND THEN
				RETURN TRUE;
			END IF;
		END IF;
	END IF;
	--check pre task and protocol
	IF is_completed(in_object, in_object_id, _pre_task.from_task_id, _pre_task.from_protocol_id) THEN
		RETURN is_ready(in_object, in_object_id, _pre_task.from_task_id, _pre_task.from_protocol_id);
	END IF;
	RETURN FALSE;
END;
$BODY$ LANGUAGE plpgsql
SECURITY DEFINER;

--start task from task queue with dependence and func check
CREATE OR REPLACE FUNCTION start_task(in_object TEXT, in_object_id TEXT, 
	in_task_id INT, in_protocol_id INT, in_user TEXT, in_params JSONB DEFAULT NULL) 
RETURNS BIGINT AS $BODY$
DECLARE
	_id BIGINT;
	_ready BOOL;
	_func TEXT;
	_res BOOL;
BEGIN
	--check task queue
	SELECT id INTO _id FROM task_queue WHERE task_id = in_task_id 
	AND protocol_id = in_protocol_id AND object = in_object 
	AND object_id = in_object_id;
	IF NOT FOUND THEN
		SELECT id INTO _id FROM task_queue WHERE task_id = in_task_id 
		AND protocol_id IS NULL AND object = in_object 
		AND object_id = in_object_id;
		IF NOT FOUND THEN
			RETURN NULL;
		END IF;
	END IF;
	_ready := is_ready(in_object, in_object_id, in_task_id, in_protocol_id);
	IF NOT _ready THEN
		RETURN NULL;
	END IF;
	--check task
	SELECT check_func INTO _func FROM tasks WHERE task_id = in_task_id;
	IF _func IS NOT NULL THEN
		EXECUTE 'SELECT ' || _func || '($1, $2, $3)' INTO _res 
		USING in_object, in_object_id, in_task_id;
		IF NOT _res THEN
			RETURN NULL;
		END IF;
	END IF;
	--check protocol
	SELECT check_func INTO _func FROM protocols 
	WHERE task_id = in_task_id AND protocol_id = in_protocol_id;
	IF _func IS NOT NULL THEN
		EXECUTE 'SELECT ' || _func || '($1, $2, $3, $4)' INTO _res 
		USING in_object, in_object_id, in_task_id, in_protocol_id;
		IF NOT _res THEN
			RETURN NULL;
		END IF;
	END IF;
	DELETE FROM task_queue WHERE id = _id;
	INSERT INTO task_log(task_id, protocol_id, object, object_id, state, 
		created_by, params) 
	VALUES (in_task_id, in_protocol_id, in_object, 
		in_object_id, 'In Progress', in_user, in_params) RETURNING id INTO _id;
	RETURN _id;
END;
$BODY$ LANGUAGE plpgsql;


------------
--triggers--
------------

--check state transfer before task state transition
CREATE OR REPLACE FUNCTION tp_transfer_state() RETURNS TRIGGER 
AS $BODY$
DECLARE
	_state task_state_type;
	_func TEXT;
	_res BOOL;
BEGIN
	IF NEW.state = 'Queued' THEN
		RETURN NEW;
	END IF;
	_state := get_state(NEW.object, NEW.object_id, NEW.task_id, NEW.protocol_id);
	IF _state IS NULL THEN
		_state := 'Queued'::task_state_type;
	END IF;
	--deny state transition if not specified in state_transfer
	SELECT check_func INTO _func FROM state_transfer 
	WHERE task_id = NEW.task_id AND protocol_id = NEW.protocol_id 
	AND from_state = _state AND to_state = NEW.state;
	IF NOT FOUND THEN
		SELECT check_func INTO _func FROM state_transfer 
		WHERE task_id = NEW.task_id AND protocol_id IS NULL  
		AND from_state = _state AND to_state = NEW.state;
		IF NOT FOUND THEN
			RETURN NULL;
		END IF;
	END IF;
	--check transition func if exists
	IF _func IS NOT NULL THEN
		EXECUTE 'SELECT ' || _func || '($1, $2)' INTO _res USING _state, NEW::task_log;
		IF NOT _res THEN
			RETURN NULL;
		END IF;
	END IF;
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql
SECURITY DEFINER;

CREATE TRIGGER tg_task_log BEFORE INSERT ON task_log 
FOR EACH ROW EXECUTE PROCEDURE tp_transfer_state();

--ensure created_at and updated_at in insert and update
CREATE OR REPLACE FUNCTION tp_change() RETURNS TRIGGER AS $BODY$
DECLARE
	
BEGIN
	CASE TG_OP
	WHEN 'INSERT' THEN
		NEW.created_at := now();
		NEW.updated_at := now();
		RETURN NEW;
	WHEN 'UPDATE' THEN
		NEW.created_at := OLD.created_at;
		NEW.updated_at := now();
		RETURN NEW;
	ELSE
		RETURN NULL;
	END CASE;
END;
$BODY$ LANGUAGE plpgsql
SECURITY DEFINER;

CREATE TRIGGER tg_tasks BEFORE INSERT OR UPDATE ON tasks 
FOR EACH ROW EXECUTE PROCEDURE tp_change();

CREATE TRIGGER tg_protocols BEFORE INSERT OR UPDATE ON protocols 
FOR EACH ROW EXECUTE PROCEDURE tp_change();

---------
--views--
---------

CREATE OR REPLACE VIEW view_protocols AS 
SELECT protocol_id, p.label AS protocol, p.task_id, t.label AS task, 
version, priority, t.descr AS task_descr, p.descr AS protocol_descr, 
p.created_at, p.updated_at, p.deleted_at 
FROM protocols AS p JOIN tasks AS t ON p.task_id = t.task_id;
