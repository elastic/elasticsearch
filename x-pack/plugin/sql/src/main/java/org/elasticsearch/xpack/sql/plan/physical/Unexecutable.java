/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.planner.PlanningException;
import org.elasticsearch.xpack.sql.session.Executable;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;


// this is mainly a marker interface to validate a plan before being executed
public interface Unexecutable extends Executable {

    default void execute(SqlSession session, ActionListener<SchemaRowSet> listener) {
        throw new PlanningException("Current plan {} is not executable", this);
    }
}
