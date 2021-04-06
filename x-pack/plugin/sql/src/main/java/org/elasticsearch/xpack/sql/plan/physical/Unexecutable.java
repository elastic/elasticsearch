/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.planner.PlanningException;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.Executable;
import org.elasticsearch.xpack.sql.session.Session;


// this is mainly a marker interface to validate a plan before being executed
public interface Unexecutable extends Executable {

    @Override
    default void execute(Session session, ActionListener<Page> listener) {
        throw new PlanningException("Current plan {} is not executable", this);
    }
}
