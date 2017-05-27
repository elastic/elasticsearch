/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.Locale;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.planner.PlanningException;
import org.elasticsearch.xpack.sql.session.Executable;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.SqlSession;

import static java.lang.String.format;

// this is mainly a marker interface to validate a plan before being executed
public interface Unexecutable extends Executable {

    default void execute(SqlSession session, ActionListener<RowSetCursor> listener) {
        throw new PlanningException(format(Locale.ROOT, "Current plan %s is not executable", this));
    }
}
