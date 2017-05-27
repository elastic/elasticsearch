/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.session.Executable;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;

public abstract class Command extends LogicalPlan implements Executable {

    public Command(Location location) {
        super(location, emptyList());
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public final void execute(SqlSession session, ActionListener<RowSetCursor> listener) {
        listener.onResponse(execute(session));
    }

    protected abstract RowSetCursor execute(SqlSession session);
}
