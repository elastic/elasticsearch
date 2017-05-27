/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.List;
import java.util.Objects;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.session.Executable;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;

public class QuerylessExec extends LeafExec {

    private final Executable executable;

    public QuerylessExec(Location location, Executable executable) {
        super(location);
        this.executable = executable;
    }

    public Executable executable() {
        return executable;
    }

    @Override
    public List<Attribute> output() {
        return executable.output();
    }

    @Override
    public void execute(SqlSession session, ActionListener<RowSetCursor> listener) {
        executable.execute(session, listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executable);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QuerylessExec other = (QuerylessExec) obj;
        return Objects.equals(executable, other.executable);
    }
}
