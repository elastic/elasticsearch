/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.Executable;
import org.elasticsearch.xpack.sql.session.Session;

import java.util.List;
import java.util.Objects;

public class LocalRelation extends LeafPlan implements Executable {

    private final Executable executable;

    public LocalRelation(Source source, Executable executable) {
        super(source);
        this.executable = executable;
    }

    @Override
    protected NodeInfo<LocalRelation> info() {
        return NodeInfo.create(this, LocalRelation::new, executable);
    }

    public Executable executable() {
        return executable;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public List<Attribute> output() {
        return executable.output();
    }

    @Override
    public void execute(Session session, ActionListener<Page> listener) {
        executable.execute(session, listener);
    }

    @Override
    public int hashCode() {
        return executable.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LocalRelation other = (LocalRelation) obj;
        return Objects.equals(executable, other.executable);
    }

}
