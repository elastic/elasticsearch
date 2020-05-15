/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.eql.session.EmptyExecutable;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Executable;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class LocalRelation extends LogicalPlan implements Executable {

    private final Executable executable;

    public LocalRelation(Source source, List<Attribute> output) {
        this(source, output, Results.Type.SEARCH_HIT);
    }

    public LocalRelation(Source source, List<Attribute> output, Results.Type resultType) {
        this(source, new EmptyExecutable(output, resultType));
    }

    private LocalRelation(Source source, Executable executable) {
        super(source, emptyList());
        this.executable = executable;
    }

    @Override
    protected NodeInfo<LocalRelation> info() {
        return NodeInfo.create(this, LocalRelation::new, executable);
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
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
    public void execute(EqlSession session, ActionListener<Results> listener) {
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
