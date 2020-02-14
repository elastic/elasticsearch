/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.eql.planner.PlanningException;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class UnplannedExec extends LeafExec implements Unexecutable {

    private final LogicalPlan plan;

    public UnplannedExec(Source source, LogicalPlan plan) {
        super(source);
        this.plan = plan;
    }

    @Override
    protected NodeInfo<UnplannedExec> info() {
        return NodeInfo.create(this, UnplannedExec::new, plan);
    }

    public LogicalPlan plan() {
        return plan;
    }

    @Override
    public List<Attribute> output() {
        return plan.output();
    }

    @Override
    public void execute(EqlSession session, ActionListener<Results> listener) {
        throw new PlanningException("Current plan {} is not executable", this);
    }

    @Override
    public int hashCode() {
        return plan.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnplannedExec other = (UnplannedExec) obj;
        return Objects.equals(plan, other.plan);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + plan.nodeString() + "]";
    }
}