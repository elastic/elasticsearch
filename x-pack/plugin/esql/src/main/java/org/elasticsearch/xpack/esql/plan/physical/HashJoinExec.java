/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class HashJoinExec extends UnaryExec implements EstimatesRowSize {
    private final LocalSourceExec joinData;
    private final List<NamedExpression> unionFields;  // NOCOMMIT why sometimes NamedExpression and sometimes Attribute?!
    private final List<Attribute> output;

    public HashJoinExec(
        Source source,
        PhysicalPlan child,
        LocalSourceExec hashData,
        List<NamedExpression> unionFields,
        List<Attribute> output
    ) {
        super(source, child);
        this.joinData = hashData;
        this.unionFields = unionFields;
        this.output = output;
    }

    public LocalSourceExec joinData() {
        return joinData;
    }

    public List<NamedExpression> unionFields() {
        return unionFields;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, output);
        return this;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public HashJoinExec replaceChild(PhysicalPlan newChild) {
        return new HashJoinExec(source(), newChild, joinData, unionFields, output);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, HashJoinExec::new, child(), joinData, unionFields, output);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        HashJoinExec hash = (HashJoinExec) o;
        return joinData.equals(hash.joinData)
            && unionFields.equals(hash.unionFields)
            && output.equals(hash.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinData, unionFields, output);
    }
}
