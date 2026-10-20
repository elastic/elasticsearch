/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.function.Predicate.not;
import static java.util.stream.Stream.concat;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Like {@link LookupJoin} but unmatched results are dropped.
 */
public class InnerJoin extends Join implements SubPlanJoin, SortPreserving {

    private final List<Attribute> addedFields;
    private final boolean unique;
    private List<Attribute> lazyOutput;

    public InnerJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields,
        boolean unique
    ) {
        super(source, left, right, JoinTypes.INNER, leftFields, rightFields, null, ExecuteLocation.COORDINATOR);
        this.addedFields = addedFields;
        this.unique = unique;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public List<Attribute> leftFields() {
        return config().leftFields();
    }

    public List<Attribute> rightFields() {
        return config().rightFields();
    }

    public List<Attribute> addedFields() {
        return addedFields;
    }

    public boolean unique() {
        return unique;
    }

    @Override
    public LogicalPlan inlineSubqueryResult(LocalRelation result, SubqueryResultContext context) {
        return replaceChildren(left(), result);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(
                concat(right().output().stream().filter(not(rightFields()::contains)), leftFields().stream()).toList(),
                left().output().stream().filter(not(leftFields()::contains)).toList()
            );
        }
        return lazyOutput;
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return new ArrayList<>(output());
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public InnerJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new InnerJoin(source(), left, right, leftFields(), rightFields(), addedFields, unique);
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(this, InnerJoin::new, left(), right(), leftFields(), rightFields(), addedFields, unique);
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
        InnerJoin innerJoin = (InnerJoin) o;
        return unique == innerJoin.unique && addedFields.equals(innerJoin.addedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), addedFields, unique);
    }
}
