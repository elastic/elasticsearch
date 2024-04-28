/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.expression.TableColumnAttribute;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class HashJoinExec extends UnaryExec implements EstimatesRowSize {
    private final Attribute tableName;
    private final List<Attribute> matchFields;
    private final List<TableColumnAttribute> matchValues;
    private final List<TableColumnAttribute> mergeValues;

    public HashJoinExec(
        Source source,
        PhysicalPlan child,
        Attribute tableName,
        List<Attribute> matchFields,
        @Nullable List<TableColumnAttribute> matchValues,
        @Nullable List<TableColumnAttribute> mergeValues
    ) {
        super(source, child);
        this.tableName = tableName;
        this.matchFields = matchFields;
        this.matchValues = matchValues;
        this.mergeValues = mergeValues;
    }

    public List<Attribute> matchFields() {
        return matchFields;
    }

    public List<TableColumnAttribute> matchValues() {
        return matchValues;
    }

    public List<TableColumnAttribute> mergeValues() {
        return mergeValues;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, mergeValues);
        return this;
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(mergeValues, child().output());
    }

    @Override
    public HashJoinExec replaceChild(PhysicalPlan newChild) {
        return new HashJoinExec(source(), newChild, tableName, matchFields, matchValues, mergeValues);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, HashJoinExec::new, child(), tableName, matchFields, matchValues, mergeValues);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        HashJoinExec lookup = (HashJoinExec) o;
        return Objects.equals(tableName, lookup.matchFields)
            && Objects.equals(matchFields, lookup.matchFields)
            && Objects.equals(matchValues, lookup.matchValues)
            && Objects.equals(mergeValues, lookup.mergeValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tableName, matchFields, matchValues, mergeValues);
    }
}
