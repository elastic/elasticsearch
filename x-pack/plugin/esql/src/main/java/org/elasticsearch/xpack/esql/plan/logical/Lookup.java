/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.expression.TableColumnAttribute;
import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Looks up values from the {@code tables} request parameter.
 * <p>
 *     This class exists in two very different modes - an "unresolved" mode
 *     which contains only the {@link #tableName} and {@link #matchValues},
 *     and a "resolved" mode which contains everything. The process of
 *     resolving all of the references adds the extra columns by looking
 *     up the table from the analysis context.
 * </p>
 */
public class Lookup extends UnaryPlan {
    private final Attribute tableName;
    private final List<Attribute> matchFields;
    private final List<TableColumnAttribute> matchValues;
    private final List<TableColumnAttribute> mergeValues;
    private List<Attribute> output;

    public Lookup(
        Source source,
        LogicalPlan child,
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

    public Attribute tableName() {
        return tableName;
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
    public boolean expressionsResolved() {
        if (matchValues == null || mergeValues == null) {
            return false;
        }
        return tableName.resolved()
            && Resolvables.resolved(matchFields)
            && Resolvables.resolved(matchValues)
            && Resolvables.resolved(mergeValues);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Lookup(source(), newChild, tableName, matchFields, matchValues, mergeValues);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Lookup::new, child(), tableName, matchFields, matchValues, mergeValues);
    }

    @Override
    public List<Attribute> output() {
        if (mergeValues == null) {
            throw new IllegalStateException("unresolved");
        }
        if (mergeValues.isEmpty()) {
            return child().output();
        }
        if (this.output == null) {
            this.output = mergeOutputAttributes(mergeValues, child().output());
        }
        return output;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Lookup lookup = (Lookup) o;
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
