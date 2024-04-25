/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Lookup extends UnaryPlan {
    private final Literal tableName;
    private final List<UnresolvedAttribute> matchFields;
    private List<Attribute> output;

    public Lookup(Source source, LogicalPlan child, Literal tableName, List<UnresolvedAttribute> matchFields) {
        super(source, child);
        this.tableName = tableName;
        this.matchFields = matchFields;
    }

    public Literal tableName() {
        return tableName;
    }

    public List<UnresolvedAttribute> matchFields() {
        return matchFields;
    }

    @Override
    public boolean expressionsResolved() {
        return tableName.resolved() && Resolvables.resolved(matchFields);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Lookup(source(), newChild, tableName, matchFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Lookup::new, child(), tableName, matchFields);
    }

    @Override
    public List<Attribute> output() {
        throw new UnsupportedOperationException();
        // if (enrichFields == null) {
        // return child().output();
        // }
        // if (this.output == null) {
        // this.output = mergeOutputAttributes(enrichFields(), child().output());
        // }
        // return output;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Lookup lookup = (Lookup) o;
        return Objects.equals(tableName, lookup.matchFields) && Objects.equals(matchFields, lookup.matchFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tableName, matchFields);
    }
}
