/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.search;

import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Holders class representing one or multiple query definitions.
 */
public class DefineQuery extends UnaryPlan {

    private final List<Alias> definitions;
    private List<Attribute> lazyOutput;

    public DefineQuery(Source source, LogicalPlan child, List<Alias> definitions) {
        super(source, child);
        this.definitions = definitions;
    }

    public List<Alias> definitions() {
        return definitions;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(definitions, child().output());
        }

        return lazyOutput;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(definitions);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new DefineQuery(source(), newChild, definitions);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, DefineQuery::new, child(), definitions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefineQuery def = (DefineQuery) o;
        return child().equals(def.child()) && Objects.equals(definitions, def.definitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), definitions);
    }
}
