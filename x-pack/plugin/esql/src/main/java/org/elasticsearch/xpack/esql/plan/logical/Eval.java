/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Eval extends UnaryPlan implements GeneratingPlan<Eval> {

    private final List<Alias> fields;
    private List<Attribute> lazyOutput;

    public Eval(Source source, LogicalPlan child, List<Alias> fields) {
        super(source, child);
        this.fields = fields;
    }

    public List<Alias> fields() {
        return fields;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(fields, child().output());
        }

        return lazyOutput;
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return asAttributes(fields);
    }

    @Override
    public Eval withGeneratedNames(List<String> newNames) {
        return new Eval(source(), child(), GeneratingPlan.renameAliases(fields, newNames));
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(fields);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Eval(source(), newChild, fields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Eval::new, child(), fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Eval eval = (Eval) o;
        return child().equals(eval.child()) && Objects.equals(fields, eval.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }
}
