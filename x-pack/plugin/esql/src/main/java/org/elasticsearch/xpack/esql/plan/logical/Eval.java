/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Eval extends UnaryPlan {

    private final List<NamedExpression> fields;

    public Eval(Source source, LogicalPlan child, List<NamedExpression> fields) {
        super(source, child);
        this.fields = fields;
    }

    public List<NamedExpression> fields() {
        return fields;
    }

    @Override
    public List<Attribute> output() {
        return output(fields, child().output());
    }

    /**
     * Calculates the actual output of the eval given the eval fields plus other inputs that are emitted as outputs
     * @param fields the eval fields
     * @param childOutput the eval input that has to be propagated as output
     * @return
     */
    public static List<Attribute> output(List<? extends NamedExpression> fields, List<? extends NamedExpression> childOutput) {
        return outputExpressions(fields, childOutput).stream().map(NamedExpression::toAttribute).collect(Collectors.toList());
    }

    public static List<NamedExpression> outputExpressions(
        List<? extends NamedExpression> fields,
        List<? extends NamedExpression> childOutput
    ) {
        List<String> fieldNames = Expressions.names(fields);
        List<NamedExpression> output = new ArrayList<>(childOutput.size() + fields.size());
        for (NamedExpression childAttr : childOutput) {
            if (fieldNames.contains(childAttr.name()) == false) {
                output.add(childAttr);
            }
        }
        // do not add duplicate fields multiple times, only last one matters as output
        for (int i = 0; i < fields.size(); i++) {
            NamedExpression field = fields.get(i);
            if (fieldNames.lastIndexOf(field.name()) == i) {
                output.add(field);
            }
        }
        return output;
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
