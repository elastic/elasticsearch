/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

@Experimental
public class EvalExec extends UnaryExec {

    private final List<NamedExpression> fields;

    public EvalExec(Source source, PhysicalPlan child, List<NamedExpression> fields) {
        super(source, child);
        this.fields = fields;
    }

    public List<NamedExpression> fields() {
        return fields;
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(fields, child().output());
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new EvalExec(source(), newChild, fields);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EvalExec::new, child(), fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EvalExec eval = (EvalExec) o;
        return child().equals(eval.child()) && Objects.equals(fields, eval.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }
}
