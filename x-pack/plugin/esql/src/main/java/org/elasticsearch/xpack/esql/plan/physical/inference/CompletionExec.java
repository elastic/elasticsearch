/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class CompletionExec extends InferenceExec {

    private final Expression prompt;
    private final Attribute targetField;
    private List<Attribute> lazyOutput;

    public CompletionExec(Source source, PhysicalPlan child, Expression inferenceId, Expression prompt, Attribute targetField) {
        super(source, child, inferenceId);
        this.prompt = prompt;
        this.targetField = targetField;
    }

    public Expression prompt() {
        return prompt;
    }

    public Attribute targetField() {
        return targetField;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, CompletionExec::new, child(), inferenceId(), prompt, targetField);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new CompletionExec(source(), newChild, inferenceId(), prompt, targetField);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(List.of(targetField), child().output());
        }

        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return prompt.references();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        CompletionExec completion = (CompletionExec) o;

        return Objects.equals(prompt, completion.prompt) && Objects.equals(targetField, completion.targetField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prompt, targetField);
    }
}
