/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public abstract class RegexExtractExec extends UnaryExec {

    protected final Expression inputExpression;
    protected final List<Attribute> extractedFields;

    protected RegexExtractExec(Source source, PhysicalPlan child, Expression inputExpression, List<Attribute> extractedFields) {
        super(source, child);
        this.inputExpression = inputExpression;
        this.extractedFields = extractedFields;
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(extractedFields, child().output());
    }

    public Expression inputExpression() {
        return inputExpression;
    }

    public List<Attribute> extractedFields() {
        return extractedFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        RegexExtractExec that = (RegexExtractExec) o;
        return Objects.equals(inputExpression, that.inputExpression) && Objects.equals(extractedFields, that.extractedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inputExpression, extractedFields);
    }
}
