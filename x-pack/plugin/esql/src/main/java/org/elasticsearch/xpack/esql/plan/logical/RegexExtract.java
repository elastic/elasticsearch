/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public abstract class RegexExtract extends UnaryPlan implements GeneratingPlan<RegexExtract> {
    protected final Expression input;
    protected final List<Alias> extractedFields;

    protected RegexExtract(Source source, LogicalPlan child, Expression input, List<Alias> extracted) {
        super(source, child);
        this.input = input;
        // TODO: Test the case where the alias' names deviate from the names of their ReferenceAttributes.
        // Consider and test also the case where the same name may be used multiple times in the RegexExtract's aliases.
        this.extractedFields = extracted;
    }

    @Override
    public boolean expressionsResolved() {
        return input.resolved();
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(extractedFields, child().output());
    }

    public Expression input() {
        return input;
    }

    public List<Alias> extractedFields() {
        return extractedFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        RegexExtract that = (RegexExtract) o;
        return Objects.equals(input, that.input) && Objects.equals(extractedFields, that.extractedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, extractedFields);
    }
}
