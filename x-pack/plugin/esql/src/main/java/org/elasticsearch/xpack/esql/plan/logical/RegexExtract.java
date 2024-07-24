/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public abstract class RegexExtract extends UnaryPlan implements GeneratingPlan<RegexExtract> {
    protected final Expression input;
    protected final List<Attribute> extractedFields;

    protected RegexExtract(Source source, LogicalPlan child, Expression input, List<Attribute> extracted) {
        super(source, child);
        this.input = input;
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

    /**
     * Upon parsing, these are named according to the {@link Dissect} or {@link Grok} pattern, but can be renamed without changing the
     * pattern.
     */
    public List<Attribute> extractedFields() {
        return extractedFields;
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return extractedFields;
    }

    List<Attribute> renameExtractedFields(List<String> newNames) {
        checkNumberOfNewNames(newNames);

        List<Attribute> renamedExtractedFields = new ArrayList<>(extractedFields.size());
        for (int i = 0; i < newNames.size(); i++) {
            Attribute extractedField = extractedFields.get(i);
            String newName = newNames.get(i);
            if (extractedField.name().equals(newName)) {
                renamedExtractedFields.add(extractedField);
            } else {
                renamedExtractedFields.add(extractedFields.get(i).withName(newNames.get(i)).withId(new NameId()));
            }
        }

        return renamedExtractedFields;
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
