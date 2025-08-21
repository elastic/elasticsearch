/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;

/**
 * A plan that creates new {@link Attribute}s and appends them to the child {@link org.elasticsearch.xpack.esql.plan.logical.UnaryPlan}'s
 * attributes.
 * Attributes are appended on the right hand side of the child's input. In case of name conflicts, the rightmost attribute with
 * a given name shadows any attributes left of it
 * (c.f. {@link org.elasticsearch.xpack.esql.expression.NamedExpressions#mergeOutputAttributes(List, List)}).
 */
public interface GeneratingPlan<PlanType extends GeneratingPlan<PlanType>> {
    List<Attribute> generatedAttributes();

    /**
     * Create a new instance of this node with new output {@link Attribute}s using the given names.
     * If an output attribute already has the desired name, we continue using it; otherwise, we
     * create a new attribute with a new {@link org.elasticsearch.xpack.esql.core.expression.NameId}.
     */
    // TODO: the generated attributes should probably become synthetic once renamed
    // blocked on https://github.com/elastic/elasticsearch/issues/98703
    PlanType withGeneratedNames(List<String> newNames);

    default void checkNumberOfNewNames(List<String> newNames) {
        if (newNames.size() != generatedAttributes().size()) {
            throw new IllegalArgumentException(
                "Number of new names is [" + newNames.size() + "] but there are [" + generatedAttributes().size() + "] existing names."
            );
        }
    }
}
