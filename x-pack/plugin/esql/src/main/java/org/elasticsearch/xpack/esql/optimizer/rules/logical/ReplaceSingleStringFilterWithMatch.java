/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MultiMatch;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.type.DataType.isString;

/**
 * Rewrites a query in the form of: "FROM index | WHERE query" (condition part of filter is a string)
 *   - For a single text field to: "FROM index | WHERE MATCH(text_field, query)"
 *   - For multiple text fields to: "FROM index | WHERE MULTI_MATCH(query, text_field_1, ..., text_field_n)"
 * allowing a more intuitive way of performing a simple full-text search over all text fields of a certain index.
 */
public final class ReplaceSingleStringFilterWithMatch extends OptimizerRules.OptimizerRule<Filter> {
    @Override
    protected LogicalPlan rule(Filter filter) {
        Expression condition = filter.condition();

        // Pattern: FROM esRelation | WHERE condition::string
        if (isString(condition.dataType()) && filter.child() instanceof EsRelation esRelation) {
            // Includes semantic_text fields
            List<Expression> allTextFields = getAllTextFields(esRelation);

            FullTextFunction matchFunction;
            // We translate to MATCH for a single text field as MATCH can handle semantic_text in comparison to MULTI_MATCH
            if (allTextFields.size() == 1) {
                matchFunction = new Match(Source.EMPTY, allTextFields.getFirst(), condition, null);
            } else {
                matchFunction = new MultiMatch(Source.EMPTY, condition, allTextFields, null);
            }

            return new Filter(filter.source(), filter.child(), matchFunction);
        }

        return filter;
    }

    private List<Expression> getAllTextFields(EsRelation esRelation) {
        return esRelation.output()
            .stream()
            .filter(attr -> attr instanceof FieldAttribute)
            .map(attr -> (FieldAttribute) attr)
            .filter(fieldAttribute -> isString(fieldAttribute.dataType()))
            .collect(Collectors.toList());
    }
}
