/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MultiMatch;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Replaces {@link MultiMatch} with {@link Match}, iff {@link MultiMatch} only specifies one text field.
 * This enables matching on a single `semantic_text` field as {@link MultiMatch} doesn't work with `semantic_text`.
 */
public final class ReplaceSingleMultiMatchWithMatch extends OptimizerRules.OptimizerRule<Filter> {
    @Override
    protected LogicalPlan rule(Filter filter) {
        Expression condition = filter.condition();

        if (condition instanceof MultiMatch multiMatch && multiMatch.fields().size() == 1) {
            // MULTI_MATCH and MATCH have a subset of common options
            Expression filteredOptions = filterOptionsForMatch(multiMatch.options());
            Match match = new Match(Source.EMPTY, multiMatch.fields().getFirst(), multiMatch.query(), filteredOptions);

            return new Filter(filter.source(), filter.child(), match);
        }

        return filter;
    }

    private Expression filterOptionsForMatch(Expression multiMatchOptions) {
        if ((multiMatchOptions instanceof MapExpression) == false) {
            return null;
        }

        MapExpression mapExpr = (MapExpression) multiMatchOptions;

        // Filter the map entries to only include allowed options
        List<Map.Entry<Expression, Expression>> filteredEntries = mapExpr.map().entrySet().stream().filter(entry -> {
            if (entry.getKey() instanceof Literal literal && literal.dataType() == KEYWORD) {
                String optionName = ((BytesRef) literal.value()).utf8ToString();
                return Match.ALLOWED_OPTIONS.containsKey(optionName);
            }
            return false;
        }).toList();

        // Return null if no valid options remain
        if (filteredEntries.isEmpty()) {
            return null;
        }

        List<Expression> entries = new ArrayList<>();

        filteredEntries.forEach(entry -> {
            entries.add(entry.getKey());
            entries.add(entry.getValue());
        });

        return new MapExpression(multiMatchOptions.source(), entries);
    }
}
