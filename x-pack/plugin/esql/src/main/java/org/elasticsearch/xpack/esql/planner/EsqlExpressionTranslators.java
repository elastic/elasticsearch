/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.planner.translator.BinaryComparisons;
import org.elasticsearch.xpack.esql.planner.translator.BinaryLogic;
import org.elasticsearch.xpack.esql.planner.translator.EqualsIgnoreCaseTranslator;
import org.elasticsearch.xpack.esql.planner.translator.FullTextFunctions;
import org.elasticsearch.xpack.esql.planner.translator.InComparisons;
import org.elasticsearch.xpack.esql.planner.translator.IsNotNulls;
import org.elasticsearch.xpack.esql.planner.translator.IsNulls;
import org.elasticsearch.xpack.esql.planner.translator.Likes;
import org.elasticsearch.xpack.esql.planner.translator.Matches;
import org.elasticsearch.xpack.esql.planner.translator.MultiMatches;
import org.elasticsearch.xpack.esql.planner.translator.Nots;
import org.elasticsearch.xpack.esql.planner.translator.Ranges;
import org.elasticsearch.xpack.esql.planner.translator.Scalars;
import org.elasticsearch.xpack.esql.planner.translator.SpatialRelatesTranslator;
import org.elasticsearch.xpack.esql.planner.translator.StringQueries;

import java.util.List;

public final class EsqlExpressionTranslators {

    public static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = List.of(
        new EqualsIgnoreCaseTranslator(),
        new BinaryComparisons(),
        new SpatialRelatesTranslator(),
        new InComparisons(),
        new Ranges(), // Create Range in PushFiltersToSource for qualified pushable filters on the same field.
        new BinaryLogic(),
        new IsNulls(),
        new IsNotNulls(),
        new Nots(),
        new Likes(),
        new StringQueries(),
        new Matches(),
        new MultiMatches(),
        new FullTextFunctions(),
        new Scalars()
    );

    public static Query toQuery(Expression e, TranslatorHandler handler) {
        for (ExpressionTranslator<?> translator : QUERY_TRANSLATORS) {
            Query translation = translator.translate(e, handler);
            if (translation != null) {
                return translation;
            }
        }

        throw new QlIllegalArgumentException("Don't know how to translate {} {}", e.nodeName(), e);
    }
}
