/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.planner.EsqlExpressionTranslators;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.Foldables.valueOf;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;

public class InComparisons extends ExpressionTranslator<In> {
    @Override
    protected Query asQuery(In in, TranslatorHandler handler) {
        return doTranslate(in, handler);
    }

    public static Query doTranslate(In in, TranslatorHandler handler) {
        return handler.wrapFunctionQuery(in, in.value(), () -> translate(in, handler));
    }

    private static boolean needsTypeSpecificValueHandling(DataType fieldType) {
        return DataType.isDateTime(fieldType) || fieldType == IP || fieldType == VERSION || fieldType == UNSIGNED_LONG;
    }

    private static Query translate(In in, TranslatorHandler handler) {
        TypedAttribute attribute = checkIsPushableAttribute(in.value());

        Set<Object> terms = new LinkedHashSet<>();
        List<Query> queries = new ArrayList<>();

        for (Expression rhs : in.list()) {
            if (DataType.isNull(rhs.dataType()) == false) {
                if (needsTypeSpecificValueHandling(attribute.dataType())) {
                    // delegates to BinaryComparisons translator to ensure consistent handling of date and time values
                    Query query = BinaryComparisons.translate(new Equals(in.source(), in.value(), rhs), handler);

                    if (query instanceof TermQuery) {
                        terms.add(((TermQuery) query).value());
                    } else {
                        queries.add(query);
                    }
                } else {
                    terms.add(valueOf(rhs));
                }
            }
        }

        if (terms.isEmpty() == false) {
            String fieldName = pushableAttributeName(attribute);
            queries.add(new TermsQuery(in.source(), fieldName, terms));
        }

        return queries.stream().reduce((q1, q2) -> BinaryLogic.or(in.source(), q1, q2)).get();
    }
}
