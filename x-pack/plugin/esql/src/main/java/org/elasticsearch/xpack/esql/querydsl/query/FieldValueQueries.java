/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextFamilyFieldType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.ExpressionQueryBuilder;

import static org.elasticsearch.index.query.WildcardQueryBuilder.expressionTransportSupported;

/**
 * Pushing a predicate over a field's value to the values the field keeps, for a field whose index holds no exact form
 * of them - a {@code text} field, whose index holds the tokens its values analyze into instead.
 *
 * <p>The predicate travels as an expression and builds its Lucene query on the shard, where the field type says how
 * its values are framed.
 */
public final class FieldValueQueries {

    private FieldValueQueries() {}

    /**
     * {@code fieldType} as the family that answers a predicate over its values. The planner pushes a predicate here
     * only for a field every shard of the request answers, so a field of another kind is a bug rather than a shape to
     * handle.
     */
    public static TextFamilyFieldType textFamily(MappedFieldType fieldType) {
        if (fieldType instanceof TextFamilyFieldType textFamily) {
            return textFamily;
        }
        throw new IllegalStateException("field [" + fieldType.name() + "] does not answer a predicate over its values");
    }

    /** Whether every node of the request understands a predicate pushed as an expression. */
    public static boolean pushable(@Nullable TransportVersion minTransportVersion) {
        return minTransportVersion == null || expressionTransportSupported(minTransportVersion);
    }

    /** {@code predicate} pushed over {@code fieldName}'s values, for it to answer on the shard. */
    public static Query over(Source source, String fieldName, Expression predicate) {
        return new TranslationAwareExpressionQuery(source, new ExpressionQueryBuilder(fieldName, predicate));
    }
}
