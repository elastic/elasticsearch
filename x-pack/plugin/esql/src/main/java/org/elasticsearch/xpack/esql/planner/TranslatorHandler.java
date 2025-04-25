/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;

/**
 * Handler used during query translation.
 *
 * Expressions that need to translate children into queries during own translation should always use this handler, as it provides
 * SingleValueQuery-wrapping when necessary.
 */
public final class TranslatorHandler {

    public static final TranslatorHandler TRANSLATOR_HANDLER = new TranslatorHandler();

    private TranslatorHandler() {}

    public Query asQuery(LucenePushdownPredicates predicates, Expression e) {
        if (e instanceof TranslationAware ta) {
            Query query = ta.asQuery(predicates, this);
            return ta instanceof TranslationAware.SingleValueTranslationAware sv ? wrapFunctionQuery(sv.singleValueField(), query) : query;
        }

        throw new QlIllegalArgumentException("Don't know how to translate {} {}", e.nodeName(), e);
    }

    private static Query wrapFunctionQuery(Expression field, Query query) {
        if (query instanceof SingleValueQuery) {
            // Already wrapped
            return query;
        }
        if (field instanceof FieldAttribute fa) {
            fa = fa.getExactInfo().hasExact() ? fa.exactAttribute() : fa;
            // Extract the real field name from MultiTypeEsField, and use it in the push down query if it is found
            String fieldNameFromMultiTypeEsField = LucenePushdownPredicates.extractFieldNameFromMultiTypeEsField(fa);
            String fieldName = fieldNameFromMultiTypeEsField != null ? fieldNameFromMultiTypeEsField : fa.name();
            return new SingleValueQuery(query, fieldName, false);
        }
        if (field instanceof MetadataAttribute) {
            return query; // MetadataAttributes are always single valued
        }
        throw new EsqlIllegalArgumentException("Expected a FieldAttribute or MetadataAttribute but received [" + field + "]");
    }

    // TODO: is this method necessary?
    public String nameOf(Expression e) {
        return Expressions.name(e);
    }
}
