/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.planner;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

public abstract class ExpressionTranslator<E extends Expression> {

    private final Class<E> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

    @SuppressWarnings("unchecked")
    public Query translate(Expression exp, TranslatorHandler handler) {
        return (typeToken.isInstance(exp) ? asQuery((E) exp, handler) : null);
    }

    protected abstract Query asQuery(E e, TranslatorHandler handler);

    public static Query wrapIfNested(Query query, Expression exp) {
        if (query != null && exp instanceof FieldAttribute) {
            FieldAttribute fa = (FieldAttribute) exp;
            if (fa.isNested()) {
                return new NestedQuery(fa.source(), fa.nestedParent().name(), query);
            }
        }
        return query;
    }
}
