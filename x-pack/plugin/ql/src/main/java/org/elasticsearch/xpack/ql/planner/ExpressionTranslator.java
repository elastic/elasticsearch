/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.planner;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.MetadataAttribute;
import org.elasticsearch.xpack.ql.expression.TypedAttribute;
import org.elasticsearch.xpack.ql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.util.Check;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

public abstract class ExpressionTranslator<E extends Expression> {

    private final Class<E> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

    @SuppressWarnings("unchecked")
    public Query translate(Expression exp, TranslatorHandler handler) {
        return (typeToken.isInstance(exp) ? asQuery((E) exp, handler) : null);
    }

    protected abstract Query asQuery(E e, TranslatorHandler handler);

    public static Query wrapIfNested(Query query, Expression exp) {
        if (query != null && exp instanceof FieldAttribute fa) {
            if (fa.isNested()) {
                return new NestedQuery(fa.source(), fa.nestedParent().name(), query);
            }
        }
        return query;
    }

    public static FieldAttribute checkIsFieldAttribute(Expression e) {
        Check.isTrue(e instanceof FieldAttribute, "Expected a FieldAttribute but received [{}]", e);
        return (FieldAttribute) e;
    }

    public static TypedAttribute checkIsPushableAttribute(Expression e) {
        Check.isTrue(
            e instanceof FieldAttribute || e instanceof MetadataAttribute,
            "Expected a FieldAttribute or MetadataAttribute but received [{}]",
            e
        );
        return (TypedAttribute) e;
    }

    public static String pushableAttributeName(TypedAttribute attribute) {
        return attribute instanceof FieldAttribute fa
            ? fa.exactAttribute().name() // equality should always be against an exact match (which is important for strings)
            : attribute.name();
    }
}
