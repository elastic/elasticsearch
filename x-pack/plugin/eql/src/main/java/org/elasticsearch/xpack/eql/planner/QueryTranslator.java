/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.expression.function.scalar.string.CIDRMatch;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslators;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.ql.planner.ExpressionTranslators.and;
import static org.elasticsearch.xpack.ql.planner.ExpressionTranslators.or;

final class QueryTranslator {

    public static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = List.of(
            new ExpressionTranslators.BinaryComparisons(),
            new ExpressionTranslators.Ranges(),
            new BinaryLogic(),
            new ExpressionTranslators.Nots(),
            new ExpressionTranslators.Likes(),
            new ExpressionTranslators.InComparisons(),
            new ExpressionTranslators.StringQueries(),
            new ExpressionTranslators.Matches(),
            new ExpressionTranslators.MultiMatches(),
            new Scalars()
    );

    public static Query toQuery(Expression e) {
        return toQuery(e, new EqlTranslatorHandler());
    }

    public static Query toQuery(Expression e, TranslatorHandler handler) {
        Query translation = null;
        for (ExpressionTranslator<?> translator : QUERY_TRANSLATORS) {
            translation = translator.translate(e, handler);
            if (translation != null) {
                return translation;
            }
        }

        throw new QlIllegalArgumentException("Don't know how to translate {} {}", e.nodeName(), e);
    }

    public static class BinaryLogic extends ExpressionTranslator<org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic> {

        @Override
        protected Query asQuery(org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic e, TranslatorHandler handler) {
            if (e instanceof And) {
                return and(e.source(), toQuery(e.left(), handler), toQuery(e.right(), handler));
            }
            if (e instanceof Or) {
                return or(e.source(), toQuery(e.left(), handler), toQuery(e.right(), handler));
            }

            return null;
        }
    }

    public static Object valueOf(Expression e) {
        if (e.foldable()) {
            return e.fold();
        }
        throw new QlIllegalArgumentException("Cannot determine value for {}", e);
    }

    public static class Scalars extends ExpressionTranslator<ScalarFunction> {

        @Override
        protected Query asQuery(ScalarFunction f, TranslatorHandler handler) {
            return doTranslate(f, handler);
        }

        public static Query doTranslate(ScalarFunction f, TranslatorHandler handler) {
            Query q = ExpressionTranslators.Scalars.doKnownTranslate(f, handler);
            if (q != null) {
                return q;
            }
            if (f instanceof CIDRMatch) {
                CIDRMatch cm = (CIDRMatch) f;
                if (cm.input() instanceof FieldAttribute && Expressions.foldable(cm.addresses())) {
                    String targetFieldName = handler.nameOf(((FieldAttribute) cm.input()).exactAttribute());

                    Set<Object> set = new LinkedHashSet<>(CollectionUtils.mapSize(cm.addresses().size()));

                    for (Expression e : cm.addresses()) {
                        set.add(valueOf(e));
                    }

                    return new TermsQuery(f.source(), targetFieldName, set);
                }
            }

            return handler.wrapFunctionQuery(f, f, new ScriptQuery(f.source(), f.asScript()));
        }
    }
}
