/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.expression.function.scalar.string.CIDRMatch;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.StringContains;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveBinaryComparison;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveNotEquals;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.BinaryComparisonCaseInsensitiveFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.CaseInsensitiveScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslators;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.NotQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.ql.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.Check;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.ql.planner.ExpressionTranslators.and;
import static org.elasticsearch.xpack.ql.planner.ExpressionTranslators.or;
import static org.elasticsearch.xpack.ql.util.StringUtils.WILDCARD;

final class QueryTranslator {

    public static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = List.of(
        new InsensitiveBinaryComparisons(),
        new ExpressionTranslators.BinaryComparisons(),
        new ExpressionTranslators.Ranges(),
        new BinaryLogic(),
        new ExpressionTranslators.IsNotNulls(),
        new ExpressionTranslators.IsNulls(),
        new ExpressionTranslators.Nots(),
        new ExpressionTranslators.Likes(),
        new ExpressionTranslators.InComparisons(),
        new CaseInsensitiveScalarFunctions(),
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
                break;
            }
        }
        if (translation != null) {
            if (translation instanceof ScriptQuery) {
                // check the operators and the expressions involved in these operations so that all can be used
                // in a doc-values multi-valued context
                boolean multiValuedIncompatible = e.anyMatch(exp -> {
                    return false == (exp instanceof Literal || exp instanceof FieldAttribute || exp instanceof Function);
                });
                if (multiValuedIncompatible == false) {
                    ScriptQuery query = (ScriptQuery) translation;
                    return new MultiValueAwareScriptQuery(query.source(), Scripts.multiValueDocValuesRewrite(query.script()));
                }
            }
            return translation;
        }

        throw new QlIllegalArgumentException("Don't know how to translate {} {}", e.nodeName(), e);
    }

    public static class InsensitiveBinaryComparisons extends ExpressionTranslator<InsensitiveBinaryComparison> {

        @Override
        protected Query asQuery(InsensitiveBinaryComparison bc, TranslatorHandler handler) {
            return doTranslate(bc, handler);
        }

        public static Query doTranslate(InsensitiveBinaryComparison bc, TranslatorHandler handler) {
            checkInsensitiveComparison(bc);
            return handler.wrapFunctionQuery(bc, bc.left(), () -> translate(bc, handler));
        }

        public static void checkInsensitiveComparison(InsensitiveBinaryComparison bc) {
            Check.isTrue(bc.right().foldable(),
                "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [{}]",
                bc.right().sourceLocation().getLineNumber(), bc.right().sourceLocation().getColumnNumber(),
                Expressions.name(bc.right()), bc.symbol());
            bc.left().forEachDown(ArithmeticOperation.class,
                op -> ExpressionTranslators.BinaryComparisons.checkFieldsUsageInArithmeticOperation(op, bc));
        }

        private static Query translate(InsensitiveBinaryComparison bc, TranslatorHandler handler) {
            Source source = bc.source();
            String name = handler.nameOf(bc.left());
            Object value = valueOf(bc.right());

            if (bc instanceof InsensitiveEquals || bc instanceof InsensitiveNotEquals) {
                if (bc.left() instanceof FieldAttribute) {
                    // equality should always be against an exact match
                    // (which is important for strings)
                    name = ((FieldAttribute) bc.left()).exactAttribute().name();
                }
                Query query = new TermQuery(source, name, value, true);

                if (bc instanceof InsensitiveNotEquals) {
                    query = new NotQuery(source, query);
                }

                return query;
            }

            throw new QlIllegalArgumentException("Don't know how to translate binary comparison [{}] in [{}]", bc.right().nodeString(), bc);
        }
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

            return handler.wrapFunctionQuery(f, f, () -> new ScriptQuery(f.source(), f.asScript()));
        }
    }

    public static class CaseInsensitiveScalarFunctions extends ExpressionTranslator<CaseInsensitiveScalarFunction> {

        @Override
        protected Query asQuery(CaseInsensitiveScalarFunction f, TranslatorHandler handler) {
            return doTranslate(f, handler);
        }

        public static Query doTranslate(CaseInsensitiveScalarFunction f, TranslatorHandler handler) {
            Query q = ExpressionTranslators.Scalars.doKnownTranslate(f, handler);
            if (q != null) {
                return q;
            }

            if (f instanceof BinaryComparisonCaseInsensitiveFunction) {
                BinaryComparisonCaseInsensitiveFunction bccif = (BinaryComparisonCaseInsensitiveFunction) f;

                String targetFieldName = null;
                String wildcardQuery = null;

                Expression field = bccif.left();
                Expression constant = bccif.right();

                if (field instanceof FieldAttribute && constant.foldable()) {
                    targetFieldName = handler.nameOf(((FieldAttribute) field).exactAttribute());
                    String string = (String) constant.fold();

                    if (f instanceof StringContains) {
                        wildcardQuery = WILDCARD + string + WILDCARD;
                    } else if (f instanceof EndsWith) {
                        wildcardQuery = WILDCARD + string;
                    }
                }

                q = wildcardQuery != null ? new WildcardQuery(f.source(), targetFieldName, wildcardQuery, f.isCaseInsensitive()) : null;
            }
            return q;
        }
    }
}
