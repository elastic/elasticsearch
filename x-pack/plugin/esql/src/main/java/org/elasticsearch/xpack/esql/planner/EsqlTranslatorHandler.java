/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.MetadataAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslators;
import org.elasticsearch.xpack.ql.planner.QlTranslatorHandler;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public final class EsqlTranslatorHandler extends QlTranslatorHandler {

    public static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = List.of(
        new ExpressionTranslators.BinaryComparisons(),
        new ExpressionTranslators.Ranges(),
        new ExpressionTranslators.BinaryLogic(),
        new ExpressionTranslators.IsNulls(),
        new ExpressionTranslators.IsNotNulls(),
        new ExpressionTranslators.Nots(),
        new ExpressionTranslators.Likes(),
        new ExpressionTranslators.InComparisons(),
        new ExpressionTranslators.StringQueries(),
        new ExpressionTranslators.Matches(),
        new ExpressionTranslators.MultiMatches(),
        new Scalars()
    );

    @Override
    public Query asQuery(Expression e) {
        Query translation = null;
        for (ExpressionTranslator<?> translator : QUERY_TRANSLATORS) {
            translation = translator.translate(e, this);
            if (translation != null) {
                return translation;
            }
        }

        throw new QlIllegalArgumentException("Don't know how to translate {} {}", e.nodeName(), e);
    }

    @Override
    public Object convert(Object value, DataType dataType) {
        return EsqlDataTypeConverter.convert(value, dataType);
    }

    @Override
    public Query wrapFunctionQuery(ScalarFunction sf, Expression field, Supplier<Query> querySupplier) {
        if (field instanceof FieldAttribute fa) {
            if (fa.getExactInfo().hasExact()) {
                var exact = fa.exactAttribute();
                if (exact != fa) {
                    fa = exact;
                }
            }
            // don't wrap is null/is not null with SVQ
            Query query = querySupplier.get();
            if ((sf instanceof IsNull || sf instanceof IsNotNull) == false) {
                query = new SingleValueQuery(query, fa.name());
            }
            return ExpressionTranslator.wrapIfNested(query, field);
        }
        if (field instanceof MetadataAttribute) {
            return querySupplier.get(); // MetadataAttributes are always single valued
        }
        throw new EsqlIllegalArgumentException("Expected a FieldAttribute or MetadataAttribute but received [" + field + "]");
    }

    public static class Scalars extends ExpressionTranslator<ScalarFunction> {
        @Override
        protected Query asQuery(ScalarFunction f, TranslatorHandler handler) {
            return doTranslate(f, handler);
        }

        public static Query doTranslate(ScalarFunction f, TranslatorHandler handler) {
            if (f instanceof CIDRMatch cm) {
                if (cm.ipField() instanceof FieldAttribute fa && Expressions.foldable(cm.matches())) {
                    String targetFieldName = handler.nameOf(fa.exactAttribute());
                    Set<Object> set = new LinkedHashSet<>(Expressions.fold(cm.matches()));

                    Query query = new TermsQuery(f.source(), targetFieldName, set);
                    // CIDR_MATCH applies only to single values.
                    return handler.wrapFunctionQuery(f, cm.ipField(), () -> query);
                }
            }

            return ExpressionTranslators.Scalars.doTranslate(f, handler);
        }
    }
}
