/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;

import java.util.LinkedHashSet;
import java.util.Set;

public class Scalars extends ExpressionTranslator<ScalarFunction> {
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
        // TODO we could optimize starts_with as well

        throw new QlIllegalArgumentException("Cannot translate expression:[" + f.sourceText() + "]");
    }
}
