/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;

import static org.elasticsearch.xpack.esql.core.expression.Foldables.valueOf;

public class EqualsIgnoreCaseTranslator extends ExpressionTranslator<InsensitiveEquals> {

    @Override
    protected Query asQuery(InsensitiveEquals bc, TranslatorHandler handler) {
        return doTranslate(bc, handler);
    }

    public static Query doTranslate(InsensitiveEquals bc, TranslatorHandler handler) {
        checkInsensitiveComparison(bc);
        return handler.wrapFunctionQuery(bc, bc.left(), () -> translate(bc));
    }

    public static void checkInsensitiveComparison(InsensitiveEquals bc) {
        Check.isTrue(
            bc.right().foldable(),
            "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [{}]",
            bc.right().sourceLocation().getLineNumber(),
            bc.right().sourceLocation().getColumnNumber(),
            Expressions.name(bc.right()),
            bc.symbol()
        );
    }

    static Query translate(InsensitiveEquals bc) {
        TypedAttribute attribute = checkIsPushableAttribute(bc.left());
        Source source = bc.source();
        BytesRef value = BytesRefs.toBytesRef(valueOf(bc.right()));
        String name = pushableAttributeName(attribute);
        return new TermQuery(source, name, value.utf8ToString(), true);
    }
}
