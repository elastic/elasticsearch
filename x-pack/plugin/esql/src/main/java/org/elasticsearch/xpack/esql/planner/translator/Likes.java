/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Like;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardLike;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RegexQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;

// TODO: see whether escaping is needed
@SuppressWarnings("rawtypes")
public class Likes extends ExpressionTranslator<RegexMatch> {
    @Override
    protected Query asQuery(RegexMatch e, TranslatorHandler handler) {
        return doTranslate(e, handler);
    }

    public static Query doTranslate(RegexMatch e, TranslatorHandler handler) {
        Query q;
        Expression field = e.field();

        if (field instanceof FieldAttribute fa) {
            return handler.wrapFunctionQuery(e, fa, () -> translateField(e, handler.nameOf(fa.exactAttribute())));
        } else if (field instanceof MetadataAttribute ma) {
            q = translateField(e, handler.nameOf(ma));
        } else {
            throw new QlIllegalArgumentException("Cannot translate query for " + e);
        }

        return q;
    }

    private static Query translateField(RegexMatch e, String targetFieldName) {
        if (e instanceof Like l) {
            return new WildcardQuery(e.source(), targetFieldName, l.pattern().asLuceneWildcard(), l.caseInsensitive());
        }
        if (e instanceof WildcardLike l) {
            return new WildcardQuery(e.source(), targetFieldName, l.pattern().asLuceneWildcard(), l.caseInsensitive());
        }
        if (e instanceof RLike rl) {
            return new RegexQuery(e.source(), targetFieldName, rl.pattern().asJavaRegex(), rl.caseInsensitive());
        }
        return null;
    }
}
