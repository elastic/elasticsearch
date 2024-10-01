/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;

public class BinaryLogic extends ExpressionTranslator<org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic> {

    @Override
    protected Query asQuery(org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic e, TranslatorHandler handler) {
        if (e instanceof And) {
            return and(e.source(), handler.asQuery(e.left()), handler.asQuery(e.right()));
        }
        if (e instanceof Or) {
            return or(e.source(), handler.asQuery(e.left()), handler.asQuery(e.right()));
        }

        return null;
    }

    static Query and(Source source, Query left, Query right) {
        return boolQuery(source, left, right, true);
    }

    static Query or(Source source, Query left, Query right) {
        return boolQuery(source, left, right, false);
    }

    private static Query boolQuery(Source source, Query left, Query right, boolean isAnd) {
        Check.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        List<Query> queries;
        // check if either side is already a bool query to an extra bool query
        if (left instanceof BoolQuery leftBool && leftBool.isAnd() == isAnd) {
            if (right instanceof BoolQuery rightBool && rightBool.isAnd() == isAnd) {
                queries = CollectionUtils.combine(leftBool.queries(), rightBool.queries());
            } else {
                queries = CollectionUtils.combine(leftBool.queries(), right);
            }
        } else if (right instanceof BoolQuery bool && bool.isAnd() == isAnd) {
            queries = CollectionUtils.combine(bool.queries(), left);
        } else {
            queries = Arrays.asList(left, right);
        }
        return new BoolQuery(source, isAnd, queries);
    }
}
