/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.planner.QlTranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.Query;

public class EqlTranslatorHandler extends QlTranslatorHandler {

    @Override
    public Query asQuery(Expression e) {
        return QueryTranslator.toQuery(e, this);
    }
}
