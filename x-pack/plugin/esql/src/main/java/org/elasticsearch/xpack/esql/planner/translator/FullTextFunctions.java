/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;

public class FullTextFunctions extends ExpressionTranslator<FullTextFunction> {
    @Override
    protected Query asQuery(FullTextFunction fullTextFunction, TranslatorHandler handler) {
        return fullTextFunction.asQuery();
    }
}
