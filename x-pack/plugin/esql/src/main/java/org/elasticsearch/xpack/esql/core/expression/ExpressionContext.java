/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Context required for different expression functionalities,
 * like {@link Expression#fold} or {@link EvaluatorMapper.ToEvaluator#toEvaluator}.
 */
public interface ExpressionContext {
    Configuration configuration();

    FoldContext foldCtx();

    record Impl(Configuration configuration, FoldContext foldCtx) implements ExpressionContext {}
}
