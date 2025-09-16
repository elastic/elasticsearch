/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.inference.InferenceService;

public class LogicalPreOptimizerContext {

    private final FoldContext foldCtx;

    private final InferenceService inferenceService;

    public LogicalPreOptimizerContext(FoldContext foldCtx, InferenceService inferenceService) {
        this.foldCtx = foldCtx;
        this.inferenceService = inferenceService;
    }

    public FoldContext foldCtx() {
        return foldCtx;
    }

    @Override
    public String toString() {
        return "LogicalPreOptimizerContext[foldCtx=" + foldCtx + ']';
    }

    public InferenceService inferenceService() {
        return inferenceService;
    }
}
