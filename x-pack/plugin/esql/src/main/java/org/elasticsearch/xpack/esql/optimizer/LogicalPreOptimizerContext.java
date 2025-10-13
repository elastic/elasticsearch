/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.inference.InferenceService;

/**
 * Context passed to logical pre-optimizer rules.
 */
public record LogicalPreOptimizerContext(FoldContext foldCtx, InferenceService inferenceService) {

}
