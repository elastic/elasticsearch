/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.search.SearchService;

/**
 * Node-level state that {@link EvalMapper} hands to expressions while building their evaluators, for the
 * functions that need more than their children (e.g. runtime full-text functions resolving analyzers and
 * compiling Query DSL queries).
 *
 * @param analysisRegistry      the node-level analysis registry for resolving analyzers by name, may be {@code null}
 * @param allowExpensiveQueries the value of {@link SearchService#ALLOW_EXPENSIVE_QUERIES} on this node, so that
 *                              queries compiled at runtime refuse the same expensive query types Query DSL does
 */
public record EvaluatorServices(@Nullable AnalysisRegistry analysisRegistry, boolean allowExpensiveQueries) {}
