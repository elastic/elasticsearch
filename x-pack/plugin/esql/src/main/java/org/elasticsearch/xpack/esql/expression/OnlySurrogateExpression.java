/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Expression;

/**
 * Marker for {@link Expression}s that can only be evaluated by {@link #surrogate()}ing
 * on the coordinating node. These require different tests and the test-checking
 * infrastructure looks for this marker to check which tests are missing.
 */
public interface OnlySurrogateExpression extends SurrogateExpression {}
