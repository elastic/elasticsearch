/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Expression evaluation infrastructure. Most actual expressions are defined in
 * {@code ESQL}, not the compute engine. Here you'll mostly find constants.
 * <p>
 *     Also see places like {@link org.elasticsearch.compute.operator.EvalOperator}
 *     and {@link org.elasticsearch.compute.operator.FilterOperator} for how
 *     these are used at runtime.
 * </p>
 */
package org.elasticsearch.compute.expression;
