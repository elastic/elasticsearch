/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.function.Function;

/**
 * A factory that takes a {@link LogicalPlan} and returns another {@link LogicalPlan}.
 * This is used to chaining sub-plans after they've been created by the parser.
 */
public interface PlanFactory extends Function<LogicalPlan, LogicalPlan> {}
