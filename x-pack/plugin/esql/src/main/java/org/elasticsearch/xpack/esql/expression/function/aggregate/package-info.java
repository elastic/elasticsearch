/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * TODO Checklist:
 * - Discuss with the team about the new aggregation
 * - Add CSV tests for it
 * - Adding aggregation. Check existing base classes, which simplify some configurations
 * - List aggregation methods
 *   - Add a implement SurrogateExpression and surrogate() if constants, or if some case can be optimally solved with existing functions
 *   - Add a implement ToAggregator and suppliers() that will eventually return the aggregators in the next section
 *     - Call [Agg][Type]AggregatorFunctionSupplier from there for each specific type
 *
 * - Make An aggregator at `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/`
 *   - Make a `X-*.java.st` template if required for multiple types
 *   - Test with AggregatorFunctionTestCase for easy aggregator tests per type
 *   - Methods:
 *     - initSingle and initGrouping
 *     - combine and combineIntermediate
 *     - evaluateFinal
 *   - State class and methods:
 *     - toIntermediate
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;
