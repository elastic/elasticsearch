/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;

import java.util.List;

/**
 * Expressions that have a mapping to an {@link AggregatorFunctionSupplier}.
 */
public interface ToAggregator {
    AggregatorFunctionSupplier supplier(BigArrays bigArrays, List<Integer> inputChannels);
}
