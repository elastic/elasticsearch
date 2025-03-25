/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

/**
 * An interface indicates that this is a time-series aggregator and it requires time-series source
 */
public interface ToTimeSeriesAggregator extends ToAggregator {

}
