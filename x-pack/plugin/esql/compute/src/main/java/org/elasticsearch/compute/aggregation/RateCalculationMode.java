/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

/**
 * The mode to use when calculating a rate of change in a time series.
 */
public enum RateCalculationMode {
    RATE,
    DELTA,
    INCREASE
}
