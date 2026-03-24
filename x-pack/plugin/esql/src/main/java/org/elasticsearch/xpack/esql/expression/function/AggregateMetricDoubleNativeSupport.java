/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

/**
 * Marker interface to say that an aggregate funciton supports aggregate_metric_double natively
 * i.e. max, min, sum, count, avg
 */
public interface AggregateMetricDoubleNativeSupport {}
