/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

public interface TTest extends NumericMetricsAggregation.SingleValue {

    double getValue();

}
