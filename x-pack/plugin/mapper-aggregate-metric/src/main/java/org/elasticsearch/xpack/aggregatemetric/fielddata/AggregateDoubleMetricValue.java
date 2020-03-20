/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.fielddata;

/**
 * Per-document aggregate_metric value.
 */
public abstract class AggregateDoubleMetricValue {

    /**
     * the current value of the aggregate metric
     * @return the current value of the aggregate metric
     */
    public abstract double doubleValue();

}
