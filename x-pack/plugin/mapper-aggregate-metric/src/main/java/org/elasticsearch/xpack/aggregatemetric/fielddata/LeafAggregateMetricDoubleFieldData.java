/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.fielddata;

import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.Metric;

/**
 * {@link LeafFieldData} specialization for aggregate_metric_double data.
 */
public interface LeafAggregateMetricDoubleFieldData extends LeafFieldData {

    /**
     * Return aggregate_metric of double values for a given metric
     */
    SortedNumericDoubleValues getAggregateMetricValues(Metric metric);

}
