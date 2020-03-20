/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.aggregatemetric.fielddata;


import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;

/**
 * Specialization of {@link IndexFieldData} for aggregate_metric.
 */
public abstract class IndexAggregateDoubleMetricFieldData extends DocValuesIndexFieldData
    implements IndexFieldData<LeafAggregateDoubleMetricFieldData> {

    public IndexAggregateDoubleMetricFieldData(Index index, String fieldName) {
        super(index, fieldName);
    }
}
