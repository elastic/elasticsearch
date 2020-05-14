/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.aggregatemetric.fielddata;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldData;

/**
 * Specialization of {@link IndexFieldData} for aggregate_metric.
 */
public abstract class IndexAggregateDoubleMetricFieldData implements IndexFieldData<LeafAggregateDoubleMetricFieldData> {

    protected final Index index;
    protected final String fieldName;

    public IndexAggregateDoubleMetricFieldData(Index index, String fieldName) {
        this.index = index;
        this.fieldName = fieldName;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public final void clear() {
        // can't do
    }

    @Override
    public final Index index() {
        return index;
    }
}
