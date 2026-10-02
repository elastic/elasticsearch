/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.support;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.LeafExponentialHistogramFieldData;

/**
 * Specialization of {@link IndexFieldData} for exponential_histogram fields.
 */
public abstract class IndexExponentialHistogramFieldData implements IndexFieldData<LeafExponentialHistogramFieldData> {

    protected final String fieldName;

    public IndexExponentialHistogramFieldData(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return AnalyticsValuesSourceType.EXPONENTIAL_HISTOGRAM;
    }
}
