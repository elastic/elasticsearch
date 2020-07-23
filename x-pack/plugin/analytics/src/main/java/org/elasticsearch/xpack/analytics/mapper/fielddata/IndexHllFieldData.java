/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper.fielddata;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public abstract class IndexHllFieldData implements IndexFieldData<LeafHllFieldData> {

    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;

    public IndexHllFieldData(String fieldName, ValuesSourceType valuesSourceType) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public final void clear() {
        // can't do
    }
}
