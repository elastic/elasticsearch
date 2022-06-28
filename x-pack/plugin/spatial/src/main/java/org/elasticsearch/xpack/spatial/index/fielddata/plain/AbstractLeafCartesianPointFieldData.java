/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.elasticsearch.index.fielddata.LeafPointFieldData;
import org.elasticsearch.index.fielddata.MultiPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSource;

public abstract class AbstractLeafCartesianPointFieldData extends LeafPointFieldData<CartesianPoint> {

    protected final ToScriptFieldFactory<MultiPointValues<CartesianPoint>> toScriptFieldFactory;

    public AbstractLeafCartesianPointFieldData(ToScriptFieldFactory<MultiPointValues<CartesianPoint>> toScriptFieldFactory) {
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public final CartesianPointValuesSource.CartesianPointValues getPointValues() {
        return new CartesianPointValuesSource.CartesianPointValues(getSortedNumericDocValues());
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("scripts and term aggs are not supported by point doc values");
    }

    @Override
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return toScriptFieldFactory.getScriptFieldFactory(getPointValues(), name);
    }
}
