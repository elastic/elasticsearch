/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafPointFieldData;
import org.elasticsearch.index.fielddata.MultiPointValues;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexCartesianPointFieldData;

public class CartesianPointIndexFieldData extends AbstractLatLonPointIndexFieldData<CartesianPoint>
    implements
        IndexCartesianPointFieldData {
    public CartesianPointIndexFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<MultiPointValues<CartesianPoint>> toScriptFieldFactory
    ) {
        super(fieldName, valuesSourceType, toScriptFieldFactory);
    }

    @Override
    public LeafPointFieldData<CartesianPoint> load(LeafReaderContext context) {
        LeafReader reader = context.reader();
        FieldInfo info = reader.getFieldInfos().fieldInfo(fieldName);
        if (info != null) {
            checkCompatible(info);
        }
        return new CartesianPointDVLeafFieldData(reader, fieldName, toScriptFieldFactory);
    }

    @Override
    public LeafPointFieldData<CartesianPoint> loadDirect(LeafReaderContext context) {
        return load(context);
    }

    /** helper: checks a fieldinfo and throws exception if it's definitely not a LatLonDocValuesField */
    static void checkCompatible(FieldInfo fieldInfo) {
        // dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
        if (fieldInfo.getDocValuesType() != DocValuesType.NONE
            && fieldInfo.getDocValuesType() != LatLonDocValuesField.TYPE.docValuesType()) {
            throw new IllegalArgumentException(
                "field=\""
                    + fieldInfo.name
                    + "\" was indexed with docValuesType="
                    + fieldInfo.getDocValuesType()
                    + " but this type has docValuesType="
                    + LatLonDocValuesField.TYPE.docValuesType()
                    + ", is the field really a LatLonDocValuesField?"
            );
        }
    }

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ValuesSourceType valuesSourceType;
        private final ToScriptFieldFactory<MultiPointValues<CartesianPoint>> toScriptFieldFactory;

        public Builder(
            String name,
            ValuesSourceType valuesSourceType,
            ToScriptFieldFactory<MultiPointValues<CartesianPoint>> toScriptFieldFactory
        ) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            // ignore breaker
            return new CartesianPointIndexFieldData(name, valuesSourceType, toScriptFieldFactory);
        }
    }
}
