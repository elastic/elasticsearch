/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeScriptDocValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

public final class GeoShapeScriptFieldData extends AbstractShapeIndexFieldData<GeoShapeValues> {
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final GeometryFieldScript.LeafFactory leafFactory;
        private final ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory;

        public Builder(
            String name,
            GeometryFieldScript.LeafFactory leafFactory,
            ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory
        ) {
            this.name = name;
            this.leafFactory = leafFactory;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public GeoShapeScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new GeoShapeScriptFieldData(name, leafFactory, toScriptFieldFactory);
        }
    }

    private final GeometryFieldScript.LeafFactory leafFactory;

    private GeoShapeScriptFieldData(
        String fieldName,
        GeometryFieldScript.LeafFactory leafFactory,
        ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory
    ) {
        super(fieldName, GeoShapeValuesSourceType.instance(), toScriptFieldFactory);
        this.leafFactory = leafFactory;
    }

    @Override
    protected IllegalArgumentException sortException() {
        throw new IllegalArgumentException("can't sort on geo_shape field");
    }

    @Override
    public LeafShapeFieldData<GeoShapeValues> load(LeafReaderContext context) {
        final GeometryFieldScript script = leafFactory.newInstance(context);
        return new LeafShapeFieldData<>(toScriptFieldFactory) {
            @Override
            public GeoShapeValues getShapeValues() {
                return new GeoShapeScriptDocValues(script, fieldName);
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public void close() {

            }
        };
    }
}
