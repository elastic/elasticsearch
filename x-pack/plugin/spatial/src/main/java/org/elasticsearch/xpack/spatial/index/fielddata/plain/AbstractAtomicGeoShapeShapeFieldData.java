/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafGeoShapeFieldData;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public abstract class AbstractAtomicGeoShapeShapeFieldData implements LeafGeoShapeFieldData {

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("scripts and term aggs are not supported by geo_shape doc values");
    }

    @Override
    public final ScriptDocValues.Geometry<GeoShapeValues.GeoShapeValue> getScriptValues() {
        return new GeoShapeScriptValues(getGeoShapeValues());
    }

    public static LeafGeoShapeFieldData empty(final int maxDoc) {
        return new AbstractAtomicGeoShapeShapeFieldData() {

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public void close() {
            }

            @Override
            public GeoShapeValues getGeoShapeValues() {
                return GeoShapeValues.EMPTY;
            }
        };
    }

    private static final class GeoShapeScriptValues extends ScriptDocValues.Geometry<GeoShapeValues.GeoShapeValue> {

        private final GeoShapeValues in;
        private GeoShapeValues.GeoShapeValue value;

        private GeoShapeScriptValues(GeoShapeValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            value = in.advanceExact(docId) ? in.value() : null;
        }

        @Override
        public double getCentroidLat() {
            return value.lat();
        }

        @Override
        public double getCentroidLon() {
            return value.lon();
        }

        @Override
        public double width() {
            return value.boundingBox().maxX() - value.boundingBox().minX();
        }

        @Override
        public double height() {
            return value.boundingBox().maxY() - value.boundingBox().minY();
        }

        @Override
        public GeoShapeValues.GeoShapeValue get(int index) {
            return value;
        }

        @Override
        public int size() {
            return 1;
        }
    }
}
