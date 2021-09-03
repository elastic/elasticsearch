/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.Field;
import org.elasticsearch.script.FieldValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafGeoShapeFieldData;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.geo.SphericalMercatorUtils.latToSphericalMercator;
import static org.elasticsearch.common.geo.SphericalMercatorUtils.lonToSphericalMercator;

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
        private final GeoPoint centroid = new GeoPoint();
        private final GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        private GeoShapeValues.GeoShapeValue value;

        private GeoShapeScriptValues(GeoShapeValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                value = in.value();
                centroid.reset(value.lat(), value.lon());
                boundingBox.topLeft().reset(value.boundingBox().maxY(), value.boundingBox().minX());
                boundingBox.bottomRight().reset(value.boundingBox().minY(), value.boundingBox().maxX());
            } else {
                value = null;
            }
        }

        @Override
        public int getDimensionalType() {
            return value == null ? -1 : value.dimensionalShapeType().ordinal();
        }

        @Override
        public GeoPoint getCentroid() {
            return value == null ? null : centroid;
        }

        @Override
        public double getMercatorWidth() {
            return lonToSphericalMercator(boundingBox.right()) - lonToSphericalMercator(boundingBox.left());
        }

        @Override
        public double getMercatorHeight() {
            return latToSphericalMercator(boundingBox.top()) - latToSphericalMercator(boundingBox.bottom());
        }

        @Override
        public GeoBoundingBox getBoundingBox() {
            return value == null ? null : boundingBox;
        }

        @Override
        public GeoShapeValues.GeoShapeValue get(int index) {
            return value;
        }

        @Override
        public int size() {
            return value == null ? 0 : 1;
        }

        @Override
        public Field<GeoShapeValues.GeoShapeValue> toField(String fieldName) {
            return new GeoShapeField(fieldName, this);
        }
    }

    public static class GeoShapeField extends Field<GeoShapeValues.GeoShapeValue> {
        public GeoShapeField(String name, FieldValues<GeoShapeValues.GeoShapeValue> values) {
            super(name, values);
        }
    }
}
