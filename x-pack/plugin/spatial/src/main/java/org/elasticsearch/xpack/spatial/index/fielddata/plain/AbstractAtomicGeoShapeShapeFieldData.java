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
import org.elasticsearch.script.field.DocValuesField;
import org.elasticsearch.script.field.ToScriptField;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues.GeoShapeValue;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafGeoShapeFieldData;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.geo.SphericalMercatorUtils.latToSphericalMercator;
import static org.elasticsearch.common.geo.SphericalMercatorUtils.lonToSphericalMercator;

public abstract class AbstractAtomicGeoShapeShapeFieldData implements LeafGeoShapeFieldData {

    private final ToScriptField<GeoShapeValues> toScriptField;

    public AbstractAtomicGeoShapeShapeFieldData(ToScriptField<GeoShapeValues> toScriptField) {
        this.toScriptField = toScriptField;
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("scripts and term aggs are not supported by geo_shape doc values");
    }

    @Override
    public final DocValuesField<?> getScriptField(String name) {
        return toScriptField.getScriptField(getGeoShapeValues(), name);
    }

    public static LeafGeoShapeFieldData empty(final int maxDoc, ToScriptField<GeoShapeValues> toScriptField) {
        return new AbstractAtomicGeoShapeShapeFieldData(toScriptField) {

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public void close() {}

            @Override
            public GeoShapeValues getGeoShapeValues() {
                return GeoShapeValues.EMPTY;
            }
        };
    }

    public static final class GeoShapeSupplier implements ScriptDocValues.GeometrySupplier<GeoShapeValue> {

        private final GeoShapeValues in;
        private final GeoPoint centroid = new GeoPoint();
        private final GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        private GeoShapeValues.GeoShapeValue value;

        public GeoShapeSupplier(GeoShapeValues in) {
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
        public GeoShapeValue getInternal(int index) {
            throw new UnsupportedOperationException();
        }

        public GeoShapeValue getInternal() {
            return value;
        }

        @Override
        public int size() {
            return value == null ? 0 : 1;
        }

        @Override
        public GeoPoint getInternalCentroid() {
            return centroid;
        }

        @Override
        public GeoBoundingBox getInternalBoundingBox() {
            return boundingBox;
        }
    }

    public static final class GeoShapeScriptValues extends ScriptDocValues.Geometry<GeoShapeValue> {

        private final GeoShapeSupplier gsSupplier;

        public GeoShapeScriptValues(GeoShapeSupplier supplier) {
            super(supplier);
            this.gsSupplier = supplier;
        }

        @Override
        public int getDimensionalType() {
            return gsSupplier.getInternal() == null ? -1 : gsSupplier.getInternal().dimensionalShapeType().ordinal();
        }

        @Override
        public GeoPoint getCentroid() {
            return gsSupplier.getInternal() == null ? null : gsSupplier.getInternalCentroid();
        }

        @Override
        public double getMercatorWidth() {
            return lonToSphericalMercator(getBoundingBox().right()) - lonToSphericalMercator(getBoundingBox().left());
        }

        @Override
        public double getMercatorHeight() {
            return latToSphericalMercator(getBoundingBox().top()) - latToSphericalMercator(getBoundingBox().bottom());
        }

        @Override
        public GeoBoundingBox getBoundingBox() {
            return gsSupplier.getInternal() == null ? null : gsSupplier.getInternalBoundingBox();
        }

        @Override
        public GeoShapeValues.GeoShapeValue get(int index) {
            return gsSupplier.getInternal();
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }
}
