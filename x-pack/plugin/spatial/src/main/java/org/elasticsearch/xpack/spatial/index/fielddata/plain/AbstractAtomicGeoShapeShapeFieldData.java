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
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues.GeoShapeValue;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafGeoShapeFieldData;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.geo.SphericalMercatorUtils.latToSphericalMercator;
import static org.elasticsearch.common.geo.SphericalMercatorUtils.lonToSphericalMercator;

public abstract class AbstractAtomicGeoShapeShapeFieldData implements LeafGeoShapeFieldData {

    private final ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory;

    public AbstractAtomicGeoShapeShapeFieldData(ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory) {
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("scripts and term aggs are not supported by geo_shape doc values");
    }

    @Override
    public final DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return toScriptFieldFactory.getScriptFieldFactory(getGeoShapeValues(), name);
    }

    public static LeafGeoShapeFieldData empty(final int maxDoc, ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory) {
        return new AbstractAtomicGeoShapeShapeFieldData(toScriptFieldFactory) {

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

    public static final class GeoShapeScriptValues extends ScriptDocValues.Geometry<GeoShapeValue> {

        private final GeometrySupplier<GeoShapeValue> gsSupplier;

        public GeoShapeScriptValues(GeometrySupplier<GeoShapeValue> supplier) {
            super(supplier);
            this.gsSupplier = supplier;
        }

        @Override
        public int getDimensionalType() {
            return gsSupplier.getInternal(0) == null ? -1 : gsSupplier.getInternal(0).dimensionalShapeType().ordinal();
        }

        @Override
        public GeoPoint getCentroid() {
            return gsSupplier.getInternal(0) == null ? null : gsSupplier.getInternalCentroid();
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
            return gsSupplier.getInternal(0) == null ? null : gsSupplier.getInternalBoundingBox();
        }

        @Override
        public GeoShapeValues.GeoShapeValue get(int index) {
            return gsSupplier.getInternal(0);
        }

        public GeoShapeValues.GeoShapeValue getValue() {
            return gsSupplier.getInternal(0);
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }
}
