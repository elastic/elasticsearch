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
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.geo.SphericalMercatorUtils.latToSphericalMercator;
import static org.elasticsearch.common.geo.SphericalMercatorUtils.lonToSphericalMercator;

public abstract class AbstractAtomicGeoShapeShapeFieldData extends LeafShapeFieldData<GeoShapeValues> {

    private static class Empty extends AbstractAtomicGeoShapeShapeFieldData {
        private final GeoShapeValues emptyValues;

        Empty(ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory, GeoShapeValues emptyValues) {
            super(toScriptFieldFactory);
            this.emptyValues = emptyValues;
        }

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
        public GeoShapeValues getShapeValues() {
            return emptyValues;
        }
    }

    public AbstractAtomicGeoShapeShapeFieldData(ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory) {
        super(toScriptFieldFactory);
    }

    public static AbstractAtomicGeoShapeShapeFieldData empty(final int maxDoc, ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory) {
        return new Empty(toScriptFieldFactory, GeoShapeValues.EMPTY);
    }

    public static final class GeoShapeScriptValues extends LeafShapeFieldData.ShapeScriptValues<GeoPoint, GeoShapeValues.GeoShapeValue>
        implements
            ScriptDocValues.Geometry {

        public GeoShapeScriptValues(GeometrySupplier<GeoPoint, GeoShapeValues.GeoShapeValue> supplier) {
            super(supplier);
        }

        @Override
        public GeoShapeValues.GeoShapeValue get(int index) {
            return super.get(index);
        }

        @Override
        public GeoShapeValues.GeoShapeValue getValue() {
            return super.getValue();
        }

        @Override
        public GeoBoundingBox getBoundingBox() {
            return (GeoBoundingBox) super.getBoundingBox();
        }

        @Override
        public double getMercatorWidth() {
            return lonToSphericalMercator(getBoundingBox().right()) - lonToSphericalMercator(getBoundingBox().left());
        }

        @Override
        public double getMercatorHeight() {
            return latToSphericalMercator(getBoundingBox().top()) - latToSphericalMercator(getBoundingBox().bottom());
        }
    }
}
