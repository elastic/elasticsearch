/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;

import static org.elasticsearch.common.geo.SphericalMercatorUtils.latToSphericalMercator;
import static org.elasticsearch.common.geo.SphericalMercatorUtils.lonToSphericalMercator;

public final class GeoShapeScriptValues extends LeafShapeFieldData.ShapeScriptValues<GeoPoint, GeoShapeValues.GeoShapeValue>
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
