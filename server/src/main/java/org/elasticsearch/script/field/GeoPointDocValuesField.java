/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

public class GeoPointDocValuesField extends PointDocValuesField<GeoPoint> {
    // maintain bwc by making centroid and bounding box available to ScriptDocValues.GeoPoints
    private ScriptDocValues.GeoPoints geoPoints = null;

    public GeoPointDocValuesField(MultiGeoPointValues input, String name) {
        super(input, name, GeoPoint::new, new GeoBoundingBox(new GeoPoint(), new GeoPoint()), new GeoPoint[0]);
    }

    @Override
    protected void resetPointAt(int i, GeoPoint point) {
        values[i].reset(point.lat(), point.lon());
    }

    @Override
    protected void resetCentroidAndBounds(GeoPoint point, GeoPoint topLeft, GeoPoint bottomRight) {
        centroid.reset(point.lat() / count, point.lon() / count);
        boundingBox.topLeft().reset(topLeft.lat(), topLeft.lon());
        boundingBox.bottomRight().reset(bottomRight.lat(), bottomRight.lon());
    }

    @Override
    protected double getXFrom(GeoPoint point) {
        return point.lon();
    }

    @Override
    protected double getYFrom(GeoPoint point) {
        return point.lat();
    }

    @Override
    protected GeoPoint pointOf(double x, double y) {
        return new GeoPoint(y, x);
    }

    @Override
    protected double planeDistance(double x1, double y1, GeoPoint point) {
        return GeoUtils.planeDistance(y1, x1, point.lat(), point.lon());
    }

    @Override
    public GeoPoint get(GeoPoint defaultValue) {
        // While this method seems redundant, it is needed for painless scripting method lookups which cannot handle generics
        return super.get(defaultValue);
    }

    @Override
    public GeoPoint get(int index, GeoPoint defaultValue) {
        // While this method seems redundant, it is needed for painless scripting method lookups which cannot handle generics
        return super.get(index, defaultValue);
    }

    @Override
    public ScriptDocValues<GeoPoint> toScriptDocValues() {
        if (geoPoints == null) {
            geoPoints = new ScriptDocValues.GeoPoints(this);
        }

        return geoPoints;
    }
}
