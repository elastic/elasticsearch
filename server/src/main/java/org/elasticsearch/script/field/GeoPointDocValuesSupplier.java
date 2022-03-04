/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class GeoPointDocValuesSupplier implements DocValuesSupplier<GeoPoint>, FieldSupplier.Supplier<GeoPoint> {

    protected final MultiGeoPointValues input;

    protected GeoPoint[] values = new GeoPoint[0];
    protected int count;

    // maintain bwc by making centroid and bounding box available to ScriptDocValues.GeoPoints
    private final GeoPoint centroid = new GeoPoint();
    private final GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());

    public GeoPointDocValuesSupplier(MultiGeoPointValues input) {
        this.input = input;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            if (count == 1) {
                setSingleValue();
            } else {
                setMultiValue();
            }
        } else {
            resize(0);
        }
    }

    protected void resize(int newSize) {
        count = newSize;
        if (newSize > values.length) {
            int oldLength = values.length;
            values = ArrayUtil.grow(values, count);
            for (int i = oldLength; i < values.length; ++i) {
                values[i] = new GeoPoint();
            }
        }
    }

    protected void setSingleValue() throws IOException {
        GeoPoint point = input.nextValue();
        values[0].reset(point.lat(), point.lon());
        centroid.reset(point.lat(), point.lon());
        boundingBox.topLeft().reset(point.lat(), point.lon());
        boundingBox.bottomRight().reset(point.lat(), point.lon());
    }

    protected void setMultiValue() throws IOException {
        double centroidLat = 0;
        double centroidLon = 0;
        double maxLon = Double.NEGATIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double minLat = Double.POSITIVE_INFINITY;
        for (int i = 0; i < count; i++) {
            GeoPoint point = input.nextValue();
            values[i].reset(point.lat(), point.lon());
            centroidLat += point.getLat();
            centroidLon += point.getLon();
            maxLon = Math.max(maxLon, values[i].getLon());
            minLon = Math.min(minLon, values[i].getLon());
            maxLat = Math.max(maxLat, values[i].getLat());
            minLat = Math.min(minLat, values[i].getLat());
        }
        centroid.reset(centroidLat / count, centroidLon / count);
        boundingBox.topLeft().reset(maxLat, minLon);
        boundingBox.bottomRight().reset(minLat, maxLon);
    }

    @Override
    public GeoPoint getCompatible(int index) {
        return values[index];
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public GeoPoint get(int index) {
        return values[index];
    }

    public GeoPoint getCentroid() {
        return centroid;
    }

    public GeoBoundingBox getBoundingBox() {
        return boundingBox;
    }
}
