/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class InternalGeoBounds extends InternalBounds<GeoPoint> {
    public final boolean wrapLongitude;

    public InternalGeoBounds(
        String name,
        double top,
        double bottom,
        double posLeft,
        double posRight,
        double negLeft,
        double negRight,
        boolean wrapLongitude,
        Map<String, Object> metadata
    ) {
        super(name, top, bottom, posLeft, posRight, negLeft, negRight, metadata);
        this.wrapLongitude = wrapLongitude;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoBounds(StreamInput in) throws IOException {
        super(in);
        wrapLongitude = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeBoolean(wrapLongitude);
    }

    @Override
    public String getWriteableName() {
        return GeoBoundsAggregationBuilder.NAME;
    }

    @Override
    protected InternalGeoBounds makeInternalBounds(
        String name,
        double top,
        double bottom,
        double posLeft,
        double posRight,
        double negLeft,
        double negRight,
        Map<String, Object> metadata
    ) {
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude, getMetadata());
    }

    @Override
    protected Object selectCoordinate(String coordinateString, GeoPoint cornerPoint) {
        return switch (coordinateString) {
            case "lat" -> cornerPoint.lat();
            case "lon" -> cornerPoint.lon();
            default -> throw new IllegalArgumentException("Found unknown path element [" + coordinateString + "] in [" + getName() + "]");
        };
    }

    @Override
    protected GeoBoundingBox resolveBoundingBox() {
        if (Double.isInfinite(top)) {
            return null;
        } else if (Double.isInfinite(posLeft)) {
            return new GeoBoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, negRight));
        } else if (Double.isInfinite(negLeft)) {
            return new GeoBoundingBox(new GeoPoint(top, posLeft), new GeoPoint(bottom, posRight));
        } else if (wrapLongitude) {
            double unwrappedWidth = posRight - negLeft;
            double wrappedWidth = (180 - posLeft) - (-180 - negRight);
            if (unwrappedWidth <= wrappedWidth) {
                return new GeoBoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, posRight));
            } else {
                return new GeoBoundingBox(new GeoPoint(top, posLeft), new GeoPoint(bottom, negRight));
            }
        } else {
            return new GeoBoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, posRight));
        }
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && wrapLongitude == ((InternalGeoBounds) obj).wrapLongitude;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), wrapLongitude);
    }
}
