/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo.builders;

import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Point;

import java.io.IOException;

public class PointBuilder extends ShapeBuilder<Point, org.elasticsearch.geometry.Point, PointBuilder> {
    public static final GeoShapeType TYPE = GeoShapeType.POINT;

    /**
     * Create a point at [0.0,0.0]
     */
    public PointBuilder() {
        super();
        this.coordinates.add(ZERO_ZERO);
    }

    public PointBuilder(double lon, double lat) {
        //super(new ArrayList<>(1));
        super();
        this.coordinates.add(new Coordinate(lon, lat));
    }

    public PointBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public PointBuilder coordinate(Coordinate coordinate) {
        this.coordinates.set(0, coordinate);
        return this;
    }

    public double longitude() {
        return coordinates.get(0).x;
    }

    public double latitude() {
        return coordinates.get(0).y;
    }

    /**
     * Create a new point
     *
     * @param longitude longitude of the point
     * @param latitude latitude of the point
     * @return a new {@link PointBuilder}
     */
    public static PointBuilder newPoint(double longitude, double latitude) {
        return new PointBuilder().coordinate(new Coordinate(longitude, latitude));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
       builder.startObject();
       builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
       builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
       toXContent(builder, coordinates.get(0));
       return builder.endObject();
    }

    @Override
    public Point buildS4J() {
        return SPATIAL_CONTEXT.makePoint(coordinates.get(0).x, coordinates.get(0).y);
    }

    @Override
    public org.elasticsearch.geometry.Point buildGeometry() {
        return new org.elasticsearch.geometry.Point(coordinates.get(0).x, coordinates.get(0).y);
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public int numDimensions() {
        return Double.isNaN(coordinates.get(0).z) ? 2 : 3;
    }
}
