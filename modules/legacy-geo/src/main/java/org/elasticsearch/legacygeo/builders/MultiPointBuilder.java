/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.builders;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.legacygeo.GeoShapeType;
import org.elasticsearch.legacygeo.XShapeCollection;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Point;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MultiPointBuilder extends ShapeBuilder<XShapeCollection<Point>, MultiPoint, MultiPointBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.MULTIPOINT;

    /**
     * Create a new {@link MultiPointBuilder}.
     * @param coordinates needs at least two coordinates to be valid, otherwise will throw an exception
     */
    public MultiPointBuilder(List<Coordinate> coordinates) {
        super(coordinates);
    }

    /**
     * Creates a new empty MultiPoint builder
     */
    public MultiPointBuilder() {
        super();
    }

    /**
     * Read from a stream.
     */
    public MultiPointBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
        super.coordinatesToXcontent(builder, false);
        builder.endObject();
        return builder;
    }

    @Override
    public XShapeCollection<Point> buildS4J() {
        // Could wrap JtsGeometry but probably slower due to conversions to/from JTS in relate()
        // MultiPoint geometry = FACTORY.createMultiPoint(points.toArray(new Coordinate[points.size()]));
        List<Point> shapes = new ArrayList<>(coordinates.size());
        for (Coordinate coord : coordinates) {
            shapes.add(SPATIAL_CONTEXT.makePoint(coord.x, coord.y));
        }
        XShapeCollection<Point> multiPoints = new XShapeCollection<>(shapes, SPATIAL_CONTEXT);
        multiPoints.setPointsOnly(true);
        return multiPoints;
    }

    @Override
    public MultiPoint buildGeometry() {
        if (coordinates.isEmpty()) {
            return MultiPoint.EMPTY;
        }
        return new MultiPoint(
            coordinates.stream().map(coord -> new org.elasticsearch.geometry.Point(coord.x, coord.y)).collect(Collectors.toList())
        );
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public int numDimensions() {
        if (coordinates == null || coordinates.isEmpty()) {
            throw new IllegalStateException("unable to get number of dimensions, " + "LineString has not yet been initialized");
        }
        return Double.isNaN(coordinates.get(0).z) ? 2 : 3;
    }
}
