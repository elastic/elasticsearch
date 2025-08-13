/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.legacygeo.builders;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.legacygeo.GeoShapeType;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LineStringBuilder extends ShapeBuilder<JtsGeometry, org.elasticsearch.geometry.Geometry, LineStringBuilder> {
    public static final GeoShapeType TYPE = GeoShapeType.LINESTRING;

    /**
     * Construct a new LineString.
     * Per GeoJSON spec (http://geojson.org/geojson-spec.html#linestring)
     * a LineString must contain two or more coordinates
     * @param coordinates the initial list of coordinates
     * @throws IllegalArgumentException if there are less then two coordinates defined
     */
    public LineStringBuilder(List<Coordinate> coordinates) {
        super(coordinates);
        if (coordinates.size() < 2) {
            throw new IllegalArgumentException(
                "invalid number of points in LineString (found [" + coordinates.size() + "] - must be >= 2)"
            );
        }
    }

    public LineStringBuilder(CoordinatesBuilder coordinates) {
        this(coordinates.build());
    }

    /**
     * Read from a stream.
     */
    public LineStringBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
        coordinatesToXcontent(builder, false);
        builder.endObject();
        return builder;
    }

    /**
     * Closes the current lineString by adding the starting point as the end point.
     * This will have no effect if starting and end point are already the same.
     */
    public LineStringBuilder close() {
        Coordinate start = coordinates.get(0);
        Coordinate end = coordinates.get(coordinates.size() - 1);
        if (start.x != end.x || start.y != end.y) {
            coordinates.add(start);
        }
        return this;
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

    @Override
    public JtsGeometry buildS4J() {
        Coordinate[] coordinates = this.coordinates.toArray(new Coordinate[this.coordinates.size()]);
        Geometry geometry;
        if (wrapdateline) {
            ArrayList<LineString> strings = decomposeS4J(FACTORY, coordinates, new ArrayList<LineString>());

            if (strings.size() == 1) {
                geometry = strings.get(0);
            } else {
                LineString[] linestrings = strings.toArray(new LineString[strings.size()]);
                geometry = FACTORY.createMultiLineString(linestrings);
            }

        } else {
            geometry = FACTORY.createLineString(coordinates);
        }
        return jtsGeometry(geometry);
    }

    @Override
    public org.elasticsearch.geometry.Geometry buildGeometry() {
        return new Line(coordinates.stream().mapToDouble(i -> i.x).toArray(), coordinates.stream().mapToDouble(i -> i.y).toArray());
    }

    static ArrayList<LineString> decomposeS4J(GeometryFactory factory, Coordinate[] coordinates, ArrayList<LineString> strings) {
        for (Coordinate[] part : decompose(+DATELINE, coordinates)) {
            for (Coordinate[] line : decompose(-DATELINE, part)) {
                strings.add(factory.createLineString(line));
            }
        }
        return strings;
    }

    /**
     * Decompose a linestring given as array of coordinates at a vertical line.
     *
     * @param dateline x-axis intercept of the vertical line
     * @param coordinates coordinates forming the linestring
     * @return array of linestrings given as coordinate arrays
     */
    private static Coordinate[][] decompose(double dateline, Coordinate[] coordinates) {
        int offset = 0;
        ArrayList<Coordinate[]> parts = new ArrayList<>();

        double shift = coordinates[0].x > DATELINE ? DATELINE : (coordinates[0].x < -DATELINE ? -DATELINE : 0);

        for (int i = 1; i < coordinates.length; i++) {
            double t = intersection(coordinates[i - 1], coordinates[i], dateline);
            if (Double.isNaN(t) == false) {
                Coordinate[] part;
                if (t < 1) {
                    part = Arrays.copyOfRange(coordinates, offset, i + 1);
                    part[part.length - 1] = Edge.position(coordinates[i - 1], coordinates[i], t);
                    coordinates[offset + i - 1] = Edge.position(coordinates[i - 1], coordinates[i], t);
                    shift(shift, part);
                    offset = i - 1;
                    shift = coordinates[i].x > DATELINE ? DATELINE : (coordinates[i].x < -DATELINE ? -DATELINE : 0);
                } else {
                    part = shift(shift, Arrays.copyOfRange(coordinates, offset, i + 1));
                    offset = i;
                }
                parts.add(part);
            }
        }

        if (offset == 0) {
            parts.add(shift(shift, coordinates));
        } else if (offset < coordinates.length - 1) {
            Coordinate[] part = Arrays.copyOfRange(coordinates, offset, coordinates.length);
            parts.add(shift(shift, part));
        }
        return parts.toArray(new Coordinate[parts.size()][]);
    }

    private static Coordinate[] shift(double shift, Coordinate... coordinates) {
        if (shift != 0) {
            for (int j = 0; j < coordinates.length; j++) {
                coordinates[j] = new Coordinate(coordinates[j].x - 2 * shift, coordinates[j].y);
            }
        }
        return coordinates;
    }
}
