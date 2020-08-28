/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo.builders;

import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.XShapeCollection;
import org.elasticsearch.common.geo.parsers.GeoWKTParser;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class MultiPolygonBuilder extends ShapeBuilder<Shape, MultiPolygon, MultiPolygonBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.MULTIPOLYGON;

    private final List<PolygonBuilder> polygons = new ArrayList<>();

    private final Orientation orientation;

    /**
     * Build a MultiPolygonBuilder with RIGHT orientation.
     */
    public MultiPolygonBuilder() {
        this(Orientation.RIGHT);
    }

    /**
     * Build a MultiPolygonBuilder with an arbitrary orientation.
     */
    public MultiPolygonBuilder(Orientation orientation) {
        this.orientation = orientation;
    }

    /**
     * Read from a stream.
     */
    public MultiPolygonBuilder(StreamInput in) throws IOException {
        orientation = Orientation.readFrom(in);
        int holes = in.readVInt();
        for (int i = 0; i < holes; i++) {
            polygon(new PolygonBuilder(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        orientation.writeTo(out);
        out.writeVInt(polygons.size());
        for (PolygonBuilder polygon : polygons) {
            polygon.writeTo(out);
        }
    }

    public Orientation orientation() {
        return this.orientation;
    }

    /**
     * Add a shallow copy of the polygon to the multipolygon. This will apply the orientation of the
     * {@link MultiPolygonBuilder} to the polygon if polygon has different orientation.
     */
    public MultiPolygonBuilder polygon(PolygonBuilder polygon) {
        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder().coordinates(polygon.shell().coordinates(false)), this.orientation);
        for (LineStringBuilder hole : polygon.holes()) {
            pb.hole(hole);
        }
        this.polygons.add(pb);
        return this;
    }

    /**
     * get the list of polygons
     */
    public List<PolygonBuilder> polygons() {
        return polygons;
    }

    private static String polygonCoordinatesToWKT(PolygonBuilder polygon) {
        StringBuilder sb = new StringBuilder();
        sb.append(GeoWKTParser.LPAREN);
        sb.append(ShapeBuilder.coordinateListToWKT(polygon.shell().coordinates));
        for (LineStringBuilder hole : polygon.holes()) {
            sb.append(GeoWKTParser.COMMA);
            sb.append(ShapeBuilder.coordinateListToWKT(hole.coordinates));
        }
        sb.append(GeoWKTParser.RPAREN);
        return sb.toString();
    }

    @Override
    protected StringBuilder contentToWKT() {
        final StringBuilder sb = new StringBuilder();
        if (polygons.isEmpty()) {
            sb.append(GeoWKTParser.EMPTY);
        } else {
            sb.append(GeoWKTParser.LPAREN);
            if (polygons.size() > 0) {
                sb.append(polygonCoordinatesToWKT(polygons.get(0)));
            }
            for (int i = 1; i < polygons.size(); ++i) {
                sb.append(GeoWKTParser.COMMA);
                sb.append(polygonCoordinatesToWKT(polygons.get(i)));
            }
            sb.append(GeoWKTParser.RPAREN);
        }
        return sb;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.field(ShapeParser.FIELD_ORIENTATION.getPreferredName(), orientation.name().toLowerCase(Locale.ROOT));
        builder.startArray(ShapeParser.FIELD_COORDINATES.getPreferredName());
        for(PolygonBuilder polygon : polygons) {
            builder.startArray();
            polygon.coordinatesArray(builder, params);
            builder.endArray();
        }
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public int numDimensions() {
        if (polygons == null || polygons.isEmpty()) {
            throw new IllegalStateException("unable to get number of dimensions, " +
                "Polygons have not yet been initialized");
        }
        return polygons.get(0).numDimensions();
    }

    @Override
    public Shape buildS4J() {

        List<Shape> shapes = new ArrayList<>(this.polygons.size());

        if(wrapdateline) {
            for (PolygonBuilder polygon : this.polygons) {
                for(Coordinate[][] part : polygon.coordinates()) {
                    shapes.add(jtsGeometry(PolygonBuilder.polygonS4J(FACTORY, part)));
                }
            }
        } else {
            for (PolygonBuilder polygon : this.polygons) {
                shapes.add(jtsGeometry(polygon.toPolygonS4J(FACTORY)));
            }
        }
        if (shapes.size() == 1)
            return shapes.get(0);
        else
            return new XShapeCollection<>(shapes, SPATIAL_CONTEXT);
        //note: ShapeCollection is probably faster than a Multi* geom.
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public MultiPolygon buildGeometry() {
        List<Polygon> shapes = new ArrayList<>(this.polygons.size());
        Object poly;
        for (PolygonBuilder polygon : this.polygons) {
            poly = polygon.buildGeometry();
            if (poly instanceof List) {
                shapes.addAll((List<Polygon>) poly);
            } else {
                shapes.add((Polygon)poly);
            }
        }
        if (shapes.isEmpty()) {
            return MultiPolygon.EMPTY;
        }
        return new MultiPolygon(shapes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(polygons, orientation);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MultiPolygonBuilder other = (MultiPolygonBuilder) obj;
        return Objects.equals(polygons, other.polygons) &&
                Objects.equals(orientation, other.orientation);
    }
}
