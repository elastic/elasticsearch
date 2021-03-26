/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo.builders;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.XShapeCollection;
import org.elasticsearch.common.geo.parsers.GeoWKTParser;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GeometryCollectionBuilder extends ShapeBuilder<Shape,
    GeometryCollection<Geometry>, GeometryCollectionBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.GEOMETRYCOLLECTION;

    /**
     * List of shapes. Package scope for testing.
     */
    final List<ShapeBuilder> shapes = new ArrayList<>();

    /**
     * Build and empty GeometryCollectionBuilder.
     */
    public GeometryCollectionBuilder() {
    }

    /**
     * Read from a stream.
     */
    public GeometryCollectionBuilder(StreamInput in) throws IOException {
        int shapes = in.readVInt();
        for (int i = 0; i < shapes; i++) {
            shape(in.readNamedWriteable(ShapeBuilder.class));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shapes.size());
        for (ShapeBuilder shape : shapes) {
            out.writeNamedWriteable(shape);
        }
    }

    public GeometryCollectionBuilder shape(ShapeBuilder shape) {
        this.shapes.add(shape);
        return this;
    }

    public GeometryCollectionBuilder point(PointBuilder point) {
        this.shapes.add(point);
        return this;
    }

    public GeometryCollectionBuilder multiPoint(MultiPointBuilder multiPoint) {
        this.shapes.add(multiPoint);
        return this;
    }

    public GeometryCollectionBuilder line(LineStringBuilder line) {
        this.shapes.add(line);
        return this;
    }

    public GeometryCollectionBuilder multiLine(MultiLineStringBuilder multiLine) {
        this.shapes.add(multiLine);
        return this;
    }

    public GeometryCollectionBuilder polygon(PolygonBuilder polygon) {
        this.shapes.add(polygon);
        return this;
    }

    public GeometryCollectionBuilder multiPolygon(MultiPolygonBuilder multiPolygon) {
        this.shapes.add(multiPolygon);
        return this;
    }

    public GeometryCollectionBuilder envelope(EnvelopeBuilder envelope) {
        this.shapes.add(envelope);
        return this;
    }

    public GeometryCollectionBuilder circle(CircleBuilder circle) {
        this.shapes.add(circle);
        return this;
    }

    public ShapeBuilder getShapeAt(int i) {
        if (i >= this.shapes.size() || i < 0) {
            throw new ElasticsearchException("GeometryCollection contains " + this.shapes.size() + " shapes. + " +
                    "No shape found at index " + i);
        }
        return this.shapes.get(i);
    }

    public int numShapes() {
        return this.shapes.size();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.startArray(ShapeParser.FIELD_GEOMETRIES.getPreferredName());
        for (ShapeBuilder shape : shapes) {
            shape.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    @Override
    protected StringBuilder contentToWKT() {
        StringBuilder sb = new StringBuilder();
        if (shapes.isEmpty()) {
            sb.append(GeoWKTParser.EMPTY);
        } else {
            sb.append(GeoWKTParser.LPAREN);
            sb.append(shapes.get(0).toWKT());
            for (int i = 1; i < shapes.size(); ++i) {
                sb.append(GeoWKTParser.COMMA);
                sb.append(shapes.get(i).toWKT());
            }
            sb.append(GeoWKTParser.RPAREN);
        }
        return sb;
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public int numDimensions() {
        if (shapes == null || shapes.isEmpty()) {
            throw new IllegalStateException("unable to get number of dimensions, " +
                "GeometryCollection has not yet been initialized");
        }
        return shapes.get(0).numDimensions();
    }

    @Override
    public Shape buildS4J() {

        List<Shape> shapes = new ArrayList<>(this.shapes.size());

        for (ShapeBuilder shape : this.shapes) {
            shapes.add(shape.buildS4J());
        }

        if (shapes.size() == 1)
            return shapes.get(0);
        else
            return new XShapeCollection<>(shapes, SPATIAL_CONTEXT);
        //note: ShapeCollection is probably faster than a Multi* geom.
    }

    @Override
    public GeometryCollection<Geometry> buildGeometry() {
        if (this.shapes.isEmpty()) {
            return GeometryCollection.EMPTY;
        }
        List<Geometry> shapes = new ArrayList<>(this.shapes.size());

        for (ShapeBuilder shape : this.shapes) {
            shapes.add(shape.buildGeometry());
        }

        return new GeometryCollection<>(shapes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shapes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        GeometryCollectionBuilder other = (GeometryCollectionBuilder) obj;
        return Objects.equals(shapes, other.shapes);
    }
}
