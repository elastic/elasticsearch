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

import org.locationtech.spatial4j.shape.Shape;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.XShapeCollection;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GeometryCollectionBuilder extends ShapeBuilder {

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
        builder.field(FIELD_TYPE, TYPE.shapeName());
        builder.startArray(FIELD_GEOMETRIES);
        for (ShapeBuilder shape : shapes) {
            shape.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public Shape build() {

        List<Shape> shapes = new ArrayList<>(this.shapes.size());

        for (ShapeBuilder shape : this.shapes) {
            shapes.add(shape.build());
        }

        if (shapes.size() == 1)
            return shapes.get(0);
        else
            return new XShapeCollection<>(shapes, SPATIAL_CONTEXT);
        //note: ShapeCollection is probably faster than a Multi* geom.
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
