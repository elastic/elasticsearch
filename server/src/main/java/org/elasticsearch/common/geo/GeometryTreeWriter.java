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
package org.elasticsearch.common.geo;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.GeometryVisitor;
import org.elasticsearch.geo.geometry.Line;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.geo.geometry.Rectangle;
import org.elasticsearch.geo.geometry.ShapeType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GeometryTreeWriter {

    private final GeometryTreeBuilder builder;

    public GeometryTreeWriter(Geometry geometry) {
        builder = new GeometryTreeBuilder();
        geometry.visit(builder);
    }

    public BytesRef toBytesRef() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeVInt(builder.shapeWriters.size());
        for (EdgeTreeWriter writer : builder.shapeWriters) {
            output.writeEnum(ShapeType.POLYGON);
            output.writeBytesRef(writer.toBytesRef());
        }
        output.close();
        return output.bytes().toBytesRef();
    }

    class GeometryTreeBuilder implements GeometryVisitor<Void> {

        private List<EdgeTreeWriter> shapeWriters;

        GeometryTreeBuilder() {
            shapeWriters = new ArrayList<>();
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {
            // TODO
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            // TODO (support holes)
            LinearRing outerShell = polygon.getPolygon();
            shapeWriters.add(new EdgeTreeWriter(asIntArray(outerShell.getLons()), asIntArray(outerShell.getLats())));
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for (Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            int[] lats = new int[] { (int) r.getMinLat(), (int) r.getMinLat(), (int) r.getMaxLat(), (int) r.getMaxLat(),
                (int) r.getMinLat()};
            int[] lons = new int[] { (int) r.getMinLon(), (int) r.getMaxLon(), (int) r.getMaxLon(), (int) r.getMinLon(),
                (int) r.getMinLon()};
            shapeWriters.add(new EdgeTreeWriter(lons, lats));
            return null;
        }

        @Override
        public Void visit(Point point) {
            // TODO
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            // TODO
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [Circle]");
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle]");
        }

        private int[] asIntArray(double[] doub) {
            int[] intArr = new int[doub.length];
            for (int i = 0; i < intArr.length; i++) {
                intArr[i] = (int) doub[i];
            }
            return intArr;
        }
    }
}
