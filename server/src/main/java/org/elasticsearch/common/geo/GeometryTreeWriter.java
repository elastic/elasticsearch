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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is a tree-writer that serializes the
 * appropriate tree structure for each type of
 * {@link Geometry} into a byte array.
 */
public class GeometryTreeWriter implements Writeable {

    private final GeometryTreeBuilder builder;

    public GeometryTreeWriter(Geometry geometry) {
        builder = new GeometryTreeBuilder();
        geometry.visit(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // only write a geometry extent for the tree if the tree
        // contains multiple sub-shapes
        boolean prependExtent = builder.shapeWriters.size() > 1;
        Extent extent = null;
        if (prependExtent) {
            extent = new Extent(builder.top, builder.bottom, builder.negLeft, builder.negRight, builder.posLeft, builder.posRight);
        }
        out.writeOptionalWriteable(extent);
        out.writeVInt(builder.shapeWriters.size());
        for (ShapeTreeWriter writer : builder.shapeWriters) {
            out.writeEnum(writer.getShapeType());
            writer.writeTo(out);
        }
    }

    class GeometryTreeBuilder implements GeometryVisitor<Void, RuntimeException> {

        private List<ShapeTreeWriter> shapeWriters;
        // integers are used to represent int-encoded lat/lon values
        int top = Integer.MIN_VALUE;
        int bottom = Integer.MAX_VALUE;
        int negLeft = Integer.MAX_VALUE;
        int negRight = Integer.MIN_VALUE;
        int posLeft = Integer.MAX_VALUE;
        int posRight = Integer.MIN_VALUE;

        GeometryTreeBuilder() {
            shapeWriters = new ArrayList<>();
        }

        private void addWriter(ShapeTreeWriter writer) {
            Extent extent = writer.getExtent();
            top = Math.max(top, extent.top);
            bottom = Math.min(bottom, extent.bottom);
            negLeft = Math.min(negLeft, extent.negLeft);
            negRight = Math.max(negRight, extent.negRight);
            posLeft = Math.min(posLeft, extent.posLeft);
            posRight = Math.max(posRight, extent.posRight);
            shapeWriters.add(writer);
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
            addWriter(new EdgeTreeWriter(asLonEncodedArray(line.getLons()), asLatEncodedArray(line.getLats())));
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) {
            int size = multiLine.size();
            List<int[]> x = new ArrayList<>(size);
            List<int[]> y = new ArrayList<>(size);
            for (Line line : multiLine) {
                x.add(asLonEncodedArray(line.getLons()));
                y.add(asLatEncodedArray(line.getLats()));
            }
            addWriter(new EdgeTreeWriter(x, y));
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            LinearRing outerShell = polygon.getPolygon();
            int numHoles = polygon.getNumberOfHoles();
            List<int[]> x = new ArrayList<>(numHoles);
            List<int[]> y = new ArrayList<>(numHoles);
            for (int i = 0; i < numHoles; i++) {
                LinearRing innerRing = polygon.getHole(i);
                x.add(asLonEncodedArray(innerRing.getLons()));
                y.add(asLatEncodedArray(innerRing.getLats()));
            }
            addWriter(new PolygonTreeWriter(asLonEncodedArray(outerShell.getLons()), asLatEncodedArray(outerShell.getLats()), x, y));
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
            int encodedMinLat = GeoEncodingUtils.encodeLatitude(r.getMinLat());
            int encodedMaxLat = GeoEncodingUtils.encodeLatitude(r.getMaxLat());
            int encodedMinLon = GeoEncodingUtils.encodeLongitude(r.getMinLon());
            int encodedMaxLon = GeoEncodingUtils.encodeLongitude(r.getMaxLon());
            int[] lats = new int[] { encodedMinLat, encodedMinLat, encodedMaxLat, encodedMaxLat, encodedMinLat };
            int[] lons = new int[] { encodedMinLon, encodedMaxLon, encodedMaxLon, encodedMinLon, encodedMinLon };
            addWriter(new PolygonTreeWriter(lons, lats, Collections.emptyList(), Collections.emptyList()));
            return null;
        }

        @Override
        public Void visit(Point point) {
            int x = GeoEncodingUtils.encodeLongitude(point.getLon());
            int y = GeoEncodingUtils.encodeLatitude(point.getLat());
            Point2DWriter writer = new Point2DWriter(x, y);
            addWriter(writer);
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            int[] x = new int[multiPoint.size()];
            int[] y = new int[x.length];
            for (int i = 0; i < multiPoint.size(); i++) {
                x[i] = GeoEncodingUtils.encodeLongitude(multiPoint.get(i).getLon());
                y[i] = GeoEncodingUtils.encodeLatitude(multiPoint.get(i).getLat());
            }
            Point2DWriter writer = new Point2DWriter(x, y);
            addWriter(writer);
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

        private int[] asLonEncodedArray(double[] doub) {
            int[] intArr = new int[doub.length];
            for (int i = 0; i < intArr.length; i++) {
                intArr[i] = GeoEncodingUtils.encodeLongitude(doub[i]);
            }
            return intArr;
        }

        private int[] asLatEncodedArray(double[] doub) {
            int[] intArr = new int[doub.length];
            for (int i = 0; i < intArr.length; i++) {
                intArr[i] = GeoEncodingUtils.encodeLatitude(doub[i]);
            }
            return intArr;
        }
    }
}
