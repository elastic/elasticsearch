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

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.GeoShapeIndexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * This is a tree-writer that serializes a {@link Geometry} and tessellate it to write it into a byte array.
 * Internally it tessellate the given {@link Geometry} and it builds an interval tree with the
 * tessellation.
 */
public class TriangleTreeWriter extends ShapeTreeWriter {

    private final TriangleTreeNode node;
    private final CoordinateEncoder coordinateEncoder;
    private final CentroidCalculator centroidCalculator;
    private final ShapeType type;
    private final int highestDimension;
    private Extent extent;

    public TriangleTreeWriter(Geometry geometry, CoordinateEncoder coordinateEncoder) {
        this.coordinateEncoder = coordinateEncoder;
        this.centroidCalculator = new CentroidCalculator();
        this.extent = new Extent();
        this.type = geometry.type();
        TriangleTreeBuilder builder = new TriangleTreeBuilder(coordinateEncoder);
        geometry.visit(builder);
        this.node = builder.build();
        this.highestDimension = builder.highestDimension();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(coordinateEncoder.encodeX(centroidCalculator.getX()));
        out.writeInt(coordinateEncoder.encodeY(centroidCalculator.getY()));
        out.writeEnum(type);
        if (ShapeType.GEOMETRYCOLLECTION.equals(type)) {
            // if the shape is a geometry-collection, there
            // needs to be extra information about what the highest
            // dimensional sub-shape is present for centroid calculations
            out.writeVInt(highestDimension);
        }
        out.writeInt(extent.top);
        out.writeVLong((long) extent.top - extent.bottom);
        out.writeInt(extent.posRight);
        out.writeInt(extent.posLeft);
        out.writeInt(extent.negRight);
        out.writeInt(extent.negLeft);
        node.writeTo(out);
    }

    @Override
    public Extent getExtent() {
        return extent;
    }

    @Override
    public ShapeType getShapeType() {
        return type;
    }

    @Override
    public CentroidCalculator getCentroidCalculator() {
        return centroidCalculator;
    }

    /**
     * Class that tessellate the geometry and build an interval tree in memory.
     */
    class TriangleTreeBuilder implements GeometryVisitor<Void, RuntimeException> {

        private final List<TriangleTreeLeaf> triangles;
        private final CoordinateEncoder coordinateEncoder;
        // the the dimension of the highest-dimensional shape present in the tree.
        // the empty builder is initialized to zero and updated when the geometry
        // is visited
        private int highestDimension = 0;


        TriangleTreeBuilder(CoordinateEncoder coordinateEncoder) {
            this.coordinateEncoder = coordinateEncoder;
            this.triangles = new ArrayList<>();
        }

        int highestDimension() {
            return highestDimension;
        }

        private void addTriangles(List<TriangleTreeLeaf> triangles) {
            this.triangles.addAll(triangles);
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
            for (int i = 0; i < line.length(); i++) {
                centroidCalculator.addCoordinate(line.getX(i), line.getY(i));
            }
            org.apache.lucene.geo.Line luceneLine = GeoShapeIndexer.toLuceneLine(line);
            addToExtent(luceneLine.minLon, luceneLine.maxLon, luceneLine.minLat, luceneLine.maxLat);
            addTriangles(TriangleTreeLeaf.fromLine(coordinateEncoder, luceneLine));
            highestDimension = Math.max(highestDimension, 2);
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
            // TODO: Shall we consider holes for centroid computation?
            for (int i =0; i < polygon.getPolygon().length() - 1; i++) {
                centroidCalculator.addCoordinate(polygon.getPolygon().getX(i), polygon.getPolygon().getY(i));
            }
            org.apache.lucene.geo.Polygon lucenePolygon = GeoShapeIndexer.toLucenePolygon(polygon);
            addToExtent(lucenePolygon.minLon, lucenePolygon.maxLon, lucenePolygon.minLat, lucenePolygon.maxLat);
            addTriangles(TriangleTreeLeaf.fromPolygon(coordinateEncoder, lucenePolygon));
            highestDimension = Math.max(highestDimension, 3);
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
            centroidCalculator.addCoordinate(r.getMinX(), r.getMinY());
            centroidCalculator.addCoordinate(r.getMaxX(), r.getMaxY());
            addToExtent(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat());
            addTriangles(TriangleTreeLeaf.fromRectangle(coordinateEncoder, r));
            highestDimension = Math.max(highestDimension, 2);
            return null;
        }

        @Override
        public Void visit(Point point) {
            centroidCalculator.addCoordinate(point.getX(), point.getY());
            addToExtent(point.getLon(), point.getLon(), point.getLat(), point.getLat());
            addTriangles(TriangleTreeLeaf.fromPoints(coordinateEncoder, point));
            highestDimension = Math.max(highestDimension, 1);
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for (Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing]");
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle]");
        }

        private void addToExtent(double minLon, double maxLon, double minLat, double maxLat) {
            int minX = coordinateEncoder.encodeX(minLon);
            int maxX = coordinateEncoder.encodeX(maxLon);
            int minY = coordinateEncoder.encodeY(minLat);
            int maxY = coordinateEncoder.encodeY(maxLat);
            extent.addRectangle(minX, minY, maxX, maxY);
        }


        public TriangleTreeNode build() {
            if (triangles.size() == 1) {
                return new TriangleTreeNode(triangles.get(0));
            }
            TriangleTreeNode[] nodes = new TriangleTreeNode[triangles.size()];
            for (int i = 0; i < triangles.size(); i++) {
                nodes[i] = new TriangleTreeNode(triangles.get(i));
            }
            TriangleTreeNode root =  createTree(nodes, 0, triangles.size() - 1, true);
            return root;
        }

        /** Creates tree from sorted components (with range low and high inclusive) */
        private TriangleTreeNode createTree(TriangleTreeNode[] components, int low, int high, boolean splitX) {
            if (low > high) {
                return null;
            }
            final int mid = (low + high) >>> 1;
            if (low < high) {
                Comparator<TriangleTreeNode> comparator;
                if (splitX) {
                    comparator = (left, right) -> {
                        int ret = Double.compare(left.minX, right.minX);
                        if (ret == 0) {
                            ret = Double.compare(left.maxX, right.maxX);
                        }
                        return ret;
                    };
                } else {
                    comparator = (left, right) -> {
                        int ret = Double.compare(left.minY, right.minY);
                        if (ret == 0) {
                            ret = Double.compare(left.maxY, right.maxY);
                        }
                        return ret;
                    };
                }
                ArrayUtil.select(components, low, high + 1, mid, comparator);
            }
            TriangleTreeNode newNode = components[mid];
            // find children
            newNode.left = createTree(components, low, mid - 1, !splitX);
            newNode.right = createTree(components, mid + 1, high, !splitX);

            // pull up max values to this node
            if (newNode.left != null) {
                newNode.maxX = Math.max(newNode.maxX, newNode.left.maxX);
                newNode.maxY = Math.max(newNode.maxY, newNode.left.maxY);
            }
            if (newNode.right != null) {
                newNode.maxX = Math.max(newNode.maxX, newNode.right.maxX);
                newNode.maxY = Math.max(newNode.maxY, newNode.right.maxY);
            }
            return newNode;
        }
    }

    /**
     * Represents an inner node of the tree.
     */
    static class TriangleTreeNode implements Writeable {
        /** minimum latitude of this geometry's bounding box area */
        private int minY;
        /** maximum latitude of this geometry's bounding box area */
        private int maxY;
        /** minimum longitude of this geometry's bounding box area */
        private int minX;
        /** maximum longitude of this geometry's bounding box area */
        private int maxX;
        // child components, or null. Note internal nodes might mot have
        // a consistent bounding box. Internal nodes should not be accessed
        // outside if this class.
        private TriangleTreeNode left;
        private TriangleTreeNode right;
        /** root node of edge tree */
        private TriangleTreeLeaf component;

        protected TriangleTreeNode(TriangleTreeLeaf component) {
            this.minY = component.minY;
            this.maxY = component.maxY;
            this.minX = component.minX;
            this.maxX = component.maxX;
            this.component = component;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            BytesStreamOutput scratchBuffer = new BytesStreamOutput();
            writeMetadata(out);
            writeComponent(out);
            if (left != null) {
                left.writeNode(out, maxX, maxY, scratchBuffer);
            }
            if (right != null) {
                right.writeNode(out, maxX, maxY, scratchBuffer);
            }
        }

        private void writeNode(StreamOutput out, int parentMaxX, int parentMaxY, BytesStreamOutput scratchBuffer) throws IOException {
            out.writeVLong((long) parentMaxX - maxX);
            out.writeVLong((long) parentMaxY - maxY);
            int size = nodeSize(false, parentMaxX, parentMaxY, scratchBuffer);
            out.writeVInt(size);
            writeMetadata(out);
            writeComponent(out);
            if (left != null) {
                left.writeNode(out, maxX, maxY, scratchBuffer);
            }
            if (right != null) {
                int rightSize = right.nodeSize(true, maxX, maxY,scratchBuffer);
                out.writeVInt(rightSize);
                right.writeNode(out, maxX, maxY, scratchBuffer);
            }
        }

        private void writeMetadata(StreamOutput out) throws IOException {
            byte metadata = 0;
            metadata |= (left != null) ? (1 << 0) : 0;
            metadata |= (right != null) ? (1 << 1) : 0;
            if (component.type == TriangleTreeLeaf.TYPE.POINT) {
                metadata |= (1 << 2);
            } else if (component.type == TriangleTreeLeaf.TYPE.LINE) {
                metadata |= (1 << 3);
            } else {
                metadata |= (component.ab) ? (1 << 4) : 0;
                metadata |= (component.bc) ? (1 << 5) : 0;
                metadata |= (component.ca) ? (1 << 6) : 0;
            }
            out.writeByte(metadata);
        }

        private void writeComponent(StreamOutput out) throws IOException {
            if (component.type == TriangleTreeLeaf.TYPE.POINT) {
                out.writeVLong((long) maxX - component.aX);
                out.writeVLong((long) maxY - component.aY);
            } else if (component.type == TriangleTreeLeaf.TYPE.LINE) {
                out.writeVLong((long) maxX - component.aX);
                out.writeVLong((long) maxY - component.aY);
                out.writeVLong((long) maxX - component.bX);
                out.writeVLong((long) maxY - component.bY);
            } else {
                out.writeVLong((long) maxX - component.aX);
                out.writeVLong((long) maxY - component.aY);
                out.writeVLong((long) maxX - component.bX);
                out.writeVLong((long) maxY - component.bY);
                out.writeVLong((long) maxX - component.cX);
                out.writeVLong((long) maxY - component.cY);
            }
        }

        public int nodeSize(boolean includeBox, int parentMaxX, int parentMaxY, BytesStreamOutput scratchBuffer) throws IOException {
            int size =0;
            size++; //metadata
            size += componentSize(scratchBuffer);
            if (left != null) {
                size +=  left.nodeSize(true, maxX, maxY, scratchBuffer);
            }
            if (right != null) {
                int rightSize = right.nodeSize(true, maxX, maxY, scratchBuffer);
                scratchBuffer.reset();
                scratchBuffer.writeVLong(rightSize);
                size +=  scratchBuffer.size(); // jump size
                size +=  rightSize;
            }
            if (includeBox) {
                int jumpSize = size;
                scratchBuffer.reset();
                scratchBuffer.writeVLong((long) parentMaxX - maxX);
                scratchBuffer.writeVLong((long) parentMaxY - maxY);
                scratchBuffer.writeVLong(jumpSize);
                size += scratchBuffer.size();// box
            }
            return size;
        }

        public int componentSize(BytesStreamOutput scratchBuffer) throws IOException {
            scratchBuffer.reset();
            if (component.type == TriangleTreeLeaf.TYPE.POINT) {
                scratchBuffer.writeVLong((long) maxX - component.aX);
                scratchBuffer.writeVLong((long) maxY - component.aY);
            } else if (component.type == TriangleTreeLeaf.TYPE.LINE) {
                scratchBuffer.writeVLong((long) maxX - component.aX);
                scratchBuffer.writeVLong((long) maxY - component.aY);
                scratchBuffer.writeVLong((long) maxX - component.bX);
                scratchBuffer.writeVLong((long) maxY - component.bY);
            } else {
                scratchBuffer.writeVLong((long) maxX - component.aX);
                scratchBuffer.writeVLong((long) maxY - component.aY);
                scratchBuffer.writeVLong((long) maxX - component.bX);
                scratchBuffer.writeVLong((long) maxY - component.bY);
                scratchBuffer.writeVLong((long) maxX - component.cX);
                scratchBuffer.writeVLong((long) maxY - component.cY);
            }
            return scratchBuffer.size();
        }

    }

    /**
     * Represents an leaf of the tree containing one of the triangles.
     */
    static class TriangleTreeLeaf {

        public enum TYPE {
            POINT, LINE, TRIANGLE
        }

        int minX, maxX, minY, maxY;
        int aX, aY, bX, bY, cX, cY;
        boolean ab, bc, ca;
        TYPE type;

        // constructor for points
        TriangleTreeLeaf(int aXencoded, int aYencoded) {
            encodePoint(aXencoded, aYencoded);
        }

        // constructor for points and lines
        TriangleTreeLeaf(int aXencoded, int aYencoded, int bXencoded, int bYencoded) {
            if (aXencoded == bXencoded && aYencoded == bYencoded) {
                encodePoint(aXencoded, aYencoded);
            } else {
                encodeLine(aXencoded, aYencoded, bXencoded, bYencoded);
            }
        }

        // generic constructor
        TriangleTreeLeaf(int aXencoded, int aYencoded, boolean ab,
                         int bXencoded, int bYencoded, boolean bc,
                         int cXencoded, int cYencoded, boolean ca) {
            if (aXencoded == bXencoded && aYencoded == bYencoded) {
                if (aXencoded == cXencoded && aYencoded == cYencoded) {
                    encodePoint(aYencoded, aXencoded);
                } else {
                    encodeLine(aYencoded, aXencoded, cYencoded, cXencoded);
                    return;
                }
            } else if (aXencoded == cXencoded && aYencoded == cYencoded) {
                encodeLine(aYencoded, aXencoded, bYencoded, bXencoded);
            } else {
                encodeTriangle(aXencoded, aYencoded, ab, bXencoded, bYencoded, bc, cXencoded, cYencoded, ca);
            }
        }

        private void encodePoint(int aXencoded, int aYencoded) {
            this.type = TYPE.POINT;
            aX = aXencoded;
            aY = aYencoded;
            minX = aX;
            maxX = aX;
            minY = aY;
            maxY = aY;
        }

        private void encodeLine(int aXencoded, int aYencoded, int bXencoded, int bYencoded) {
            this.type = TYPE.LINE;
            //rotate edges and place minX at the beginning
            if (aXencoded > bXencoded) {
                aX = bXencoded;
                aY = bYencoded;
                bX = aXencoded;
                bY = aYencoded;
            } else {
                aX = aXencoded;
                aY = aYencoded;
                bX = bXencoded;
                bY = bYencoded;
            }
            this.minX = aX;
            this.maxX = bX;
            this.minY = Math.min(aY, bY);
            this.maxY = Math.max(aY, bY);
        }

        private void encodeTriangle(int aXencoded, int aYencoded, boolean abFromShape,
                                    int bXencoded, int bYencoded, boolean bcFromShape,
                                    int cXencoded, int cYencoded, boolean caFromShape) {

            int aX, aY, bX, bY, cX, cY;
            boolean ab, bc, ca;
            //change orientation if CW
            if (GeoUtils.orient(aXencoded, aYencoded, bXencoded, bYencoded, cXencoded, cYencoded) == -1) {
                aX = cXencoded;
                bX = bXencoded;
                cX = aXencoded;
                aY = cYencoded;
                bY = bYencoded;
                cY = aYencoded;
                ab = bcFromShape;
                bc = abFromShape;
                ca = caFromShape;
            } else {
                aX = aXencoded;
                bX = bXencoded;
                cX = cXencoded;
                aY = aYencoded;
                bY = bYencoded;
                cY = cYencoded;
                ab = abFromShape;
                bc = bcFromShape;
                ca = caFromShape;
            }
            //rotate edges and place minX at the beginning
            if (bX < aX || cX < aX) {
                if (bX < cX) {
                    int tempX = aX;
                    int tempY = aY;
                    boolean tempBool = ab;
                    aX = bX;
                    aY = bY;
                    ab = bc;
                    bX = cX;
                    bY = cY;
                    bc = ca;
                    cX = tempX;
                    cY = tempY;
                    ca = tempBool;
                } else if (cX < aX) {
                    int tempX = aX;
                    int tempY = aY;
                    boolean tempBool = ab;
                    aX = cX;
                    aY = cY;
                    ab = ca;
                    cX = bX;
                    cY = bY;
                    ca = bc;
                    bX = tempX;
                    bY = tempY;
                    bc = tempBool;
                }
            } else if (aX == bX && aX == cX) {
                //degenerated case, all points with same longitude
                //we need to prevent that aX is in the middle (not part of the MBS)
                if (bY < aY || cY < aY) {
                    if (bY < cY) {
                        int tempX = aX;
                        int tempY = aY;
                        boolean tempBool = ab;
                        aX = bX;
                        aY = bY;
                        ab = bc;
                        bX = cX;
                        bY = cY;
                        bc = ca;
                        cX = tempX;
                        cY = tempY;
                        ca = tempBool;
                    } else if (cY < aY) {
                        int tempX = aX;
                        int tempY = aY;
                        boolean tempBool = ab;
                        aX = cX;
                        aY = cY;
                        ab = ca;
                        cX = bX;
                        cY = bY;
                        ca = bc;
                        bX = tempX;
                        bY = tempY;
                        bc = tempBool;
                    }
                }
            }
            this.aX = aX;
            this.aY = aY;
            this.bX = bX;
            this.bY = bY;
            this.cX = cX;
            this.cY = cY;
            this.ab = ab;
            this.bc = bc;
            this.ca = ca;
            this.minX = aX;
            this.maxX = Math.max(aX, Math.max(bX, cX));
            this.minY = Math.min(aY, Math.min(bY, cY));
            this.maxY = Math.max(aY, Math.max(bY, cY));
            type = TYPE.TRIANGLE;
        }

        private static List<TriangleTreeLeaf> fromPoints(CoordinateEncoder encoder, Point... points) {
            List<TriangleTreeLeaf> triangles = new ArrayList<>(points.length);
            for (int i = 0; i < points.length; i++) {
                triangles.add(new TriangleTreeLeaf(encoder.encodeX(points[i].getX()), encoder.encodeY(points[i].getY())));
            }
            return triangles;
        }

        private static List<TriangleTreeLeaf> fromRectangle(CoordinateEncoder encoder, Rectangle... rectangles) {
            List<TriangleTreeLeaf> triangles = new ArrayList<>(2 * rectangles.length);
            for (Rectangle r : rectangles) {
                triangles.add(new TriangleTreeLeaf(
                    encoder.encodeX(r.getMinX()), encoder.encodeY(r.getMinY()), true,
                    encoder.encodeX(r.getMaxX()), encoder.encodeY(r.getMinY()), false,
                    encoder.encodeX(r.getMinX()), encoder.encodeY(r.getMaxY()), true));
                triangles.add(new TriangleTreeLeaf(
                    encoder.encodeX(r.getMinX()), encoder.encodeY(r.getMaxY()), false,
                    encoder.encodeX(r.getMaxX()), encoder.encodeY(r.getMinY()), true,
                    encoder.encodeX(r.getMaxX()), encoder.encodeY(r.getMaxY()), true));
            }
            return triangles;
        }

        private static List<TriangleTreeLeaf> fromLine(CoordinateEncoder encoder, org.apache.lucene.geo.Line line) {
            List<TriangleTreeLeaf> triangles = new ArrayList<>(line.numPoints() - 1);
            for (int i = 0, j = 1; i < line.numPoints() - 1; i++, j++) {
                triangles.add(new TriangleTreeLeaf(encoder.encodeX(line.getLon(i)), encoder.encodeY(line.getLat(i)),
                    encoder.encodeX(line.getLon(j)), encoder.encodeY(line.getLat(j))));
            }
            return triangles;
        }

        private static List<TriangleTreeLeaf> fromPolygon(CoordinateEncoder encoder, org.apache.lucene.geo.Polygon polygon) {
            // TODO: We are going to be tessellating the polygon twice, can we do something?
            // TODO: Tessellator seems to have some reference to the encoding but does not need to have.
            List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
            List<TriangleTreeLeaf> triangles = new ArrayList<>(tessellation.size());
            for (Tessellator.Triangle t : tessellation) {
                triangles.add(new TriangleTreeLeaf(encoder.encodeX(t.getX(0)), encoder.encodeY(t.getY(0)), t.isEdgefromPolygon(0),
                    encoder.encodeX(t.getX(1)), encoder.encodeY(t.getY(1)), t.isEdgefromPolygon(1),
                    encoder.encodeX(t.getX(2)), encoder.encodeY(t.getY(2)), t.isEdgefromPolygon(2)));
            }
            return triangles;
        }
    }
}
