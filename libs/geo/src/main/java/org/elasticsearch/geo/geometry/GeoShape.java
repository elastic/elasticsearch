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

package org.elasticsearch.geo.geometry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.geo.parsers.WKBParser;
import org.elasticsearch.geo.parsers.WKBParser.ByteOrder;
import org.elasticsearch.geo.parsers.WKTParser;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.OutputStreamDataOutput;

/**
 */
public abstract class GeoShape {
    protected Rectangle boundingBox;
    protected double area = Double.NaN;

    public Rectangle getBoundingBox() {
        return boundingBox;
    }

    public double minLat() {
        return boundingBox.minLat;
    }

    public double maxLat() {
        return boundingBox.maxLat;
    }

    public double minLon() {
        return boundingBox.minLon;
    }

    public double maxLon() {
        return boundingBox.maxLon;
    }


    public Point getCenter() {
        return boundingBox.getCenter();
    }

    public double getArea() {
        if (hasArea()) {
            if (Double.isNaN(area)) {
                area = computeArea();
            }
            return area;
        }
        throw new UnsupportedOperationException(type() + " does not have an area");
    }

    protected double computeArea() {
        throw new UnsupportedOperationException(type() + " does not have an area");
    }

    public abstract boolean hasArea();

    public abstract ShapeType type();

    public abstract Relation relate(double minLat, double maxLat, double minLon, double maxLon);

    public abstract Relation relate(GeoShape shape);

    interface ConnectedComponent {
        EdgeTree createEdgeTree();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GeoShape)) return false;
        GeoShape geoShape = (GeoShape) o;
        return Double.compare(geoShape.area, area) == 0 &&
                Objects.equals(boundingBox, geoShape.boundingBox);
    }

    @Override
    public int hashCode() {
        return Objects.hash(boundingBox, area);
    }

    public String toWKT() {
        StringBuilder sb = new StringBuilder();
        sb.append(type().wktName());
        sb.append(WKTParser.SPACE);
        sb.append(contentToWKT());
        return sb.toString();
    }

    public ByteArrayOutputStream toWKB() {
        return toWKB(null);
    }

    public ByteArrayOutputStream toWKB(ByteArrayOutputStream reuse) {
        if (reuse == null) {
            reuse = new ByteArrayOutputStream();
        }
        try (OutputStreamDataOutput out = new OutputStreamDataOutput(reuse)) {
            appendWKB(out);
        } catch (IOException e) {
            throw new RuntimeException(e);  // not possible
        }
        return reuse;
    }

    protected abstract StringBuilder contentToWKT();

    private void appendWKB(OutputStreamDataOutput out) throws IOException {
        out.writeVInt(WKBParser.ByteOrder.XDR.ordinal()); // byteOrder
        out.writeVInt(type().wkbOrdinal());     // shapeType ordinal
        appendWKBContent(out);
    }

    protected abstract void appendWKBContent(OutputStreamDataOutput out) throws IOException;

    public enum Relation {
        DISJOINT(PointValues.Relation.CELL_OUTSIDE_QUERY),
        INTERSECTS(PointValues.Relation.CELL_CROSSES_QUERY),
        CONTAINS(PointValues.Relation.CELL_CROSSES_QUERY),
        WITHIN(PointValues.Relation.CELL_INSIDE_QUERY),
        CROSSES(PointValues.Relation.CELL_CROSSES_QUERY);

        // used to translate between PointValues.Relation and full geo relations
        private final PointValues.Relation pointsRelation;

        Relation(PointValues.Relation pointsRelation) {
            this.pointsRelation = pointsRelation;
        }

        public PointValues.Relation toPointsRelation() {
            return pointsRelation;
        }

        public boolean intersects() {
            return this != DISJOINT;
        }

        public Relation transpose() {
            if (this == CONTAINS) {
                return WITHIN;
            } else if (this == WITHIN) {
                return CONTAINS;
            }
            return this;
        }
    }
}
