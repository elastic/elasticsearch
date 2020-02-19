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

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
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

import java.util.Comparator;

/**
 * Like {@link ShapeType} but has specific
 * types for when the geometry is a {@link GeometryCollection} and
 * more information about what the highest-dimensional sub-shape
 * is.
 */
public enum DimensionalShapeType {
    POINT,
    MULTIPOINT,
    LINESTRING,
    MULTILINESTRING,
    POLYGON,
    MULTIPOLYGON,
    GEOMETRYCOLLECTION_POINTS,     // highest-dimensional shapes are Points
    GEOMETRYCOLLECTION_LINES,      // highest-dimensional shapes are Lines
    GEOMETRYCOLLECTION_POLYGONS;   // highest-dimensional shapes are Polygons

    public static Comparator<DimensionalShapeType> COMPARATOR = Comparator.comparingInt(DimensionalShapeType::centroidDimension);

    private static DimensionalShapeType[] values = values();

    public static DimensionalShapeType max(DimensionalShapeType s1, DimensionalShapeType s2) {
        if (s1 == null) {
            return s2;
        } else if (s2 == null) {
            return s1;
        }
        return COMPARATOR.compare(s1, s2) >= 0 ? s1 : s2;
    }

    public static DimensionalShapeType fromOrdinalByte(byte ordinal) {
        return values[Byte.toUnsignedInt(ordinal)];
    }

    public void writeTo(ByteBuffersDataOutput out) {
        out.writeByte((byte) ordinal());
    }

    public static DimensionalShapeType readFrom(ByteArrayDataInput in) {
        return fromOrdinalByte(in.readByte());
    }

    public static DimensionalShapeType forGeometry(Geometry geometry) {
        return geometry.visit(new GeometryVisitor<>() {
            private DimensionalShapeType st = null;

            @Override
            public DimensionalShapeType visit(Circle circle) {
                throw new IllegalArgumentException("invalid shape type found [Circle] while computing dimensional shape type");
            }

            @Override
            public DimensionalShapeType visit(Line line) {
                st = DimensionalShapeType.max(st, DimensionalShapeType.LINESTRING);
                return st;
            }

            @Override
            public DimensionalShapeType visit(LinearRing ring)  {
                throw new UnsupportedOperationException("should not visit LinearRing");
            }

            @Override
            public DimensionalShapeType visit(MultiLine multiLine)  {
                st = DimensionalShapeType.max(st, DimensionalShapeType.MULTILINESTRING);
                return st;
            }

            @Override
            public DimensionalShapeType visit(MultiPoint multiPoint)  {
                st = DimensionalShapeType.max(st, DimensionalShapeType.MULTIPOINT);
                return st;
            }

            @Override
            public DimensionalShapeType visit(MultiPolygon multiPolygon)  {
                st = DimensionalShapeType.max(st, DimensionalShapeType.MULTIPOLYGON);
                return st;
            }

            @Override
            public DimensionalShapeType visit(Point point)  {
                st = DimensionalShapeType.max(st, DimensionalShapeType.POINT);
                return st;
            }

            @Override
            public DimensionalShapeType visit(Polygon polygon)  {
                st = DimensionalShapeType.max(st, DimensionalShapeType.POLYGON);
                return st;
            }

            @Override
            public DimensionalShapeType visit(Rectangle rectangle)  {
                st = DimensionalShapeType.max(st, DimensionalShapeType.POLYGON);
                return st;
            }

            @Override
            public DimensionalShapeType visit(GeometryCollection<?> collection)  {
                for (Geometry shape : collection) {
                    shape.visit(this);
                }
                int dimension = st.centroidDimension();
                if (dimension == 0) {
                    return DimensionalShapeType.GEOMETRYCOLLECTION_POINTS;
                } else if (dimension == 1) {
                    return DimensionalShapeType.GEOMETRYCOLLECTION_LINES;
                } else {
                    return DimensionalShapeType.GEOMETRYCOLLECTION_POLYGONS;
                }
            }
        });
    }

    /**
     * The integer representation of the dimension for the specific
     * dimensional shape type. This is to be used by the centroid
     * calculation to determine whether to add a sub-shape's centroid
     * to the overall shape calculation.
     *
     * @return 0 for points, 1 for lines, 2 for polygons
     */
    private int centroidDimension() {
        switch (this) {
            case POINT:
            case MULTIPOINT:
            case GEOMETRYCOLLECTION_POINTS:
                return 0;
            case LINESTRING:
            case MULTILINESTRING:
            case GEOMETRYCOLLECTION_LINES:
                return 1;
            case POLYGON:
            case MULTIPOLYGON:
            case GEOMETRYCOLLECTION_POLYGONS:
                return 2;
            default:
                throw new IllegalStateException("dimension calculation of DimensionalShapeType [" + this + "] is not supported");
        }
    }
}
