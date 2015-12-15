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

import com.vividsolutions.jts.geom.Coordinate;

/**
 * A collection of static methods for creating ShapeBuilders.
 */
public class ShapeBuilders {

    /**
     * Create a new point
     *
     * @param longitude longitude of the point
     * @param latitude latitude of the point
     * @return a new {@link PointBuilder}
     */
    public static PointBuilder newPoint(double longitude, double latitude) {
        return ShapeBuilders.newPoint(new Coordinate(longitude, latitude));
    }

    /**
     * Create a new {@link PointBuilder} from a {@link Coordinate}
     * @param coordinate coordinate defining the position of the point
     * @return a new {@link PointBuilder}
     */
    public static PointBuilder newPoint(Coordinate coordinate) {
        return new PointBuilder().coordinate(coordinate);
    }

    /**
     * Create a new set of points
     * @return new {@link MultiPointBuilder}
     */
    public static MultiPointBuilder newMultiPoint() {
        return new MultiPointBuilder();
    }

    /**
     * Create a new lineString
     * @return a new {@link LineStringBuilder}
     */
    public static LineStringBuilder newLineString() {
        return new LineStringBuilder();
    }

    /**
     * Create a new Collection of lineStrings
     * @return a new {@link MultiLineStringBuilder}
     */
    public static MultiLineStringBuilder newMultiLinestring() {
        return new MultiLineStringBuilder();
    }

    /**
     * Create a new Polygon
     * @return a new {@link PointBuilder}
     */
    public static PolygonBuilder newPolygon() {
        return new PolygonBuilder();
    }

    /**
     * Create a new Polygon
     * @return a new {@link PointBuilder}
     */
    public static PolygonBuilder newPolygon(ShapeBuilder.Orientation orientation) {
        return new PolygonBuilder(orientation);
    }

    /**
     * Create a new Collection of polygons
     * @return a new {@link MultiPolygonBuilder}
     */
    public static MultiPolygonBuilder newMultiPolygon() {
        return new MultiPolygonBuilder();
    }

    /**
     * Create a new Collection of polygons
     * @return a new {@link MultiPolygonBuilder}
     */
    public static MultiPolygonBuilder newMultiPolygon(ShapeBuilder.Orientation orientation) {
        return new MultiPolygonBuilder(orientation);
    }

    /**
     * Create a new GeometryCollection
     * @return a new {@link GeometryCollectionBuilder}
     */
    public static GeometryCollectionBuilder newGeometryCollection() {
        return new GeometryCollectionBuilder();
    }

    /**
     * Create a new GeometryCollection
     *
     * @return a new {@link GeometryCollectionBuilder}
     */
    public static GeometryCollectionBuilder newGeometryCollection(ShapeBuilder.Orientation orientation) {
        return new GeometryCollectionBuilder(orientation);
    }

    /**
     * create a new Circle
     *
     * @return a new {@link CircleBuilder}
     */
    public static CircleBuilder newCircleBuilder() {
        return new CircleBuilder();
    }

    /**
     * create a new rectangle
     *
     * @return a new {@link EnvelopeBuilder}
     */
    public static EnvelopeBuilder newEnvelope() {
        return new EnvelopeBuilder();
    }

    /**
     * create a new rectangle
     *
     * @return a new {@link EnvelopeBuilder}
     */
    public static EnvelopeBuilder newEnvelope(ShapeBuilder.Orientation orientation) {
        return new EnvelopeBuilder(orientation);
    }
}
