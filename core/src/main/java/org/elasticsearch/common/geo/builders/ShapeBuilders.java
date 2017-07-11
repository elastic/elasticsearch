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

import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;

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
    public static MultiPointBuilder newMultiPoint(List<Coordinate> points) {
        return new MultiPointBuilder(points);
    }

    /**
     * Create a new lineString
     * @return a new {@link LineStringBuilder}
     */
    public static LineStringBuilder newLineString(List<Coordinate> list) {
        return new LineStringBuilder(list);
    }

    /**
     * Create a new lineString
     * @return a new {@link LineStringBuilder}
     */
    public static LineStringBuilder newLineString(CoordinatesBuilder coordinates) {
        return new LineStringBuilder(coordinates);
    }

    /**
     * Create a new Collection of lineStrings
     * @return a new {@link MultiLineStringBuilder}
     */
    public static MultiLineStringBuilder newMultiLinestring() {
        return new MultiLineStringBuilder();
    }

    /**
     * Create a new PolygonBuilder
     * @return a new {@link PolygonBuilder}
     */
    public static PolygonBuilder newPolygon(List<Coordinate> shell) {
        return new PolygonBuilder(new CoordinatesBuilder().coordinates(shell));
    }

    /**
     * Create a new PolygonBuilder
     * @return a new {@link PolygonBuilder}
     */
    public static PolygonBuilder newPolygon(CoordinatesBuilder shell) {
        return new PolygonBuilder(shell);
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
    public static EnvelopeBuilder newEnvelope(Coordinate topLeft, Coordinate bottomRight) {
        return new EnvelopeBuilder(topLeft, bottomRight);
    }

    public static void register(List<Entry> namedWriteables) {
        namedWriteables.add(new Entry(ShapeBuilder.class, PointBuilder.TYPE.shapeName(), PointBuilder::new));
        namedWriteables.add(new Entry(ShapeBuilder.class, CircleBuilder.TYPE.shapeName(), CircleBuilder::new));
        namedWriteables.add(new Entry(ShapeBuilder.class, EnvelopeBuilder.TYPE.shapeName(), EnvelopeBuilder::new));
        namedWriteables.add(new Entry(ShapeBuilder.class, MultiPointBuilder.TYPE.shapeName(), MultiPointBuilder::new));
        namedWriteables.add(new Entry(ShapeBuilder.class, LineStringBuilder.TYPE.shapeName(), LineStringBuilder::new));
        namedWriteables.add(new Entry(ShapeBuilder.class, MultiLineStringBuilder.TYPE.shapeName(), MultiLineStringBuilder::new));
        namedWriteables.add(new Entry(ShapeBuilder.class, PolygonBuilder.TYPE.shapeName(), PolygonBuilder::new));
        namedWriteables.add(new Entry(ShapeBuilder.class, MultiPolygonBuilder.TYPE.shapeName(), MultiPolygonBuilder::new));
        namedWriteables.add(new Entry(ShapeBuilder.class, GeometryCollectionBuilder.TYPE.shapeName(), GeometryCollectionBuilder::new));
    }
}
