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

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;


/**
 * Utility class that transforms Elasticsearch geometry objects to the Lucene representation
 */
public class GeoShapeUtils {

    public static org.apache.lucene.geo.Polygon toLucenePolygon(Polygon polygon) {
        org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
        for(int i = 0; i<holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.Polygon(polygon.getHole(i).getY(), polygon.getHole(i).getX());
        }
        return new org.apache.lucene.geo.Polygon(polygon.getPolygon().getY(), polygon.getPolygon().getX(), holes);
    }

    public static org.apache.lucene.geo.Polygon toLucenePolygon(Rectangle r) {
        return new org.apache.lucene.geo.Polygon(
            new double[]{r.getMinLat(), r.getMinLat(), r.getMaxLat(), r.getMaxLat(), r.getMinLat()},
            new double[]{r.getMinLon(), r.getMaxLon(), r.getMaxLon(), r.getMinLon(), r.getMinLon()});
    }

    public static org.apache.lucene.geo.Rectangle toLuceneRectangle(Rectangle r) {
        return new org.apache.lucene.geo.Rectangle(r.getMinLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon());
    }

    public static org.apache.lucene.geo.Point toLucenePoint(Point point) {
        return new org.apache.lucene.geo.Point(point.getLat(), point.getLon());
    }

    public static org.apache.lucene.geo.Line toLuceneLine(Line line) {
        return new org.apache.lucene.geo.Line(line.getLats(), line.getLons());
    }

    public static org.apache.lucene.geo.Circle toLuceneCircle(Circle circle) {
        return new org.apache.lucene.geo.Circle(circle.getLat(), circle.getLon(), circle.getRadiusMeters());
    }

    private GeoShapeUtils() {
    }

}
