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

import java.io.IOException;

import org.elasticsearch.geo.geometry.Predicate.DistancePredicate;
import org.apache.lucene.store.OutputStreamDataOutput;

/**
 * Created by nknize on 9/25/17.
 */
public class Circle extends GeoShape {
    private final double lat;
    private final double lon;
    private final double radiusMeters;
    private DistancePredicate predicate;

    public Circle(final double lat, final double lon, final double radiusMeters) {
        this.lat = lat;
        this.lon = lon;
        this.radiusMeters = radiusMeters;
        this.boundingBox = Rectangle.fromPointDistance(lat, lon, radiusMeters);
    }

    public double getCenterLat() {
        return lat;
    }

    public double getCenterLon() {
        return lon;
    }

    public double getRadiusMeters() {
        return radiusMeters;
    }

    protected double computeArea() {
        return radiusMeters * radiusMeters * StrictMath.PI;
    }

    @Override
    public ShapeType type() {
        return ShapeType.CIRCLE;
    }

    @Override
    public boolean hasArea() {
        return true;
    }

    @Override
    public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
        return predicate().relate(minLat, maxLat, minLon, maxLon);
    }

    @Override
    public Relation relate(GeoShape shape) {
        throw new UnsupportedOperationException("not yet able to relate other GeoShape types to circles");
    }

    public boolean pointInside(final int encodedLat, final int encodedLon) {
        return predicate().test(encodedLat, encodedLon);
    }

    private DistancePredicate predicate() {
        if (predicate == null) {
            predicate = Predicate.DistancePredicate.create(lat, lon, radiusMeters);
        }
        return predicate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        Circle circle = (Circle) o;
        if (Double.compare(circle.lat, lat) != 0) return false;
        if (Double.compare(circle.lon, lon) != 0) return false;
        if (Double.compare(circle.radiusMeters, radiusMeters) != 0) return false;
        return predicate.equals(circle.predicate);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(lat);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(lon);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(radiusMeters);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + predicate.hashCode();
        return result;
    }

    @Override
    public String toWKT() {
        throw new UnsupportedOperationException("The WKT spec does not support CIRCLE geometry");
    }


    @Override
    protected StringBuilder contentToWKT() {
        throw new UnsupportedOperationException("The WKT spec does not support CIRCLE geometry");
    }

    @Override
    protected void appendWKBContent(OutputStreamDataOutput out) throws IOException {
        out.writeVLong(Double.doubleToRawLongBits(lat));
        out.writeVLong(Double.doubleToRawLongBits(lon));
        out.writeVLong(Double.doubleToRawLongBits(radiusMeters));
    }
}
