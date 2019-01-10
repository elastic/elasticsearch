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

/**
 * Represents a closed line on the earth's surface in lat/lon decimal degrees.
 * <p>
 * Cannot be serialized by WKT directly but used as a part of polygon
 */
public class LinearRing extends Line {
    public static final LinearRing EMPTY = new LinearRing();

    private LinearRing() {
    }

    public LinearRing(double[] lats, double[] lons) {
        super(lats, lons);
        if (lats.length < 2) {
            throw new IllegalArgumentException("linear ring cannot contain less than 2 points, found " + lats.length);
        }
        if (lats[0] != lats[lats.length - 1] || lons[0] != lons[lons.length - 1]) {
            throw new IllegalArgumentException("first and last points of the linear ring must be the same (it must close itself): lats[0]="
                + lats[0] + " lats[" + (lats.length - 1) + "]=" + lats[lats.length - 1]);
        }
    }

    @Override
    public ShapeType type() {
        return ShapeType.LINEARRING;
    }

    @Override
    public <T> T visit(GeometryVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
