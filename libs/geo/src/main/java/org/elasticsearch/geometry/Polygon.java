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

package org.elasticsearch.geometry;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a closed polygon on the earth's surface with optional holes
 */
public final class Polygon implements Geometry {
    public static final Polygon EMPTY = new Polygon();
    private final LinearRing polygon;
    private final List<LinearRing> holes;
    private final boolean hasAlt;

    private Polygon() {
        polygon = LinearRing.EMPTY;
        holes = Collections.emptyList();
        hasAlt = false;
    }

    /**
     * Creates a new Polygon from the supplied latitude/longitude array, and optionally any holes.
     */
    public Polygon(LinearRing polygon, List<LinearRing> holes) {
        this.polygon = polygon;
        this.holes = holes;
        if (holes == null) {
            throw new IllegalArgumentException("holes must not be null");
        }
        boolean hasAlt = polygon.hasZ();
        checkRing(polygon);
        for (LinearRing hole : holes) {
            if (hole.hasZ() != hasAlt) {
                throw new IllegalArgumentException("holes must have the same number of dimensions as the polygon");
            }
            checkRing(hole);
        }
        this.hasAlt = hasAlt;
    }

    /**
     * Creates a new Polygon from the supplied latitude/longitude array, and optionally any holes.
     */
    public Polygon(LinearRing polygon) {
        this(polygon, Collections.emptyList());
    }

    @Override
    public ShapeType type() {
        return ShapeType.POLYGON;
    }

    private void checkRing(LinearRing ring) {
        if (ring.length() < 4) {
            throw new IllegalArgumentException("at least 4 polygon points required");
        }
    }

    public int getNumberOfHoles() {
        return holes.size();
    }

    public LinearRing getPolygon() {
        return polygon;
    }

    public LinearRing getHole(int i) {
        if (i >= holes.size()) {
            throw new IllegalArgumentException("Index " + i + " is outside the bounds of the " + holes.size() + " polygon holes");
        }
        return holes.get(i);
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

    @Override
    public boolean isEmpty() {
        return polygon.isEmpty();
    }

    @Override
    public boolean hasZ() {
        return hasAlt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("polygon=").append(polygon);
        if (holes.size() > 0) {
            sb.append(", holes=");
            sb.append(holes);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Polygon polygon1 = (Polygon) o;
        return Objects.equals(polygon, polygon1.polygon) &&
            Objects.equals(holes, polygon1.holes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(polygon, holes);
    }
}
