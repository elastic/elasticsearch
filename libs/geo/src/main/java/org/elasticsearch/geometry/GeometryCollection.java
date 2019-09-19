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

import org.elasticsearch.geometry.utils.WellKnownText;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Collection of arbitrary geometry classes
 */
public class GeometryCollection<G extends Geometry> implements Geometry, Iterable<G> {
    public static final GeometryCollection<Geometry> EMPTY = new GeometryCollection<>();

    private final List<G> shapes;

    private boolean hasAlt;

    public GeometryCollection() {
        shapes = Collections.emptyList();
    }

    public GeometryCollection(List<G> shapes) {
        if (shapes == null || shapes.isEmpty()) {
            throw new IllegalArgumentException("the list of shapes cannot be null or empty");
        }
        hasAlt = shapes.get(0).hasZ();
        for (G shape : shapes) {
            if (shape.hasZ() != hasAlt) {
                throw new IllegalArgumentException("all elements of the collection should have the same number of dimension");
            }
        }
        this.shapes = shapes;
    }

    @Override
    public ShapeType type() {
        return ShapeType.GEOMETRYCOLLECTION;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

    @Override
    public boolean isEmpty() {
        return shapes.isEmpty();
    }

    public int size() {
        return shapes.size();
    }

    public G get(int i) {
        return shapes.get(i);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeometryCollection<?> that = (GeometryCollection<?>) o;
        return Objects.equals(shapes, that.shapes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shapes);
    }

    @Override
    public Iterator<G> iterator() {
        return shapes.iterator();
    }

    @Override
    public boolean hasZ() {
        return hasAlt;
    }

    @Override
    public String toString() {
        return WellKnownText.INSTANCE.toWKT(this);
    }
}
