/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        return WellKnownText.toWKT(this);
    }
}
