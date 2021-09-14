/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry;

/**
 * Base class for all Geometry objects supported by elasticsearch
 */
public interface Geometry {

    ShapeType type();

    <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E;

    boolean isEmpty();

    default boolean hasZ() {
        return false;
    }

    default boolean hasAlt() {
        return hasZ();
    }
}
