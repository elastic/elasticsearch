/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry.utils;

import org.elasticsearch.geometry.Geometry;

/**
 * Generic geometry validator that can be used by the parser to verify the validity of the parsed geometry
 */
public interface GeometryValidator {

    GeometryValidator NOOP = (geometry) -> {};

    /**
     * Validates the geometry and throws IllegalArgumentException if the geometry is not valid
     */
    void validate(Geometry geometry);

    /**
     * Validates a single coordinate and throws IllegalArgumentException if it is not valid.
     * Default implementation is a no-op.
     */
    default void validateCoordinate(double x, double y, double z) {}

    /**
     * Validates the ordinate ordering of a rectangle/envelope, throwing IllegalArgumentException if the
     * envelope is invalid. The default implementation only checks that maxY is not less than minY, since that
     * is always invalid, regardless of CRS. It deliberately does not check x-ordinate ordering: geographic
     * coordinates allow minX to legitimately exceed maxX, to represent an envelope that crosses the
     * antimeridian, so only implementations for CRSes without that concept (e.g. {@link CartesianValidator})
     * need to additionally check that maxX is not less than minX.
     */
    default void validateBBox(double minX, double maxX, double maxY, double minY) {
        if (maxY < minY) {
            throw new IllegalArgumentException("max y cannot be less than min y");
        }
    }

}
