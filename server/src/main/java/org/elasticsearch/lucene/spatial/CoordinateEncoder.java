/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.spatial;

/**
 * Interface for classes that help encode double-valued spatial coordinates x/y to
 * their integer-encoded serialized form and decode them back
 */
public interface CoordinateEncoder {

    CoordinateEncoder GEO = new GeoShapeCoordinateEncoder();
    CoordinateEncoder CARTESIAN = new CartesianShapeCoordinateEncoder();

    /** encode X value */
    int encodeX(double x);

    /** encode Y value */
    int encodeY(double y);

    /** decode X value */
    double decodeX(int x);

    /** decode Y value */
    double decodeY(int y);

    /** normalize X value */
    double normalizeX(double x);

    /** normalize Y value */
    double normalizeY(double y);
}
