/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry;

import java.util.Locale;

/**
 * Shape types supported by elasticsearch
 */
public enum ShapeType {
    POINT,
    MULTIPOINT,
    LINESTRING,
    MULTILINESTRING,
    POLYGON,
    MULTIPOLYGON,
    GEOMETRYCOLLECTION,
    LINEARRING, // not serialized by itself in WKT or WKB
    ENVELOPE, // not part of the actual WKB spec
    CIRCLE; // not part of the actual WKB spec

    public static ShapeType forName(String shapeName) {
        return ShapeType.valueOf(shapeName.toUpperCase(Locale.ROOT));
    }
}
