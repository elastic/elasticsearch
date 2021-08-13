/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.utils;

import org.elasticsearch.geometry.Geometry;

/**
 * Generic geometry validator that can be used by the parser to verify the validity of the parsed geometry
 */
public interface GeometryValidator {

    /**
     * Validates the geometry and throws IllegalArgumentException if the geometry is not valid
     */
    void validate(Geometry geometry);

}
