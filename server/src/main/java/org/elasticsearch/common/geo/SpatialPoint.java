/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

/**
 * To facilitate maximizing the use of common code between GeoPoint and projected CRS
 * we introduced this ElasticPoint as an interface of commonality.
 */
public interface SpatialPoint {
    double getX();

    double getY();
}
