/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.Aggregation;

/**
 * An aggregation that computes a bounding box in which all documents of the current bucket are.
 */
public interface GeoBounds extends Aggregation {

    /**
     * Get the top-left location of the bounding box.
     */
    GeoPoint topLeft();

    /**
     * Get the bottom-right location of the bounding box.
     */
    GeoPoint bottomRight();
}
