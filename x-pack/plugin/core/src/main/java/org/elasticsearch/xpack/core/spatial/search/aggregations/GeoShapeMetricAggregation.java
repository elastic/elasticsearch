/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.spatial.search.aggregations;

import java.util.Map;

/**
 * This interface provides a way for spatial aggs to easily provide appropriately formatted geoJSON geometry to describe their
 * aggregated results.
 */
public interface GeoShapeMetricAggregation {
    /**
     * Provides the geometry calculated by the aggregation in an indexible format.
     *
     * @return geometry as a geoJSON object
     */
    Map<String, Object> geoJSONGeometry();
}
