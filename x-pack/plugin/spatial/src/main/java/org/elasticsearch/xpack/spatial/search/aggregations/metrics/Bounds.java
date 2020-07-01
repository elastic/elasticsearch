/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

/**
 * An aggregation that computes a bounding box in which all documents of the current bucket are.
 */
public interface Bounds extends Aggregation {

    /**
     * Get the top-left location of the bounding box.
     */
    CartesianPoint topLeft();

    /**
     * Get the bottom-right location of the bounding box.
     */
    CartesianPoint bottomRight();
}
