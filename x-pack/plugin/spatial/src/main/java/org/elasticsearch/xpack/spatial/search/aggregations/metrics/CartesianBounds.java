/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.metrics.SpatialBounds;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

/**
 * An aggregation that computes a bounding box in which all documents of the current bucket are.
 */
public interface CartesianBounds extends SpatialBounds<CartesianPoint> {}
