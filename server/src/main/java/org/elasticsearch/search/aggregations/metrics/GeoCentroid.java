/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.xcontent.ToXContentFragment;

/**
 * Interface for {@link GeoCentroidAggregator}
 * TODO: since this is generic to both geo and cartesian, we should rename the class
 */
public interface GeoCentroid<T extends ToXContentFragment> extends Aggregation {
    T centroid();

    long count();
}
