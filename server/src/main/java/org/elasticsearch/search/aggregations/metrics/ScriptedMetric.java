/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregation;

/**
 * A metric aggregation that computes both its final and intermediate states using scripts.
 */
public interface ScriptedMetric extends Aggregation {

    /**
     * The result of the aggregation. The type of the object depends on the aggregation that was run.
     */
    Object aggregation();
}
