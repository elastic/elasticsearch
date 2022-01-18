/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;

/**
 * Accumulation of the most relevant hits for a bucket this aggregation falls into.
 */
public interface TopHits extends Aggregation {

    /**
     * @return The top matching hits for the bucket
     */
    SearchHits getHits();

}
