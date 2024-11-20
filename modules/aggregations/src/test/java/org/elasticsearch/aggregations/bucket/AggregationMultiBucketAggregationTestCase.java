/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket;

import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

/**
 * Base class for unit testing multi bucket aggregation's bucket implementations that reside in aggregations module.
 *
 * @param <T> The bucket type
 */
public abstract class AggregationMultiBucketAggregationTestCase<T extends InternalAggregation & MultiBucketsAggregation> extends
    InternalMultiBucketAggregationTestCase<T> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AggregationsPlugin();
    }

}
