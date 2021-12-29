/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

public interface IpPrefix extends MultiBucketsAggregation {

    interface Bucket extends MultiBucketsAggregation.Bucket {

    }

    /**
     * Return the buckets of this range aggregation.
     */
    @Override
    List<? extends IpPrefix.Bucket> getBuckets();
}
