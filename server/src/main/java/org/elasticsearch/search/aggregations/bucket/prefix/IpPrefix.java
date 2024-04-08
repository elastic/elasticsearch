/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.prefix;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A {@code ip prefix} aggregation. Defines multiple buckets, each representing a subnet.
 */
public interface IpPrefix extends MultiBucketsAggregation {

    /**
     * A bucket in the aggregation where documents fall in
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {

    }

    /**
     * @return  The buckets of this aggregation (each bucket representing a subnet)
     */
    @Override
    List<? extends IpPrefix.Bucket> getBuckets();
}
