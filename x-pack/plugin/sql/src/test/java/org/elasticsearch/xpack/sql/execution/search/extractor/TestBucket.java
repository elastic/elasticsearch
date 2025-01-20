/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;

import java.util.Map;

class TestBucket implements Bucket {

    private final Map<String, Object> key;
    private final long count;
    private final InternalAggregations aggs;

    TestBucket(Map<String, Object> key, long count, InternalAggregations aggs) {
        this.key = key;
        this.count = count;
        this.aggs = aggs;
    }

    @Override
    public Map<String, Object> getKey() {
        return key;
    }

    @Override
    public String getKeyAsString() {
        return key.toString();
    }

    @Override
    public long getDocCount() {
        return count;
    }

    @Override
    public InternalAggregations getAggregations() {
        return aggs;
    }
}
