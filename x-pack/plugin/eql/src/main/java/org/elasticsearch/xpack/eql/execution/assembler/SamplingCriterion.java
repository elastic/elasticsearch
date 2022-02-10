/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;

import java.util.List;

public class SamplingCriterion<Q extends QueryRequest> {

    private final List<BucketExtractor> keys;
    // the first query to run (sampling filter + composite aggs on keys)
    private final Q firstQuery;
    // intermediate queries (sampling filter + composite aggs on keys + keys values filtering)
    private final Q midQuery;
    // final stage query (sampling filter + a single set of keys values filter)
    private final Q finalQuery;

    private final int keySize;

    public SamplingCriterion(Q firstQuery, Q midQuery, Q finalQuery, List<BucketExtractor> keys) {
        this.firstQuery = firstQuery;
        this.midQuery = midQuery;
        this.finalQuery = finalQuery;
        this.keys = keys;
        this.keySize = keys.size();
    }

    public Q firstQuery() {
        return firstQuery;
    }

    public Q midQuery() {
        return midQuery;
    }

    public Q finalQuery() {
        return finalQuery;
    }

    public Object[] key(Bucket bucket) {
        Object[] key = null;
        if (keySize > 0) {
            Object[] docKeys = new Object[keySize];
            for (int i = 0; i < keySize; i++) {
                docKeys[i] = keys.get(i).extract(bucket);
            }
            key = docKeys;
        }
        return key;
    }

    @Override
    public String toString() {
        return keys.toString();
    }
}
