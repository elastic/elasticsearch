/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.global;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;

import java.io.IOException;
import java.util.Map;

/**
 * A global scope get (the document set on which we aggregate is all documents in the search context (ie. index + type)
 * regardless the query.
 */
public class InternalGlobal extends InternalSingleBucketAggregation implements SingleBucketAggregation {
    InternalGlobal(String name, long docCount, InternalAggregations aggregations, Map<String, Object> metadata) {
        super(name, docCount, aggregations, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalGlobal(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return GlobalAggregationBuilder.NAME;
    }

    @Override
    protected InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations) {
        return new InternalGlobal(name, docCount, subAggregations, getMetadata());
    }
}
