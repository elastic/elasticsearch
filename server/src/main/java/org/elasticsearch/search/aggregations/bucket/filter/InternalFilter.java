/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;

import java.io.IOException;
import java.util.Map;

public class InternalFilter extends InternalSingleBucketAggregation implements Filter {
    InternalFilter(String name, long docCount, InternalAggregations subAggregations, Map<String, Object> metadata) {
        super(name, docCount, subAggregations, metadata);
    }

    /**
     * Stream from a stream.
     */
    public InternalFilter(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return FilterAggregationBuilder.NAME;
    }

    @Override
    protected InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations) {
        return new InternalFilter(name, docCount, subAggregations, getMetadata());
    }
}
