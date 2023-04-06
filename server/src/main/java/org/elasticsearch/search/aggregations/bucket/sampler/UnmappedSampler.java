/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.sampler;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class UnmappedSampler extends InternalSampler {
    public static final String NAME = "unmapped_sampler";

    UnmappedSampler(String name, Map<String, Object> metadata) {
        super(name, 0, InternalAggregations.EMPTY, metadata);
    }

    /**
     * Read from a stream.
     */
    public UnmappedSampler(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        return new UnmappedSampler(name, metadata);
    }

    @Override
    public boolean canLeadReduction() {
        return false;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Aggregation.CommonFields.DOC_COUNT.getPreferredName(), 0);
        return builder;
    }

}
