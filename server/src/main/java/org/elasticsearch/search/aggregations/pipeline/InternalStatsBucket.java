/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalStats;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalStatsBucket extends InternalStats implements StatsBucket {
    public InternalStatsBucket(
        String name,
        long count,
        double sum,
        double min,
        double max,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, count, sum, min, max, formatter, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalStatsBucket(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return StatsBucketPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalStats reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }
}
