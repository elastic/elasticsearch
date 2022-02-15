/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public abstract class InternalMultiValueAggregation extends InternalAggregation implements MultiValueAggregation {

    protected InternalMultiValueAggregation(String name, Map<String, Object> metadata) {
        super(name, metadata);
    }

    /**
     * Read from a stream.
     */
    protected InternalMultiValueAggregation(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public final double sortValue(AggregationPath.PathElement head, Iterator<AggregationPath.PathElement> tail) {
        throw new IllegalArgumentException("Metrics aggregations cannot have sub-aggregations (at [>" + head + "]");
    }
}
