/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class InternalMapReduceAggregation extends InternalAggregation {

    private final MapReducer mapReducer;

    InternalMapReduceAggregation(String name, Map<String, Object> metadata, MapReducer mapReducer) {
        super(name, metadata);
        this.mapReducer = mapReducer;
    }

    public InternalMapReduceAggregation(StreamInput in) throws IOException {
        super(in);

        // TODO: handle error if named writable does not exist
        this.mapReducer = in.readNamedWriteable(MapReducer.class);
    }

    @Override
    public String getWriteableName() {
        return mapReducer.getAggregationWritableName();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(mapReducer);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {

        if (reduceContext.isFinalReduce()) {
            mapReducer.reduceInit();
            mapReducer.reduce(aggregations.stream().map(agg -> ((InternalMapReduceAggregation) agg).mapReducer));
            mapReducer.reduceFinalize();
        } else {
            mapReducer.combine(aggregations.stream().map(agg -> ((InternalMapReduceAggregation) agg).mapReducer));
        }

        // TODO: implement combiner

        return this;
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return mapReducer.mustReduceOnSingleInternalAgg();
    }

    @Override
    public Object getProperty(List<String> path) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return mapReducer.toXContent(builder, params);
    }

}
