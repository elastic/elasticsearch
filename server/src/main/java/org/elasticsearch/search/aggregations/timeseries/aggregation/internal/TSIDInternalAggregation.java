/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.internal;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.timeseries.aggregation.Function;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.TSIDBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The tsid internal aggregation is used to aggregator the tsid values in the coordinate reduce phase.
 * The {@link TSIDBucketFunction} collect the data, and this internal aggregation compute the reduce result.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class TSIDInternalAggregation extends InternalAggregation {
    public static final String NAME = "tsid";

    private final Map<BytesRef, InternalAggregation> values;
    private final String aggreagator;
    private final DocValueFormat formatter;

    public TSIDInternalAggregation(
        String name,
        Map<BytesRef, InternalAggregation> values,
        String aggregator,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.values = values;
        this.formatter = formatter;
        this.aggreagator = aggregator;
    }

    public TSIDInternalAggregation(StreamInput in) throws IOException {
        super(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        aggreagator = in.readString();
        values = in.readOrderedMap(StreamInput::readBytesRef, stream -> stream.readNamedWriteable(InternalAggregation.class));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        out.writeString(aggreagator);
        out.writeMap(values, StreamOutput::writeBytesRef, StreamOutput::writeNamedWriteable);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        if (aggregations.size() == 1) {
            TSIDInternalAggregation tsidAgg = (TSIDInternalAggregation) aggregations.get(0);
            if (reduceContext.isFinalReduce()) {
                Function function = Function.valueOf(aggreagator);
                final AggregatorFunction aggregatorFunction = function.getAggregatorFunction();
                tsidAgg.values.forEach(
                    (tsid, agg) -> { aggregatorFunction.collect(((InternalNumericMetricsAggregation.SingleValue) agg).value()); }
                );
                return aggregatorFunction.getAggregation(formatter, getMetadata());
            } else {
                return tsidAgg;
            }
        }

        Map<BytesRef, List<InternalAggregation>> reduced = new TreeMap<>();
        for (InternalAggregation aggregation : aggregations) {
            TSIDInternalAggregation tsidAgg = (TSIDInternalAggregation) aggregation;
            tsidAgg.values.forEach((tsid, value) -> {
                if (false == reduced.containsKey(tsid)) {
                    reduced.put(tsid, List.of(value));
                } else {
                    reduced.get(tsid).add(value);
                }
            });
        }

        if (reduceContext.isFinalReduce()) {
            Function function = Function.valueOf(aggreagator);
            final AggregatorFunction aggregatorFunction = function.getAggregatorFunction();
            reduced.forEach((tsid, aggs) -> {
                if (aggs.size() > 0) {
                    InternalAggregation first = aggs.get(0);
                    InternalNumericMetricsAggregation.SingleValue internalAggregation = (SingleValue) first.reduce(aggs, reduceContext);
                    aggregatorFunction.collect(internalAggregation.value());
                }
            });
            return aggregatorFunction.getAggregation(formatter, getMetadata());
        } else {
            Map<BytesRef, InternalAggregation> finalReduces = new TreeMap<>();
            reduced.forEach((tsid, aggs) -> {
                if (aggs.size() > 0) {
                    InternalAggregation first = aggs.get(0);
                    finalReduces.put(tsid, first.reduce(aggs, reduceContext));
                }
            });
            return new TSIDInternalAggregation(name, finalReduces, aggreagator, formatter, getMetadata());
        }
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        return null;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), aggreagator);
        return builder;
    }
}
