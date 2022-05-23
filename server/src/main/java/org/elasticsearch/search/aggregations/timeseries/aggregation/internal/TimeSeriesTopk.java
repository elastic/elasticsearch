/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.internal;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TSIDValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimeSeriesTopk extends InternalAggregation {
    public static final String NAME = "time_series_topk";

    private final List<TSIDValue<Double>> tsidValues;
    private final int topkSize;
    private final boolean isTop;
    private final DocValueFormat formatter;

    public TimeSeriesTopk(
        String name,
        List<TSIDValue<Double>> tsidValues,
        int topkSize,
        boolean isTop,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.tsidValues = tsidValues;
        this.topkSize = topkSize;
        this.isTop = isTop;
        this.formatter = formatter;
    }

    public TimeSeriesTopk(StreamInput in) throws IOException {
        super(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        tsidValues = in.readList((input -> new TSIDValue<>(in.readBytesRef(), in.readDouble())));
        topkSize = in.readInt();
        isTop = in.readBoolean();
    }

    public List<TSIDValue<Double>> getTsidValues() {
        return tsidValues;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        out.writeCollection(tsidValues, (output, value) -> {
            out.writeBytesRef(value.tsid);
            out.writeDouble(value.value);
        });
        out.writeInt(topkSize);
        out.writeBoolean(isTop);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        PriorityQueue<TSIDValue<Double>> queue = new PriorityQueue<>(topkSize) {
            @Override
            protected boolean lessThan(TSIDValue<Double> a, TSIDValue<Double> b) {
                if (isTop) {
                    return a.value > b.value;
                } else {
                    return a.value < b.value;
                }
            }
        };

        for (InternalAggregation internalAggregation : aggregations) {
            TimeSeriesTopk timeSeriesTopk = (TimeSeriesTopk) internalAggregation;
            timeSeriesTopk.tsidValues.forEach((value -> queue.insertWithOverflow(value)));
        }

        List<TSIDValue<Double>> values = new ArrayList<>(queue.size());
        for (int b = queue.size() - 1; b >= 0; --b) {
            values.add(queue.pop());
        }
        return new TimeSeriesTopk(name, values, topkSize, isTop, formatter, getMetadata());
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
        builder.field(CommonFields.VALUE.getPreferredName());
        builder.startArray();
        for (TSIDValue<Double> value : tsidValues) {
            builder.startObject();
            builder.field("_tsid", TimeSeriesIdFieldMapper.decodeTsid(value.tsid));
            builder.field(CommonFields.VALUE.getPreferredName(), value.value);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }
}
