/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public record HistogramSnapshot(double average, double p50, double p75, double p90, double p95, double p99, double max)
    implements
        ToXContentObject,
        Writeable {

    private static final ParseField AVG_FIELD = new ParseField("average");
    private static final ParseField P50_FIELD = new ParseField("50");
    private static final ParseField P75_FIELD = new ParseField("75");
    private static final ParseField P90_FIELD = new ParseField("90");
    private static final ParseField P95_FIELD = new ParseField("95");
    private static final ParseField P99_FIELD = new ParseField("99");
    private static final ParseField MAX_FIELD = new ParseField("max");

    public static final ConstructingObjectParser<HistogramSnapshot, Void> PARSER = new ConstructingObjectParser<>(
        "histogram_snapshot",
        false,
        (args, unused) -> new HistogramSnapshot(
            (double) args[0],
            (double) args[1],
            (double) args[2],
            (double) args[3],
            (double) args[4],
            (double) args[5],
            (double) args[6]
        )
    );

    static {
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), AVG_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), P50_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), P75_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), P90_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), P95_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), P99_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MAX_FIELD);
    }

    public static HistogramSnapshot fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(AVG_FIELD.getPreferredName(), average);
        builder.field(P50_FIELD.getPreferredName(), p50);
        builder.field(P75_FIELD.getPreferredName(), p75);
        builder.field(P90_FIELD.getPreferredName(), p90);
        builder.field(P95_FIELD.getPreferredName(), p95);
        builder.field(P99_FIELD.getPreferredName(), p99);
        builder.field(MAX_FIELD.getPreferredName(), max);
        builder.endObject();
        return builder;
    }

    HistogramSnapshot(StreamInput in) throws IOException {
        this(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(average);
        out.writeDouble(p50);
        out.writeDouble(p75);
        out.writeDouble(p90);
        out.writeDouble(p95);
        out.writeDouble(p99);
        out.writeDouble(max);
    }

    public static HistogramSnapshot takeSnapshot(IndicesWriteLoadStatsCollector.Histogram histogram) {
        return new HistogramSnapshot(
            histogram.getAverage(),
            histogram.getValueAtPercentile(50),
            histogram.getValueAtPercentile(75),
            histogram.getValueAtPercentile(90),
            histogram.getValueAtPercentile(95),
            histogram.getValueAtPercentile(99),
            histogram.getMaxValue()
        );
    }
}
