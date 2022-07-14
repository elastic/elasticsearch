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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record WriteLoadHistogramSnapshot(
    long timestamp,
    HistogramSnapshot indexLoadHistogramSnapshot,
    HistogramSnapshot mergeLoadHistogramSnapshot,
    HistogramSnapshot refreshLoadHistogramSnapshot
) implements Writeable, ToXContentFragment {

    private static final ParseField TIMESTAMP_FIELD = new ParseField("@timestamp");
    private static final ParseField INDEXING_LOAD_DISTRIBUTION_FIELD = new ParseField("index_load_histogram");
    private static final ParseField MERGING_LOAD_DISTRIBUTION_FIELD = new ParseField("merge_load_histogram");
    private static final ParseField REFRESH_LOAD_DISTRIBUTION_FIELD = new ParseField("refresh_load_histogram");

    static <T> void configureParser(ConstructingObjectParser<T, Void> parser) {
        parser.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP_FIELD);
        parser.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> HistogramSnapshot.fromXContent(p),
            INDEXING_LOAD_DISTRIBUTION_FIELD
        );
        parser.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> HistogramSnapshot.fromXContent(p),
            MERGING_LOAD_DISTRIBUTION_FIELD
        );
        parser.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> HistogramSnapshot.fromXContent(p),
            REFRESH_LOAD_DISTRIBUTION_FIELD
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TIMESTAMP_FIELD.getPreferredName(), timestamp);
        builder.field(INDEXING_LOAD_DISTRIBUTION_FIELD.getPreferredName(), indexLoadHistogramSnapshot);
        builder.field(MERGING_LOAD_DISTRIBUTION_FIELD.getPreferredName(), mergeLoadHistogramSnapshot);
        builder.field(REFRESH_LOAD_DISTRIBUTION_FIELD.getPreferredName(), refreshLoadHistogramSnapshot);
        return builder;
    }

    public WriteLoadHistogramSnapshot(StreamInput in) throws IOException {
        this(in.readLong(), new HistogramSnapshot(in), new HistogramSnapshot(in), new HistogramSnapshot(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        indexLoadHistogramSnapshot.writeTo(out);
        mergeLoadHistogramSnapshot.writeTo(out);
        refreshLoadHistogramSnapshot.writeTo(out);
    }
}
