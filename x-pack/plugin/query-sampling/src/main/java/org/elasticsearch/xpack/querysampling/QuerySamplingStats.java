/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Where the searches seen by one node ended up, stage by stage. Every number only counts what happened on
 * this node since it started.
 *
 * @param knnSearches        eligible kNN searches the capture gate looked at
 * @param captured           of those, searches the gate picked
 * @param dropped            captures lost because the hand-off queue was full
 * @param distinctQueries    distinct queries being counted
 * @param untrackedArrivals  arrivals of queries that could not be counted because the counter was full
 * @param picked             distinct queries picked for the sample
 * @param buffered           picked queries currently held in Tier 1
 * @param rejected           picked queries turned away because Tier 1 was full
 */
public record QuerySamplingStats(
    long knnSearches,
    long captured,
    long dropped,
    long distinctQueries,
    long untrackedArrivals,
    long picked,
    long buffered,
    long rejected
) implements Writeable, ToXContentFragment {

    public QuerySamplingStats(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(knnSearches);
        out.writeVLong(captured);
        out.writeVLong(dropped);
        out.writeVLong(distinctQueries);
        out.writeVLong(untrackedArrivals);
        out.writeVLong(picked);
        out.writeVLong(buffered);
        out.writeVLong(rejected);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("knn_searches", knnSearches);
        builder.field("captured", captured);
        builder.field("dropped", dropped);
        builder.field("distinct_queries", distinctQueries);
        builder.field("untracked_arrivals", untrackedArrivals);
        builder.field("picked", picked);
        builder.field("buffered", buffered);
        builder.field("rejected", rejected);
        return builder;
    }
}
