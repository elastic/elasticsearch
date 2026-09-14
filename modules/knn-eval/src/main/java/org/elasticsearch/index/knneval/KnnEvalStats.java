/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;

/**
 * Distribution of one per-query or per-search measurement: recall, took, vector operations. The mean alone hides the shape --
 * 0.9 everywhere is a very different proposition from 1.0 on most queries and 0.2 on a tail.
 *
 * @param sem   standard error of the mean, for the normal-approximation CI {@code mean +/- 1.96 * sem} a plot needs; the
 *              percentiles describe spread, not uncertainty
 * @param count failed queries are not counted
 */
public record KnnEvalStats(
    double mean,
    double stddev,
    double sem,
    double min,
    double p10,
    double p50,
    double p90,
    double p95,
    double max,
    long count
) implements Writeable, ToXContentObject {

    static final ParseField MEAN_FIELD = new ParseField("mean");
    static final ParseField STDDEV_FIELD = new ParseField("stddev");
    static final ParseField SEM_FIELD = new ParseField("sem");
    static final ParseField MIN_FIELD = new ParseField("min");
    static final ParseField P10_FIELD = new ParseField("p10");
    static final ParseField P50_FIELD = new ParseField("p50");
    static final ParseField P90_FIELD = new ParseField("p90");
    static final ParseField P95_FIELD = new ParseField("p95");
    static final ParseField MAX_FIELD = new ParseField("max");
    static final ParseField COUNT_FIELD = new ParseField("count");

    public static final KnnEvalStats EMPTY = new KnnEvalStats(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0);

    /** Nearest-rank over a sorted copy: one request's worth of values is too small to need a sketch. */
    public static KnnEvalStats of(Collection<? extends Number> values) {
        if (values.isEmpty()) {
            return EMPTY;
        }
        double[] sorted = values.stream().mapToDouble(Number::doubleValue).sorted().toArray();
        double sum = 0.0;
        for (double value : sorted) {
            sum += value;
        }
        double mean = sum / sorted.length;
        double squaredError = 0.0;
        for (double value : sorted) {
            squaredError += (value - mean) * (value - mean);
        }
        // population, not sample: these are all the queries that were run, not a draw from a larger set of them
        double stddev = sorted.length < 2 ? 0.0 : Math.sqrt(squaredError / sorted.length);
        return new KnnEvalStats(
            mean,
            stddev,
            sorted.length < 2 ? 0.0 : stddev / Math.sqrt(sorted.length),
            sorted[0],
            nearestRank(sorted, 10),
            nearestRank(sorted, 50),
            nearestRank(sorted, 90),
            nearestRank(sorted, 95),
            sorted[sorted.length - 1],
            sorted.length
        );
    }

    private static double nearestRank(double[] sorted, int percentile) {
        int rank = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
        return sorted[Math.min(Math.max(rank, 0), sorted.length - 1)];
    }

    KnnEvalStats(StreamInput in) throws IOException {
        this(
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readVLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(mean);
        out.writeDouble(stddev);
        out.writeDouble(sem);
        out.writeDouble(min);
        out.writeDouble(p10);
        out.writeDouble(p50);
        out.writeDouble(p90);
        out.writeDouble(p95);
        out.writeDouble(max);
        out.writeVLong(count);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MEAN_FIELD.getPreferredName(), mean);
        builder.field(STDDEV_FIELD.getPreferredName(), stddev);
        builder.field(SEM_FIELD.getPreferredName(), sem);
        builder.field(MIN_FIELD.getPreferredName(), min);
        builder.field(P10_FIELD.getPreferredName(), p10);
        builder.field(P50_FIELD.getPreferredName(), p50);
        builder.field(P90_FIELD.getPreferredName(), p90);
        builder.field(P95_FIELD.getPreferredName(), p95);
        builder.field(MAX_FIELD.getPreferredName(), max);
        builder.field(COUNT_FIELD.getPreferredName(), count);
        builder.endObject();
        return builder;
    }
}
