/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.regression;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class TimingStats implements Writeable, ToXContentObject {

    public static final ParseField ELAPSED_TIME = new ParseField("elapsed_time");
    public static final ParseField ITERATION_TIME = new ParseField("iteration_time");

    public static TimingStats fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return createParser(ignoreUnknownFields).apply(parser, null);
    }

    private static ConstructingObjectParser<TimingStats, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TimingStats, Void> parser = new ConstructingObjectParser<>("regression_timing_stats", ignoreUnknownFields,
            a -> new TimingStats(TimeValue.timeValueMillis((long) a[0]), TimeValue.timeValueMillis((long) a[1])));

        parser.declareLong(ConstructingObjectParser.constructorArg(), ELAPSED_TIME);
        parser.declareLong(ConstructingObjectParser.constructorArg(), ITERATION_TIME);
        return parser;
    }

    private final TimeValue elapsedTime;
    private final TimeValue iterationTime;

    public TimingStats(TimeValue elapsedTime, TimeValue iterationTime) {
        this.elapsedTime = Objects.requireNonNull(elapsedTime);
        this.iterationTime = Objects.requireNonNull(iterationTime);
    }

    public TimingStats(StreamInput in) throws IOException {
        this.elapsedTime = in.readTimeValue();
        this.iterationTime = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeTimeValue(elapsedTime);
        out.writeTimeValue(iterationTime);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.humanReadableField(ELAPSED_TIME.getPreferredName(), ELAPSED_TIME.getPreferredName() + "_string", elapsedTime);
        builder.humanReadableField(ITERATION_TIME.getPreferredName(), ITERATION_TIME.getPreferredName() + "_string", iterationTime);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimingStats that = (TimingStats) o;
        return Objects.equals(elapsedTime, that.elapsedTime) && Objects.equals(iterationTime, that.iterationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elapsedTime, iterationTime);
    }
}
