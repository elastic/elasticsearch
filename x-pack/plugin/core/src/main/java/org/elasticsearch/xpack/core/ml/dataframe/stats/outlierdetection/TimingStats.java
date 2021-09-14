/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class TimingStats implements Writeable, ToXContentObject {

    public static final ParseField ELAPSED_TIME = new ParseField("elapsed_time");

    public static TimingStats fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return createParser(ignoreUnknownFields).apply(parser, null);
    }

    private static ConstructingObjectParser<TimingStats, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TimingStats, Void> parser = new ConstructingObjectParser<>("outlier_detection_timing_stats",
            ignoreUnknownFields,
            a -> new TimingStats(TimeValue.timeValueMillis((long) a[0])));

        parser.declareLong(ConstructingObjectParser.constructorArg(), ELAPSED_TIME);
        return parser;
    }

    private final TimeValue elapsedTime;

    public TimingStats(TimeValue elapsedTime) {
        this.elapsedTime = Objects.requireNonNull(elapsedTime);
    }

    public TimingStats(StreamInput in) throws IOException {
        this.elapsedTime = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeTimeValue(elapsedTime);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.humanReadableField(ELAPSED_TIME.getPreferredName(), ELAPSED_TIME.getPreferredName() + "_string", elapsedTime);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimingStats that = (TimingStats) o;
        return Objects.equals(elapsedTime, that.elapsedTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elapsedTime);
    }
}
