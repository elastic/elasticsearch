/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class TimingStats implements ToXContentObject {

    public static final ParseField ELAPSED_TIME = new ParseField("elapsed_time");

    public static ConstructingObjectParser<TimingStats, Void> PARSER = new ConstructingObjectParser<>("outlier_detection_timing_stats",
        true,
        a -> new TimingStats(a[0] == null ? null : TimeValue.timeValueMillis((long) a[0])));

    static {
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), ELAPSED_TIME);
    }

    private final TimeValue elapsedTime;

    public TimingStats(TimeValue elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public TimeValue getElapsedTime() {
        return elapsedTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (elapsedTime != null) {
            builder.humanReadableField(ELAPSED_TIME.getPreferredName(), ELAPSED_TIME.getPreferredName() + "_string", elapsedTime);
        }
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
