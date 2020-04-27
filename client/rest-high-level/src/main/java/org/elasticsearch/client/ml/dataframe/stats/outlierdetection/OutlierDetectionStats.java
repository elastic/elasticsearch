/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.client.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class OutlierDetectionStats implements AnalysisStats {

    public static final ParseField NAME = new ParseField("outlier_detection_stats");

    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField PARAMETERS = new ParseField("parameters");
    public static final ParseField TIMING_STATS = new ParseField("timing_stats");

    public static final ConstructingObjectParser<OutlierDetectionStats, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(), true,
        a -> new OutlierDetectionStats((Instant) a[0], (Parameters) a[1], (TimingStats) a[2]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), Parameters.PARSER, PARAMETERS);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), TimingStats.PARSER, TIMING_STATS);
    }

    private final Instant timestamp;
    private final Parameters parameters;
    private final TimingStats timingStats;

    public OutlierDetectionStats(Instant timestamp, Parameters parameters, TimingStats timingStats) {
        this.timestamp = Instant.ofEpochMilli(Objects.requireNonNull(timestamp).toEpochMilli());
        this.parameters = Objects.requireNonNull(parameters);
        this.timingStats = Objects.requireNonNull(timingStats);
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Parameters getParameters() {
        return parameters;
    }

    public TimingStats getTimingStats() {
        return timingStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.timeField(TIMESTAMP.getPreferredName(), TIMESTAMP.getPreferredName() + "_string", timestamp.toEpochMilli());
        builder.field(PARAMETERS.getPreferredName(), parameters);
        builder.field(TIMING_STATS.getPreferredName(), timingStats);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutlierDetectionStats that = (OutlierDetectionStats) o;
        return Objects.equals(timestamp, that.timestamp)
            && Objects.equals(parameters, that.parameters)
            && Objects.equals(timingStats, that.timingStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, parameters, timingStats);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }
}
