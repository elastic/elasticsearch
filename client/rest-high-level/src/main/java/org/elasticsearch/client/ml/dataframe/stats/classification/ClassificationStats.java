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
package org.elasticsearch.client.ml.dataframe.stats.classification;

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

public class ClassificationStats implements AnalysisStats {

    public static final ParseField NAME = new ParseField("classification_stats");

    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField ITERATION = new ParseField("iteration");
    public static final ParseField HYPERPARAMETERS = new ParseField("hyperparameters");
    public static final ParseField TIMING_STATS = new ParseField("timing_stats");
    public static final ParseField VALIDATION_LOSS = new ParseField("validation_loss");

    public static final ConstructingObjectParser<ClassificationStats, Void> PARSER = new ConstructingObjectParser<>(NAME.getPreferredName(),
        true,
        a -> new ClassificationStats(
            (Instant) a[0],
            (Integer) a[1],
            (Hyperparameters) a[2],
            (TimingStats) a[3],
            (ValidationLoss) a[4]
        )
    );

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), ITERATION);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), Hyperparameters.PARSER, HYPERPARAMETERS);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), TimingStats.PARSER, TIMING_STATS);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), ValidationLoss.PARSER, VALIDATION_LOSS);
    }

    private final Instant timestamp;
    private final Integer iteration;
    private final Hyperparameters hyperparameters;
    private final TimingStats timingStats;
    private final ValidationLoss validationLoss;

    public ClassificationStats(Instant timestamp, Integer iteration, Hyperparameters hyperparameters, TimingStats timingStats,
                               ValidationLoss validationLoss) {
        this.timestamp = Instant.ofEpochMilli(Objects.requireNonNull(timestamp).toEpochMilli());
        this.iteration = iteration;
        this.hyperparameters = Objects.requireNonNull(hyperparameters);
        this.timingStats = Objects.requireNonNull(timingStats);
        this.validationLoss = Objects.requireNonNull(validationLoss);
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Integer getIteration() {
        return iteration;
    }

    public Hyperparameters getHyperparameters() {
        return hyperparameters;
    }

    public TimingStats getTimingStats() {
        return timingStats;
    }

    public ValidationLoss getValidationLoss() {
        return validationLoss;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.timeField(TIMESTAMP.getPreferredName(), TIMESTAMP.getPreferredName() + "_string", timestamp.toEpochMilli());
        if (iteration != null) {
            builder.field(ITERATION.getPreferredName(), iteration);
        }
        builder.field(HYPERPARAMETERS.getPreferredName(), hyperparameters);
        builder.field(TIMING_STATS.getPreferredName(), timingStats);
        builder.field(VALIDATION_LOSS.getPreferredName(), validationLoss);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClassificationStats that = (ClassificationStats) o;
        return Objects.equals(timestamp, that.timestamp)
            && Objects.equals(iteration, that.iteration)
            && Objects.equals(hyperparameters, that.hyperparameters)
            && Objects.equals(timingStats, that.timingStats)
            && Objects.equals(validationLoss, that.validationLoss);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, iteration, hyperparameters, timingStats, validationLoss);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }
}
