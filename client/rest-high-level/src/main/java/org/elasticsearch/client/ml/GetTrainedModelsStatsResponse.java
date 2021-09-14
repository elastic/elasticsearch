/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.inference.TrainedModelStats;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GetTrainedModelsStatsResponse {

    public static final ParseField TRAINED_MODEL_STATS = new ParseField("trained_model_stats");
    public static final ParseField COUNT = new ParseField("count");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetTrainedModelsStatsResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "get_trained_model_stats",
            true,
            args -> new GetTrainedModelsStatsResponse((List<TrainedModelStats>) args[0], (Long) args[1]));

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> TrainedModelStats.fromXContent(p), TRAINED_MODEL_STATS);
        PARSER.declareLong(constructorArg(), COUNT);
    }

    public static GetTrainedModelsStatsResponse fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final List<TrainedModelStats> trainedModelStats;
    private final Long count;


    public GetTrainedModelsStatsResponse(List<TrainedModelStats> trainedModelStats, Long count) {
        this.trainedModelStats = trainedModelStats;
        this.count = count;
    }

    public List<TrainedModelStats> getTrainedModelStats() {
        return trainedModelStats;
    }

    /**
     * @return The total count of the trained models that matched the ID pattern.
     */
    public Long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GetTrainedModelsStatsResponse other = (GetTrainedModelsStatsResponse) o;
        return Objects.equals(this.trainedModelStats, other.trainedModelStats) && Objects.equals(this.count, other.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trainedModelStats, count);
    }
}
