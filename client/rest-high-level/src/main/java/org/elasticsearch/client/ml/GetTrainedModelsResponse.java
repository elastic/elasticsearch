/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GetTrainedModelsResponse {

    public static final ParseField TRAINED_MODEL_CONFIGS = new ParseField("trained_model_configs");
    public static final ParseField COUNT = new ParseField("count");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetTrainedModelsResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "get_trained_model_configs",
            true,
            args -> new GetTrainedModelsResponse((List<TrainedModelConfig>) args[0], (Long) args[1]));

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> TrainedModelConfig.fromXContent(p), TRAINED_MODEL_CONFIGS);
        PARSER.declareLong(constructorArg(), COUNT);
    }

    public static GetTrainedModelsResponse fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final List<TrainedModelConfig> trainedModels;
    private final Long count;


    public GetTrainedModelsResponse(List<TrainedModelConfig> trainedModels, Long count) {
        this.trainedModels = trainedModels;
        this.count = count;
    }

    public List<TrainedModelConfig> getTrainedModels() {
        return trainedModels;
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

        GetTrainedModelsResponse other = (GetTrainedModelsResponse) o;
        return Objects.equals(this.trainedModels, other.trainedModels) && Objects.equals(this.count, other.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trainedModels, count);
    }
}
