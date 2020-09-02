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

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.inference.trainedmodel.metadata.TrainedModelMetadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GetTrainedModelsMetadataResponse {

    public static final ParseField TRAINED_MODELS_METADATA = new ParseField("trained_models_metadata");
    public static final ParseField COUNT = new ParseField("count");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetTrainedModelsMetadataResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "get_trained_models_metadata",
            true,
            args -> new GetTrainedModelsMetadataResponse((List<TrainedModelMetadata>) args[0], (Long) args[1]));

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> TrainedModelMetadata.fromXContent(p), TRAINED_MODELS_METADATA);
        PARSER.declareLong(constructorArg(), COUNT);
    }

    public static GetTrainedModelsMetadataResponse fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final List<TrainedModelMetadata> trainedModelsMetadata;
    private final Long count;


    public GetTrainedModelsMetadataResponse(List<TrainedModelMetadata> trainedModelsMetadata, Long count) {
        this.trainedModelsMetadata = trainedModelsMetadata;
        this.count = count;
    }

    public List<TrainedModelMetadata> getTrainedModelsMetadata() {
        return trainedModelsMetadata;
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

        GetTrainedModelsMetadataResponse other = (GetTrainedModelsMetadataResponse) o;
        return Objects.equals(this.trainedModelsMetadata, other.trainedModelsMetadata) && Objects.equals(this.count, other.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trainedModelsMetadata, count);
    }
}
