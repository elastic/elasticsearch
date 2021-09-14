/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;


public class PutTrainedModelResponse implements ToXContentObject {

    private final TrainedModelConfig trainedModelConfig;

    public static PutTrainedModelResponse fromXContent(XContentParser parser) throws IOException {
        return new PutTrainedModelResponse(TrainedModelConfig.PARSER.parse(parser, null).build());
    }

    public PutTrainedModelResponse(TrainedModelConfig trainedModelConfig) {
        this.trainedModelConfig = trainedModelConfig;
    }

    public TrainedModelConfig getResponse() {
        return trainedModelConfig;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return trainedModelConfig.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutTrainedModelResponse response = (PutTrainedModelResponse) o;
        return Objects.equals(trainedModelConfig, response.trainedModelConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trainedModelConfig);
    }
}
