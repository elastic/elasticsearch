/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;


public class PutTrainedModelRequest implements Validatable, ToXContentObject {

    private final TrainedModelConfig config;

    public PutTrainedModelRequest(TrainedModelConfig config) {
        this.config = config;
    }

    public TrainedModelConfig getTrainedModelConfig() {
        return config;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return config.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutTrainedModelRequest request = (PutTrainedModelRequest) o;
        return Objects.equals(config, request.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }

    @Override
    public final String toString() {
        return Strings.toString(config);
    }
}
