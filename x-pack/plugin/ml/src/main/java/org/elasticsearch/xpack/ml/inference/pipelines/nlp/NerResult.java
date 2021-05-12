/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class NerResult implements InferenceResults {

    public String NAME = "ner_result";

    private final List<EntityGroup> entityGroups;

    public NerResult(List<EntityGroup> entityGroups) {
        this.entityGroups = Objects.requireNonNull(entityGroups);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (EntityGroup entity : entityGroups) {
            entity.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entityGroups);
    }

    @Override
    public Map<String, Object> asMap() {
        // TODO required for Ingest Pipelines
        return null;
    }

    @Override
    public Object predictedValue() {
        // TODO required for Ingest Pipelines
        return null;
    }

    List<EntityGroup> getEntityGroups() {
        return entityGroups;
    }
}
