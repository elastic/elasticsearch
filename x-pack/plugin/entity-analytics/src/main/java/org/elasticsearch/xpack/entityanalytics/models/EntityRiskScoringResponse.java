/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class EntityRiskScoringResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, Double> response;

    public EntityRiskScoringResponse(Map<String, Double> response) { this.response = response;}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(response.toString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Double> entry : response.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }
}
