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

public class EntityRiskScoringResponse extends ActionResponse implements ToXContentObject {

    private final RiskScoreResult result;

    public EntityRiskScoringResponse(RiskScoreResult result) {
        this.result = result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(result.toString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return result.toXContent(builder, params);
    }
}
