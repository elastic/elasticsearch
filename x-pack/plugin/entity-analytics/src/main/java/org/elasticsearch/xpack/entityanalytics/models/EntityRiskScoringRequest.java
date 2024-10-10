/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class EntityRiskScoringRequest extends ActionRequest implements IndicesRequest.Replaceable, ToXContentObject {

    private final IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, false);
    private String[] indices = Strings.EMPTY_ARRAY;
    private String category1Index;
    private EntityType[] entityTypes;

    public EntityRiskScoringRequest(String category1Index, EntityType[] entityTypes) {
        this.category1Index = category1Index;
        this.entityTypes = entityTypes;
    }

    public EntityRiskScoringRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (category1Index == null) {
            validationException = addValidationError("missing category_1_index", validationException);
        }

        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString("test string");
    }

    @Override
    public String toString() {
        return "entity risk scoring";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("entity_risk_scoring_request");
        builder.field("test", "hello world");
        builder.endObject();

        return builder;
    }

    @Override
    public String[] indices() {
        return this.indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public EntityRiskScoringRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    public String getCategory1Index() {
        return category1Index;
    }

    public EntityType[] getEntityTypes() {
        return this.entityTypes;
    }
}
