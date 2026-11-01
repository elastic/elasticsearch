/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.DecisionQuestions;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.common.InferencePreferences;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.decision.ElasticInferenceServiceDecisionModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContextHandler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ElasticInferenceServiceDecisionRequest extends ElasticInferenceServiceRequest {

    private final InferenceString input;
    private final DecisionQuestions questions;
    private final ElasticInferenceServiceDecisionModel model;
    private final TraceContextHandler traceContextHandler;

    public ElasticInferenceServiceDecisionRequest(
        InferenceString input,
        DecisionQuestions questions,
        ElasticInferenceServiceDecisionModel model,
        TraceContext traceContext,
        ElasticInferenceServiceRequestMetadata metadata,
        InferencePreferences preferences,
        CCMAuthenticationApplierFactory.AuthApplier authApplier
    ) {
        super(metadata, preferences, authApplier);
        this.input = Objects.requireNonNull(input);
        this.questions = Objects.requireNonNull(questions);
        this.model = Objects.requireNonNull(model);
        this.traceContextHandler = new TraceContextHandler(traceContext);
    }

    @Override
    public SimpleHttpRequest createSimpleHttpRequest() {
        var httpPost = SimpleRequestBuilder.post(getURI()).build();
        var requestEntity = Strings.toString(
            new ElasticInferenceServiceDecisionRequestEntity(input, questions, model.getServiceSettings().modelId())
        );

        httpPost.setBody(requestEntity.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);

        traceContextHandler.propagateTraceContext(httpPost);

        return httpPost;
    }

    public TraceContext getTraceContext() {
        return traceContextHandler.traceContext();
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return model.uri();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.DECISION;
    }

    @Override
    public OutboundRequest truncate() {
        // no truncation
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // no truncation
        return null;
    }
}
