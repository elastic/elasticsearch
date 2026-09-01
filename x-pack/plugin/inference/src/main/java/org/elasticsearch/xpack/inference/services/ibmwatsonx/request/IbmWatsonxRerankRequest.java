/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.external.request.OutboundRerankRequest;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankTaskSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class IbmWatsonxRerankRequest implements OutboundRerankRequest {

    private final String query;
    private final List<String> input;
    private final IbmWatsonxRerankTaskSettings taskSettings;
    private final IbmWatsonxRerankModel model;

    public IbmWatsonxRerankRequest(String query, List<String> input, IbmWatsonxRerankModel model) {
        Objects.requireNonNull(model);

        this.input = Objects.requireNonNull(input);
        this.query = Objects.requireNonNull(query);
        taskSettings = model.getTaskSettings();
        this.model = model;
    }

    @Override
    public void createHttpRequest(ActionListener<HttpRequest> listener) {
        URI uri;

        try {
            uri = new URI(model.uri().toString());
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("cannot parse URI patter");
        }

        SimpleHttpRequest httpPost = SimpleRequestBuilder.post(uri).build();

        httpPost.setBody(
            Strings.toString(
                new IbmWatsonxRerankRequestEntity(
                    query,
                    input,
                    taskSettings,
                    model.getServiceSettings().modelId(),
                    model.getServiceSettings().projectId()
                )
            ).getBytes(StandardCharsets.UTF_8),
            ContentType.APPLICATION_JSON
        );

        model.authHeaderDecorator().accept(httpPost, model);

        listener.onResponse(new HttpRequest(httpPost, getInferenceEntityId()));
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
    public OutboundRequest truncate() {
        return this;
    }

    public String getQuery() {
        return query;
    }

    public List<String> getInput() {
        return input;
    }

    public IbmWatsonxRerankModel getModel() {
        return model;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

}
