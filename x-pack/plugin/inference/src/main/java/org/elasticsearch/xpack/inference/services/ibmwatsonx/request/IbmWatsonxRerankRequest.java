/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankTaskSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class IbmWatsonxRerankRequest implements IbmWatsonxRequest {

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
    public HttpRequest createHttpRequest() {
        URI uri;

        try {
            uri = new URI(model.uri().toString());
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("cannot parse URI patter");
        }

        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(
                new IbmWatsonxRerankRequestEntity(
                    query,
                    input,
                    taskSettings,
                    model.getServiceSettings().modelId(),
                    model.getServiceSettings().projectId()
                )
            ).getBytes(StandardCharsets.UTF_8)
        );

        httpPost.setEntity(byteEntity);
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        decorateWithAuth(httpPost);

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    public void decorateWithAuth(HttpPost httpPost) {
        IbmWatsonxRequest.decorateWithBearerToken(httpPost, model.getSecretSettings(), model.getInferenceEntityId());
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
    public Request truncate() {
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
