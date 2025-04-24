/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.ValidatingSubstitutor;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.custom.CustomModel;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.REQUEST_CONTENT;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.URL;

public class CustomRequest implements Request {
    private static final String QUERY = "query";
    private static final String INPUT = "input";

    private final URI uri;
    private final ValidatingSubstitutor substitutor;
    private final CustomModel model;

    public CustomRequest(String query, List<String> input, CustomModel model) {
        this.model = Objects.requireNonNull(model);

        var jsonParams = new HashMap<String, String>();
        addJsonStringParams(jsonParams, model.getSecretSettings().getSecretParameters());
        addJsonStringParams(jsonParams, model.getTaskSettings().getParameters());

        if (query != null) {
            jsonParams.put(QUERY, toJson(query, QUERY));
        }

        jsonParams.put(INPUT, toJson(input, INPUT));

        substitutor = new ValidatingSubstitutor(jsonParams, "${", "}");
        uri = buildUri();
    }

    private static void addJsonStringParams(Map<String, String> jsonStringParams, Map<String, ?> params) {
        for (var entry : params.entrySet()) {
            jsonStringParams.put(entry.getKey(), toJson(entry.getValue(), entry.getKey()));
        }
    }

    private URI buildUri() {
        String replacedUrl = substitutor.replace(model.getServiceSettings().getUrl(), URL);
        return URI.create(replacedUrl);
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpRequest = new HttpPost(uri);

        setHeaders(httpRequest);
        setRequestContent(httpRequest);

        return new HttpRequest(httpRequest, getInferenceEntityId());
    }

    private void setHeaders(HttpRequestBase httpRequest) {
        // Header content_type's default value, if user defines the Content-Type, it will be replaced by user's value;
        httpRequest.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        for (var entry : model.getServiceSettings().getHeaders().entrySet()) {
            String replacedHeadersValue = substitutor.replace(entry.getValue(), Strings.format("header.%s", entry.getKey()));
            httpRequest.setHeader(entry.getKey(), replacedHeadersValue);
        }
    }

    private void setRequestContent(HttpPost httpRequest) {
        String replacedRequestContentString = substitutor.replace(model.getServiceSettings().getRequestContentString(), REQUEST_CONTENT);
        StringEntity stringEntity = new StringEntity(replacedRequestContentString, StandardCharsets.UTF_8);
        httpRequest.setEntity(stringEntity);
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public URI getURI() {
        return uri;
    }

    public CustomServiceSettings getServiceSettings() {
        return model.getServiceSettings();
    }

    @Override
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }
}
