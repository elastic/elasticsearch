/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.apache.commons.text.StringSubstitutor;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.custom.CustomModel;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.REQUEST_CONTENT;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.URL;

public class CustomRequest implements Request {
    /**
     * This regex pattern matches on the string {@code "${<any characters>}"}
     */
    private static final Pattern VARIABLE_PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{.*?\\}");

    private static final String QUERY = "query";
    private static final String INPUT = "input";

    private final URI uri;
    private final StringSubstitutor substitutor;
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

        substitutor = new StringSubstitutor(jsonParams, "${", "}");
        uri = buildUri();
    }

    private static <T> String toJson(T value, String field) {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            // TODO test this, I think it'll write the quotes for us so we don't need to include them in the content string
            builder.value(value);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new IllegalStateException(Strings.format("Failed to serialize custom request value as json, field: %s", field), e);
        }
    }

    private static void addJsonStringParams(Map<String, String> jsonStringParams, Map<String, ?> params) {
        for (var entry : params.entrySet()) {
            jsonStringParams.put(entry.getKey(), toJson(entry.getValue(), entry.getKey()));
        }
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
            String replacedHeadersValue = substitutor.replace(entry.getValue());
            placeholderValidation(replacedHeadersValue, Strings.format("header.%s", entry.getKey()));
            httpRequest.setHeader(entry.getKey(), replacedHeadersValue);
        }
    }

    private void setRequestContent(HttpPost httpRequest) {
        String replacedRequestContentString = substitutor.replace(model.getServiceSettings().getRequestContentString());
        placeholderValidation(replacedRequestContentString, REQUEST_CONTENT);
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

    // default for testing
    URI buildUri() {
        String replacedUrl = substitutor.replace(model.getServiceSettings().getUrl());
        placeholderValidation(replacedUrl, URL);
        return URI.create(replacedUrl);
    }

    // default for testing
    static void placeholderValidation(String substitutedString, String settingName) {
        Matcher matcher = VARIABLE_PLACEHOLDER_PATTERN.matcher(substitutedString);
        if (matcher.find()) {
            throw new IllegalStateException(String.format("Found placeholder in [%s] after replacement call", settingName));
        }
    }
}
