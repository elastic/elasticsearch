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
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.ValidatingSubstitutor;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.custom.CustomModel;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.SerializableSecureString;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.REQUEST;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.URL;

public class CustomRequest implements Request {
    private static final String QUERY = "query";
    private static final String INPUT = "input";

    private final URI uri;
    private final ValidatingSubstitutor jsonPlaceholderReplacer;
    private final ValidatingSubstitutor stringPlaceholderReplacer;
    private final CustomModel model;

    public CustomRequest(String query, List<String> input, CustomModel model) {
        this.model = Objects.requireNonNull(model);

        var stringOnlyParams = new HashMap<String, String>();
        addStringParams(stringOnlyParams, model.getSecretSettings().getSecretParameters());
        addStringParams(stringOnlyParams, model.getTaskSettings().getParameters());

        var jsonParams = new HashMap<String, String>();
        addJsonStringParams(jsonParams, model.getSecretSettings().getSecretParameters());
        addJsonStringParams(jsonParams, model.getTaskSettings().getParameters());

        if (query != null) {
            jsonParams.put(QUERY, toJson(query, QUERY));
        }

        addInputJsonParam(jsonParams, input, model.getTaskType());

        jsonPlaceholderReplacer = new ValidatingSubstitutor(jsonParams, "${", "}");
        stringPlaceholderReplacer = new ValidatingSubstitutor(stringOnlyParams, "${", "}");
        uri = buildUri();
    }

    private static void addStringParams(Map<String, String> stringParams, Map<String, ?> paramsToAdd) {
        for (var entry : paramsToAdd.entrySet()) {
            if (entry.getValue() instanceof String str) {
                stringParams.put(entry.getKey(), str);
            } else if (entry.getValue() instanceof SerializableSecureString serializableSecureString) {
                stringParams.put(entry.getKey(), serializableSecureString.getSecureString().toString());
            } else if (entry.getValue() instanceof SecureString secureString) {
                stringParams.put(entry.getKey(), secureString.toString());
            }
        }
    }

    private static void addJsonStringParams(Map<String, String> jsonStringParams, Map<String, ?> params) {
        for (var entry : params.entrySet()) {
            jsonStringParams.put(entry.getKey(), toJson(entry.getValue(), entry.getKey()));
        }
    }

    private static void addInputJsonParam(Map<String, String> jsonParams, List<String> input, TaskType taskType) {
        if (taskType == TaskType.COMPLETION && input.isEmpty() == false) {
            jsonParams.put(INPUT, toJson(input.get(0), INPUT));
        } else {
            jsonParams.put(INPUT, toJson(input, INPUT));
        }
    }

    private URI buildUri() {
        var replacedUrl = stringPlaceholderReplacer.replace(model.getServiceSettings().getUrl(), URL);

        try {
            var builder = new URIBuilder(replacedUrl);
            for (var queryParam : model.getServiceSettings().getQueryParameters().parameters()) {
                builder.addParameter(
                    queryParam.key(),
                    stringPlaceholderReplacer.replace(queryParam.value(), Strings.format("query parameters: [%s]", queryParam.key()))
                );
            }
            return builder.build();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(Strings.format("Failed to build URI, error: %s", e.getMessage()), e);
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
            String replacedHeadersValue = stringPlaceholderReplacer.replace(entry.getValue(), Strings.format("header.%s", entry.getKey()));
            httpRequest.setHeader(entry.getKey(), replacedHeadersValue);
        }
    }

    private void setRequestContent(HttpPost httpRequest) {
        String replacedRequestContentString = jsonPlaceholderReplacer.replace(
            model.getServiceSettings().getRequestContentString(),
            REQUEST
        );
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
