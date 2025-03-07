/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.custom;

import com.google.gson.Gson;

import org.apache.commons.text.StringSubstitutor;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.custom.CustomModel;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings;
import org.elasticsearch.xpack.inference.services.custom.CustomTaskSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.REQUEST_FORMAT_JSON;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.REQUEST_FORMAT_STRING;

public class CustomRequest implements Request {
    private final long startTime;

    public static final Gson gson;
    static {
        gson = new Gson();
    }

    private final String QUERY = "query";
    private final String INPUT = "input";

    private final Map<String, String> customParams;

    private final CustomServiceSettings serviceSettings;
    private final CustomTaskSettings taskSettings;
    private final Map<String, Object> secretParameters;
    private final String url;
    private final String path;
    private final String method;
    private final String queryString;
    private final Map<String, Object> headers;
    private final String requestFormat;
    private final Map<String, Object> requestContent;
    private final String requestContentString;
    private final URI uri;
    StringSubstitutor substitutor;
    private final String inferenceEntityId;

    public CustomRequest(String query, List<String> input, CustomModel model) {
        this.startTime = System.currentTimeMillis();
        Objects.requireNonNull(model);

        serviceSettings = model.getServiceSettings();
        taskSettings = model.getTaskSettings();
        secretParameters = model.getSecretSettings().getSecretParameters();
        path = serviceSettings.getPath();
        method = serviceSettings.getMethod().toUpperCase(Locale.ROOT);
        queryString = serviceSettings.getQueryString();
        headers = serviceSettings.getHeaders();
        requestFormat = serviceSettings.getRequestFormat();
        requestContent = serviceSettings.getRequestContent();
        requestContentString = serviceSettings.getRequestContentString();
        url = model.getServiceSettings().getUrl();

        Map<String, Object> customParamsObjectMap = new HashMap<>();
        if (secretParameters != null) {
            for (String key : secretParameters.keySet()) {
                Object paramValue = secretParameters.get(key);
                if (paramValue instanceof SecureString) {
                    customParamsObjectMap.put(key, ((SecureString) paramValue).toString());
                } else {
                    customParamsObjectMap.put(key, paramValue);
                }
            }
        }

        customParams = new HashMap<String, String>();
        if (taskSettings.getParameters() != null && taskSettings.getParameters().isEmpty() == false) {
            Map<String, String> taskParams = getParameterMap(taskSettings.getParameters());
            for (String key : taskParams.keySet()) {
                customParams.put(key, taskParams.get(key));
            }
        }

        // if user's custom parameters contain input and query, it will be replaced by inference's input and query
        if (query != null) {
            customParamsObjectMap.put(QUERY, query);
        }

        String serviceType = serviceSettings.getServiceType();
        TaskType taskType = TaskType.fromStringOrStatusException(serviceType);
        if (taskType.equals(TaskType.COMPLETION)) {
            if (input.size() == 1) {
                customParamsObjectMap.put(INPUT, input.get(0));
            } else {
                customParamsObjectMap.put(INPUT, input);
            }
        } else {
            customParamsObjectMap.put(INPUT, input);
        }
        customParams.putAll(getParameterMap(customParamsObjectMap));

        substitutor = new StringSubstitutor(customParams, "${", "}");

        uri = buildUri();
        inferenceEntityId = model.getInferenceEntityId();
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpRequestBase httpRequest;
        if (method.equalsIgnoreCase(HttpGet.METHOD_NAME)) {
            httpRequest = new HttpGet(uri);
        } else if (method.equalsIgnoreCase(HttpPost.METHOD_NAME)) {
            httpRequest = new HttpPost(uri);
        } else if (method.equalsIgnoreCase(HttpPut.METHOD_NAME)) {
            httpRequest = new HttpPut(uri);
        } else {
            throw new IllegalArgumentException("unsupported http method [" + method + "], support GET, PUT and POST");
        }

        setHeaders(httpRequest);
        setRequestContent(httpRequest);

        return new HttpRequest(httpRequest, getInferenceEntityId());
    }

    private void setHeaders(HttpRequestBase httpRequest) {
        // Header content_type's default value, if user defines the Content-Type, it will be replaced by user's value;
        httpRequest.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());

        if (headers != null && headers.isEmpty() == false) {
            for (String key : headers.keySet()) {
                String headersValue = (String) headers.get(key);
                String replacedHeadersValue = substitutor.replace(headersValue);
                placeholderValidation(replacedHeadersValue);
                httpRequest.setHeader(key, replacedHeadersValue);
            }
        }
    }

    private void setRequestContent(HttpRequestBase httpRequest) {
        switch (requestFormat.toLowerCase()) {
            case REQUEST_FORMAT_JSON: {
                // todo: support json format request
                break;
            }
            case REQUEST_FORMAT_STRING: {
                if (requestContentString != null && (method.equals(HttpPost.METHOD_NAME) || method.equals(HttpPut.METHOD_NAME))) {
                    String replacedRequestContentString = substitutor.replace(requestContentString);
                    placeholderValidation(replacedRequestContentString);
                    StringEntity stringEntity = new StringEntity(replacedRequestContentString, StandardCharsets.UTF_8);
                    if (httpRequest instanceof HttpPost) {
                        ((HttpPost) httpRequest).setEntity(stringEntity);
                    } else if (httpRequest instanceof HttpPut) {
                        ((HttpPut) httpRequest).setEntity(stringEntity);
                    }
                }
                break;
            }
        }

    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    public CustomServiceSettings getServiceSettings() {
        return serviceSettings;
    }

    @Override
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

    URI buildUri() {
        try {
            String uri = url + path;
            if (queryString != null) {
                String replacedQueryString = substitutor.replace(queryString);
                placeholderValidation(replacedQueryString);
                uri = uri + replacedQueryString;
            }
            return new URI(uri);
        } catch (URISyntaxException e) {
            // using bad request here so that potentially sensitive URL information does not get logged
            throw new ElasticsearchStatusException(
                Strings.format("Failed to construct %s URL", CustomUtils.SERVICE_NAME),
                RestStatus.BAD_REQUEST,
                e
            );
        }
    }

    @SuppressWarnings("removal")
    public static Map<String, String> getParameterMap(Map<String, ?> parameterObjs) {
        Map<String, String> parameters = new HashMap<>();
        for (String key : parameterObjs.keySet()) {
            Object value = parameterObjs.get(key);
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    if (value instanceof String) {
                        parameters.put(key, (String) value);
                    } else {
                        parameters.put(key, gson.toJson(value));
                    }
                    return null;
                });
            } catch (PrivilegedActionException e) {
                throw new RuntimeException(e);
            }
        }
        return parameters;
    }

    private void placeholderValidation(String s) throws IllegalArgumentException {
        if (taskSettings.getIgnorePlaceholderCheck() != null && taskSettings.getIgnorePlaceholderCheck()) {
            return;
        }
        String pattern = "\\$\\{.*?\\}";
        Pattern compiledPattern = Pattern.compile(pattern);
        Matcher matcher = compiledPattern.matcher(s);
        if (matcher.find()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "placeholder is not replaced, find placeholder in [%s]", s));
        }
    }

    public long getStartTime() {
        return startTime;
    }
}
