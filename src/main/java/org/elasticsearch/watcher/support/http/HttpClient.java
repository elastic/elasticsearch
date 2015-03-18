/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.*;
import java.util.Map;

/**
 * Client class to wrap http connections
 */
public class HttpClient extends AbstractComponent {

    @Inject
    public HttpClient(Settings settings) {
        super(settings);
    }

    // TODO: Remove this when webhook action has been refactored to use this client properly
    public HttpResponse execute(HttpMethod method, String urlString, String body) throws IOException {
        URL url = new URL(urlString);
        HttpRequest request = new HttpRequest();
        request.method(method);
        return doExecute(url, request);
    }

    public HttpResponse execute(HttpRequest request) throws IOException {
        String queryString = null;
        if (request.params() != null) {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, String> entry : request.params().entrySet()) {
                if (builder.length() != 0) {
                    builder.append('&');
                }
                builder.append(URLEncoder.encode(entry.getKey(), "utf-8"))
                        .append('=')
                        .append(URLEncoder.encode(entry.getValue(), "utf-8"));
            }
            queryString = builder.toString();
        }

        URI uri;
        try {
            uri = new URI("http", null, request.host(), request.port(), request.path(), queryString, null);
        } catch (URISyntaxException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
        URL url = uri.toURL();
        return doExecute(url, request);
    }

    // TODO: Embed this method  in execute() when webhook action has been refactored to use this client properly
    private HttpResponse doExecute(URL url, HttpRequest request) throws IOException {
        logger.debug("making [{}] request to [{}]", request.method().method(), url);
        logger.trace("sending [{}] as body of request", request.body());
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setRequestMethod(request.method().method());
        if (request.headers() != null) {
            for (Map.Entry<String, String> entry : request.headers().entrySet()) {
                urlConnection.setRequestProperty(entry.getKey(), entry.getValue());
            }
        }
        if (request.auth() != null) {
            request.auth().update(urlConnection);
        }
        urlConnection.setUseCaches(false);
        urlConnection.setRequestProperty("Accept-Charset", Charsets.UTF_8.name());
        if (request.body() != null) {
            urlConnection.setDoOutput(true);
            byte[] bytes = request.body().getBytes(Charsets.UTF_8.name());
            urlConnection.setRequestProperty("Content-Length", String.valueOf(bytes.length));
            urlConnection.getOutputStream().write(bytes);
            urlConnection.getOutputStream().close();
        }

        HttpResponse response = new HttpResponse(urlConnection.getResponseCode());
        response.inputStream(urlConnection.getInputStream());
        logger.debug("http status code: {}", response.status());
        response.inputStream(urlConnection.getInputStream());
        return response;
    }

}
