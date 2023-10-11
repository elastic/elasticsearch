/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.methods.HttpUriRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class RequestTask extends HttpTask {
    private static final Logger logger = LogManager.getLogger(RequestTask.class);

    private final HttpUriRequest request;
    private final ActionListener<HttpResult> listener;
    private final HttpClient httpClient;

    public RequestTask(HttpUriRequest request, HttpClient httpClient, ActionListener<HttpResult> listener) {
        this.request = Objects.requireNonNull(request);
        this.httpClient = Objects.requireNonNull(httpClient);
        this.listener = Objects.requireNonNull(listener);
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    @Override
    protected void doRun() throws Exception {
        assert context != null : "the http context must be set before calling doRun";

        try {
            httpClient.send(request, context, listener);
        } catch (IOException e) {
            logger.error(format("Failed to send request [%s] via the http client", request.getRequestLine()), e);
            listener.onFailure(new ElasticsearchException(format("Failed to send request [%s]", request.getRequestLine()), e));
        }
    }

    @Override
    public String toString() {
        return request.getRequestLine().toString();
    }
}
