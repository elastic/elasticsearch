/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.webhook;

import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.handler.codec.http.HttpMethod;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 * Client class to wrap http connections
 */
public class HttpClient extends AbstractComponent {

    @Inject
    public HttpClient(Settings settings) {
        super(settings);
    }

    public int execute(HttpMethod method, String url, String body) throws IOException {
        logger.debug("making [{}] request to [{}]", method.getName(), url);
        if (logger.isTraceEnabled()) {
            logger.trace("sending [{}] as body of request", body);
        }
        URL encodedUrl = new URL(URLEncoder.encode(url, Charsets.UTF_8.name()));
        HttpURLConnection httpConnection = (HttpURLConnection) encodedUrl.openConnection();
        httpConnection.setRequestMethod(method.getName());
        httpConnection.setRequestProperty("Accept-Charset", Charsets.UTF_8.name());
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty("Content-Length", Integer.toString(body.length()));
        httpConnection.getOutputStream().write(body.getBytes(Charsets.UTF_8.name()));
        return httpConnection.getResponseCode();
    }
}
