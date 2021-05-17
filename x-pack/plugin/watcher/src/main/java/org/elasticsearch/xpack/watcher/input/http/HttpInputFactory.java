/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.http;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.input.InputFactory;

import java.io.IOException;

public final class HttpInputFactory extends InputFactory<HttpInput, HttpInput.Result, ExecutableHttpInput> {

    private final HttpClient httpClient;
    private final TextTemplateEngine templateEngine;

    public HttpInputFactory(Settings settings, HttpClient httpClient, TextTemplateEngine templateEngine) {
        this.templateEngine = templateEngine;
        this.httpClient = httpClient;
    }

    @Override
    public String type() {
        return HttpInput.TYPE;
    }

    @Override
    public HttpInput parseInput(String watchId, XContentParser parser) throws IOException {
        return HttpInput.parse(watchId, parser);
    }

    @Override
    public ExecutableHttpInput createExecutable(HttpInput input) {
        return new ExecutableHttpInput(input, httpClient, templateEngine);
    }
}
