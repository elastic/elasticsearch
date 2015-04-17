/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.http;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.input.InputFactory;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.template.TemplateEngine;

import java.io.IOException;

/**
 *
 */
public final class HttpInputFactory extends InputFactory<HttpInput, HttpInput.Result, ExecutableHttpInput> {

    private final HttpClient httpClient;
    private final TemplateEngine templateEngine;
    private final HttpRequest.Parser requestParser;
    private final HttpRequestTemplate.Parser requestTemplateParser;

    @Inject
    public HttpInputFactory(Settings settings, HttpClient httpClient, TemplateEngine templateEngine, HttpRequest.Parser requestParser, HttpRequestTemplate.Parser requestTemplateParser) {
        super(Loggers.getLogger(ExecutableHttpInput.class, settings));
        this.templateEngine = templateEngine;
        this.httpClient = httpClient;
        this.requestParser = requestParser;
        this.requestTemplateParser = requestTemplateParser;
    }

    @Override
    public String type() {
        return HttpInput.TYPE;
    }

    @Override
    public HttpInput parseInput(String watchId, XContentParser parser) throws IOException {
        return HttpInput.parse(watchId, parser, requestTemplateParser);
    }

    @Override
    public HttpInput.Result parseResult(String watchId, XContentParser parser) throws IOException {
        return HttpInput.Result.parse(watchId, parser, requestParser);
    }

    @Override
    public ExecutableHttpInput createExecutable(HttpInput input) {
        return new ExecutableHttpInput(input, inputLogger, httpClient, templateEngine);
    }
}
