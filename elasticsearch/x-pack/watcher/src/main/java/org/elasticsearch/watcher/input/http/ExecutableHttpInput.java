/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.http;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.XContentFilterKeysUtils;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.elasticsearch.watcher.watch.Payload;

import java.util.Map;

/**
 */
public class ExecutableHttpInput extends ExecutableInput<HttpInput, HttpInput.Result> {

    private final HttpClient client;
    private final TextTemplateEngine templateEngine;

    public ExecutableHttpInput(HttpInput input, ESLogger logger, HttpClient client, TextTemplateEngine templateEngine) {
        super(input, logger);
        this.client = client;
        this.templateEngine = templateEngine;
    }

    public HttpInput.Result execute(WatchExecutionContext ctx, Payload payload) {
        HttpRequest request = null;
        try {
            Map<String, Object> model = Variables.createCtxModel(ctx, payload);
            request = input.getRequest().render(templateEngine, model);
            return doExecute(ctx, request);
        } catch (Exception e) {
            logger.error("failed to execute [{}] input for [{}]", e, HttpInput.TYPE, ctx.watch());
            return new HttpInput.Result(request, e);
        }
    }

    HttpInput.Result doExecute(WatchExecutionContext ctx, HttpRequest request) throws Exception {
        HttpResponse response = client.execute(request);

        if (!response.hasContent()) {
            return new HttpInput.Result(request, response.status(), Payload.EMPTY);
        }

        XContentType contentType = response.xContentType();
        if (input.getExpectedResponseXContentType() != null) {
            if (contentType != input.getExpectedResponseXContentType().contentType()) {
                logger.warn("[{}] [{}] input expected content type [{}] but read [{}] from headers", type(), ctx.id(),
                        input.getExpectedResponseXContentType(), contentType);
            }
            if (contentType == null) {
                contentType = input.getExpectedResponseXContentType().contentType();
            }
        } else {
            //Attempt to auto detect content type
            if (contentType == null) {
                contentType = XContentFactory.xContentType(response.body());
            }
        }

        XContentParser parser = null;
        if (contentType != null) {
            try {
                parser = contentType.xContent().createParser(response.body());
            } catch (Exception e) {
                throw new ElasticsearchParseException("could not parse response body [{}] it does not appear to be [{}]", type(), ctx.id(),
                        response.body().toUtf8(), contentType.shortName());
            }
        }

        final Payload payload;
        if (input.getExtractKeys() != null) {
            Map<String, Object> filteredKeys = XContentFilterKeysUtils.filterMapOrdered(input.getExtractKeys(), parser);
            payload = new Payload.Simple(filteredKeys);
        } else {
            if (parser != null) {
                Map<String, Object> map = parser.mapOrdered();
                payload = new Payload.Simple(map);
            } else {
                payload = new Payload.Simple("_value", response.body().toUtf8());
            }
        }
        return new HttpInput.Result(request, response.status(), payload);
    }
}
