/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.input.http;


import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.support.Variables;
import org.elasticsearch.xpack.watcher.support.XContentFilterKeysUtils;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.watcher.input.http.HttpInput.TYPE;

public class ExecutableHttpInput extends ExecutableInput<HttpInput, HttpInput.Result> {
    private static final Logger logger = LogManager.getLogger(ExecutableHttpInput.class);

    private final HttpClient client;
    private final TextTemplateEngine templateEngine;

    public ExecutableHttpInput(HttpInput input, HttpClient client, TextTemplateEngine templateEngine) {
        super(input);
        this.client = client;
        this.templateEngine = templateEngine;
    }

    public HttpInput.Result execute(WatchExecutionContext ctx, Payload payload) {
        HttpRequest request = null;
        try {
            Map<String, Object> model = Variables.createCtxParamsMap(ctx, payload);
            request = input.getRequest().render(templateEngine, model);
            return doExecute(ctx, request);
        } catch (Exception e) {
            logger.error("failed to execute [{}] input for watch [{}], reason [{}]", TYPE, ctx.watch().id(), e.getMessage());
            return new HttpInput.Result(request, e);
        }
    }

    HttpInput.Result doExecute(WatchExecutionContext ctx, HttpRequest request) throws Exception {
        HttpResponse response = client.execute(request);
        Map<String, List<String>> headers = response.headers();
        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put("_status_code", response.status());
        if (headers.isEmpty() == false) {
            payloadMap.put("_headers", headers);
        }

        if (!response.hasContent()) {
            return new HttpInput.Result(request, response.status(), new Payload.Simple(payloadMap));
        }

        final XContentType contentType;
        XContentType responseContentType = response.xContentType();
        if (input.getExpectedResponseXContentType() == null) {
            //Attempt to auto detect content type, if not set in response
            contentType = responseContentType != null ? responseContentType : XContentHelper.xContentType(response.body());
        } else {
            contentType = input.getExpectedResponseXContentType().contentType();
            if (responseContentType != contentType) {
                logger.warn("[{}] [{}] input expected content type [{}] but read [{}] from headers, using expected one", type(), ctx.id(),
                        input.getExpectedResponseXContentType(), responseContentType);
            }
        }

        if (contentType != null) {
            // EMPTY is safe here because we never use namedObject
            try (InputStream stream = response.body().streamInput();
                 XContentParser parser = contentType.xContent()
                         .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                if (input.getExtractKeys() != null) {
                    payloadMap.putAll(XContentFilterKeysUtils.filterMapOrdered(input.getExtractKeys(), parser));
                } else {
                    // special handling if a list is returned, i.e. JSON like [ {},{} ]
                    XContentParser.Token token = parser.nextToken();
                    if (token == XContentParser.Token.START_ARRAY) {
                        payloadMap.put("data", parser.listOrderedMap());
                    } else {
                        payloadMap.putAll(parser.mapOrdered());
                    }
                }
            } catch (Exception e) {
                throw new ElasticsearchParseException("could not parse response body [{}] it does not appear to be [{}]", type(), ctx.id(),
                        response.body().utf8ToString(), contentType.shortName());
            }
        } else {
            payloadMap.put("_value", response.body().utf8ToString());
        }

        return new HttpInput.Result(request, response.status(), new Payload.Simple(payloadMap));
    }
}
