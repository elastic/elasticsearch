/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.http;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.http.TemplatedHttpRequest;
import org.elasticsearch.watcher.support.http.auth.HttpAuth;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.WatchExecutionContext;

import java.io.IOException;
import java.util.Map;

/**
 */
public class HttpInput extends Input<HttpInput.Result> {

    public static final String TYPE = "http";

    private final HttpClient client;
    private final TemplatedHttpRequest request;

    public HttpInput(ESLogger logger, HttpClient client, TemplatedHttpRequest request) {
        super(logger);
        this.request = request;
        this.client = client;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(WatchExecutionContext ctx) throws IOException {
        Map<String, Object> model = Variables.createCtxModel(ctx, null);
        HttpRequest httpRequest = request.render(model);
        try (HttpResponse response = client.execute(httpRequest)) {
            Tuple<XContentType, Map<String, Object>> result = XContentHelper.convertToMap(response.body(), true);
            return new Result(TYPE, new Payload.Simple(result.v2()), httpRequest, response.status());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return request.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HttpInput httpInput = (HttpInput) o;

        if (!request.equals(httpInput.request)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return request.hashCode();
    }

    TemplatedHttpRequest getRequest() {
        return request;
    }

    public final static class Result extends Input.Result {

        private final HttpRequest request;
        private final int statusCode;

        public Result(String type, Payload payload, HttpRequest request, int statusCode) {
            super(type, payload);
            this.request = request;
            this.statusCode = statusCode;
        }
        @Override
        protected XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(Parser.HTTP_STATUS_FIELD.getPreferredName(), statusCode);
            builder.field(Parser.REQUEST_FIELD.getPreferredName(), request);
            return builder;
        }

        HttpRequest request() {
            return request;
        }

        int statusCode() {
            return statusCode;
        }
    }

    public final static class Parser extends AbstractComponent implements Input.Parser<Result, HttpInput> {

        public static final ParseField REQUEST_FIELD = new ParseField("request");
        public static final ParseField HTTP_STATUS_FIELD = new ParseField("http_status");

        private final HttpClient client;
        private final HttpRequest.Parser requestParser;
        private final TemplatedHttpRequest.Parser templatedRequestParser;

        @Inject
        public Parser(Settings settings, HttpClient client, HttpRequest.Parser requestParser, TemplatedHttpRequest.Parser templatedRequestParser) {
            super(settings);
            this.client = client;
            this.requestParser = requestParser;
            this.templatedRequestParser = templatedRequestParser;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public HttpInput parse(XContentParser parser) throws IOException {
            TemplatedHttpRequest request = templatedRequestParser.parse(parser);
            return new HttpInput(logger, client, request);
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            Payload payload = null;
            HttpRequest request = null;
            int statusCode = -1;

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Input.Result.PAYLOAD_FIELD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else if (REQUEST_FIELD.match(currentFieldName)) {
                        request = requestParser.parse(parser);
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    statusCode = parser.intValue();
                }
            }
            return new Result(TYPE, payload, request, statusCode);
        }

    }

    public final static class SourceBuilder implements Input.SourceBuilder {

        private String host;
        private int port;
        private String method;
        private Template path;
        private Map<String, Template> params;
        private Map<String, Template> headers;
        private HttpAuth auth;
        private Template body;

        public SourceBuilder setHost(String host) {
            this.host = host;
            return this;
        }

        public SourceBuilder setPort(int port) {
            this.port = port;
            return this;
        }

        public SourceBuilder setMethod(String method) {
            this.method = method;
            return this;
        }

        public SourceBuilder setPath(Template path) {
            this.path = path;
            return this;
        }

        public SourceBuilder setParams(Map<String, Template> params) {
            this.params = params;
            return this;
        }

        public SourceBuilder setHeaders(Map<String, Template> headers) {
            this.headers = headers;
            return this;
        }

        public SourceBuilder setAuth(HttpAuth auth) {
            this.auth = auth;
            return this;
        }

        public SourceBuilder setBody(Template body) {
            this.body = body;
            return this;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params p) throws IOException {
            builder.startObject();
            builder.field(HttpRequest.Parser.HOST_FIELD.getPreferredName(), host);
            builder.field(HttpRequest.Parser.PORT_FIELD.getPreferredName(), port);
            if (method != null) {
                builder.field(HttpRequest.Parser.METHOD_FIELD.getPreferredName(), method);
            }
            if (path != null) {
                builder.field(HttpRequest.Parser.PATH_FIELD.getPreferredName(), path);
            }
            if (params != null) {
                builder.field(HttpRequest.Parser.PARAMS_FIELD.getPreferredName(), params);
            }
            if (headers != null) {
                builder.field(HttpRequest.Parser.HEADERS_FIELD.getPreferredName(), headers);
            }
            if (auth != null) {
                builder.field(HttpRequest.Parser.AUTH_FIELD.getPreferredName(), auth);
            }
            if (body != null) {
                builder.field(HttpRequest.Parser.BODY_FIELD.getPreferredName(), body);
            }
            return builder.endObject();
        }
    }

}
