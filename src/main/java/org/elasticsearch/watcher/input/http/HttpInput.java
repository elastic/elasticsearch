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
import org.elasticsearch.watcher.input.InputException;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.XContentFilterKeysUtils;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.http.TemplatedHttpRequest;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.WatchExecutionContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class HttpInput extends Input<HttpInput.Result> {

    public static final String TYPE = "http";

    private final HttpClient client;
    private final Set<String> extractKeys;
    private final TemplatedHttpRequest request;

    public HttpInput(ESLogger logger, HttpClient client, TemplatedHttpRequest request, Set<String> extractKeys) {
        super(logger);
        this.request = request;
        this.client = client;
        this.extractKeys = extractKeys;
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
            byte[] bytes = response.body();
            final Payload payload;
            if (extractKeys != null) {
                XContentParser parser = XContentHelper.createParser(bytes, 0, bytes.length);
                Map<String, Object> filteredKeys = XContentFilterKeysUtils.filterMapOrdered(extractKeys, parser);
                payload = new Payload.Simple(filteredKeys);
            } else {
                Tuple<XContentType, Map<String, Object>> result = XContentHelper.convertToMap(bytes, true);
                payload = new Payload.Simple(result.v2());
            }
            return new Result(TYPE, payload, httpRequest, response.status());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.REQUEST_FIELD.getPreferredName());
        builder = request.toXContent(builder, params);
        if (extractKeys != null) {
            builder.startArray(Parser.EXTRACT_FIELD.getPreferredName());
            for (String extractKey : extractKeys) {
                builder.value(extractKey);
            }
            builder.endObject();
        }
        return builder.endObject();
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
        public static final ParseField EXTRACT_FIELD = new ParseField("extract");
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
            Set<String> extract = null;
            TemplatedHttpRequest request = null;

            String currentFieldName = null;
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                switch (token) {
                    case FIELD_NAME:
                        currentFieldName = parser.currentName();
                        break;
                    case START_OBJECT:
                        if (REQUEST_FIELD.getPreferredName().equals(currentFieldName)) {
                            request = templatedRequestParser.parse(parser);
                        } else {
                            throw new InputException("could not parse [http] input. unexpected field [" + currentFieldName + "]");
                        }
                        break;
                    case START_ARRAY:
                        if (EXTRACT_FIELD.getPreferredName().equals(currentFieldName)) {
                            extract = new HashSet<>();
                            for (XContentParser.Token arrayToken = parser.nextToken(); arrayToken != XContentParser.Token.END_ARRAY; arrayToken = parser.nextToken()) {
                                if (arrayToken == XContentParser.Token.VALUE_STRING) {
                                    extract.add(parser.text());
                                }
                            }
                        } else {
                            throw new InputException("could not parse [http] input. unexpected field [" + currentFieldName + "]");
                        }
                        break;
                    default:
                        throw new InputException("could not parse [http] input. unexpected token [" + token + "]");

                }
            }

            if (request == null) {
                throw new InputException("could not parse [http] input. http request is missing or null.");
            }

            return new HttpInput(logger, client, request, extract);
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
                    if (HTTP_STATUS_FIELD.match(currentFieldName)) {
                        statusCode = parser.intValue();
                    }
                }
            }
            return new Result(TYPE, payload, request, statusCode);
        }

    }

    public final static class SourceBuilder implements Input.SourceBuilder {

        private TemplatedHttpRequest.SourceBuilder request;
        private Set<String> extractKeys;

        public SourceBuilder(TemplatedHttpRequest.SourceBuilder request) {
            this.request = request;
        }

        public SourceBuilder addExtractKey(String key) {
            if (extractKeys == null) {
                extractKeys = new HashSet<>();
            }
            extractKeys.add(key);
            return this;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (extractKeys != null) {
                builder.startArray(Parser.EXTRACT_FIELD.getPreferredName());
                for (String extractKey : extractKeys) {
                    builder.value(extractKey);
                }
                builder.endArray();
            }
            builder.field(Parser.REQUEST_FIELD.getPreferredName());
            request.toXContent(builder, params);
            return builder.endObject();
        }
    }

}
