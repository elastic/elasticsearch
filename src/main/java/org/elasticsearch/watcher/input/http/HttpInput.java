/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.http;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class HttpInput implements Input {

    public static final String TYPE = "http";

    private final HttpRequestTemplate request;
    private final @Nullable Set<String> extractKeys;

    public HttpInput(HttpRequestTemplate request, @Nullable Set<String> extractKeys) {
        this.request = request;
        this.extractKeys = extractKeys;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public HttpRequestTemplate getRequest() {
        return request;
    }

    public Set<String> getExtractKeys() {
        return extractKeys;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.REQUEST.getPreferredName(), request, params);
        if (extractKeys != null) {
            builder.field(Field.EXTRACT.getPreferredName(), extractKeys);
        }
        builder.endObject();
        return builder;
    }

    public static HttpInput parse(String watchId, XContentParser parser, HttpRequestTemplate.Parser requestParser) throws IOException {
        Set<String> extract = null;
        HttpRequestTemplate request = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.REQUEST.match(currentFieldName)) {
                try {
                    request = requestParser.parse(parser);
                } catch (HttpRequestTemplate.ParseException pe) {
                    throw new HttpInputException("could not parse [{}] input for watch [{}]. failed to parse http request template", pe, TYPE, watchId);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Field.EXTRACT.getPreferredName().equals(currentFieldName)) {
                    extract = new HashSet<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            extract.add(parser.text());
                        } else {
                            throw new HttpInputException("could not parse [{}] input for watch [{}]. expected a string value as an [{}] item but found [{}] instead", TYPE, watchId, currentFieldName, token);
                        }
                    }
                } else {
                    throw new HttpInputException("could not parse [{}] input for watch [{}]. unexpected array field [{}]", TYPE, watchId, currentFieldName);
                }
            } else {
                throw new HttpInputException("could not parse [{}] input for watch [{}]. unexpected token [{}]", TYPE, watchId, token);
            }
        }

        if (request == null) {
            throw new HttpInputException("could not parse [{}] input for watch [{}]. missing require [{}] field", TYPE, watchId, Field.REQUEST.getPreferredName());
        }

        return new HttpInput(request, extract);
    }

    public static Builder builder(HttpRequestTemplate httpRequest) {
        return new Builder(httpRequest);
    }

    public static class Result extends Input.Result {

        private final HttpRequest sentRequest;
        private final int httpStatus;

        public Result(Payload payload, HttpRequest sentRequest, int httpStatus) {
            super(TYPE, payload);
            this.sentRequest =sentRequest;
            this.httpStatus = httpStatus;
        }

        public HttpRequest sentRequest() {
            return sentRequest;
        }

        public int statusCode() {
            return httpStatus;
        }

        @Override
        protected XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder.field(Field.SENT_REQUEST.getPreferredName(), sentRequest, params)
                    .field(Field.HTTP_STATUS.getPreferredName(), httpStatus);
        }

        public static Result parse(String watchId, XContentParser parser, HttpRequest.Parser requestParser) throws IOException {
            HttpRequest sentRequest = null;
            Payload payload = null;
            int httpStatus = -1;

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.SENT_REQUEST.match(currentFieldName)) {
                    try {
                        sentRequest = requestParser.parse(parser);
                    } catch (HttpRequest.Parser.ParseException pe) {
                        throw new HttpInputException("could not parse [{}] input result for watch [{}]. failed parsing [{}] field", pe, TYPE, watchId, Field.SENT_REQUEST.getPreferredName());
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Field.PAYLOAD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else {
                        throw new HttpInputException("could not parse [{}] input result for watch [{}]. unexpected object field [{}]", TYPE, watchId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (Field.HTTP_STATUS.match(currentFieldName)) {
                        httpStatus = parser.intValue();
                    } else {
                        throw new HttpInputException("could not parse [{}] input result for watch [{}]. unexpected numeric field [{}]", TYPE, watchId, currentFieldName);
                    }
                } else {
                    throw new HttpInputException("could not parse [{}] input result for watch [{}]. unexpected token [{}]", TYPE, watchId, token);
                }
            }

            if (sentRequest == null) {
                throw new HttpInputException("could not parse [{}] input result for watch [{}]. missing required [{}] field", TYPE, watchId, Field.SENT_REQUEST.getPreferredName());
            }

            if (httpStatus < 0) {
                throw new HttpInputException("could not parse [{}] input result for watch [{}]. missing required [{}] field", TYPE, watchId, Field.HTTP_STATUS.getPreferredName());
            }

            return new HttpInput.Result(payload, sentRequest, httpStatus);
        }
    }

    public static class Builder implements Input.Builder<HttpInput> {

        private final HttpRequestTemplate request;
        private final ImmutableSet.Builder<String> extractKeys = ImmutableSet.builder();

        private Builder(HttpRequestTemplate request) {
            this.request = request;
        }

        public Builder extractKeys(Collection<String> keys) {
            extractKeys.addAll(keys);
            return this;
        }

        public Builder extractKeys(String... keys) {
            extractKeys.add(keys);
            return this;
        }

        @Override
        public HttpInput build() {
            ImmutableSet<String> keys = extractKeys.build();
            return new HttpInput(request, keys.isEmpty() ? null : keys);
        }
    }

    interface Field extends Input.Field {
        ParseField REQUEST = new ParseField("request");
        ParseField SENT_REQUEST = new ParseField("sent_request");
        ParseField EXTRACT = new ParseField("extract");
        ParseField HTTP_STATUS = new ParseField("http_status");
    }
}
