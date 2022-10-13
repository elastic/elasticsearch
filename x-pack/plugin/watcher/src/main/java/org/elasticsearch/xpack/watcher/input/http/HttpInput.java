/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.http;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpContentType;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public class HttpInput implements Input {

    public static final String TYPE = "http";

    private final HttpRequestTemplate request;
    @Nullable
    private final HttpContentType expectedResponseXContentType;
    @Nullable
    private final Set<String> extractKeys;

    public HttpInput(
        HttpRequestTemplate request,
        @Nullable HttpContentType expectedResponseXContentType,
        @Nullable Set<String> extractKeys
    ) {
        this.request = request;
        this.expectedResponseXContentType = expectedResponseXContentType;
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

    public HttpContentType getExpectedResponseXContentType() {
        return expectedResponseXContentType;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.REQUEST.getPreferredName(), request, params);
        if (extractKeys != null) {
            builder.field(Field.EXTRACT.getPreferredName(), extractKeys);
        }
        if (expectedResponseXContentType != null) {
            builder.field(Field.RESPONSE_CONTENT_TYPE.getPreferredName(), expectedResponseXContentType.id());
        }
        builder.endObject();
        return builder;
    }

    public static HttpInput parse(String watchId, XContentParser parser) throws IOException {
        Set<String> extract = null;
        HttpRequestTemplate request = null;
        HttpContentType expectedResponseBodyType = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.REQUEST.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    request = HttpRequestTemplate.Parser.parse(parser);
                } catch (ElasticsearchParseException pe) {
                    throw new ElasticsearchParseException(
                        "could not parse [{}] input for watch [{}]. failed to parse http request " + "template",
                        pe,
                        TYPE,
                        watchId
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Field.EXTRACT.getPreferredName().equals(currentFieldName)) {
                    extract = new HashSet<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            extract.add(parser.text());
                        } else {
                            throw new ElasticsearchParseException(
                                "could not parse [{}] input for watch [{}]. expected a string value as "
                                    + "an [{}] item but found [{}] instead",
                                TYPE,
                                watchId,
                                currentFieldName,
                                token
                            );
                        }
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse [{}] input for watch [{}]. unexpected array field [{}]",
                        TYPE,
                        watchId,
                        currentFieldName
                    );
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (Field.RESPONSE_CONTENT_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                    expectedResponseBodyType = HttpContentType.resolve(parser.text());
                    if (expectedResponseBodyType == null) {
                        throw new ElasticsearchParseException(
                            "could not parse [{}] input for watch [{}]. unknown content type [{}]",
                            TYPE,
                            watchId,
                            parser.text()
                        );
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse [{}] input for watch [{}]. unexpected string field [{}]",
                        TYPE,
                        watchId,
                        currentFieldName
                    );
                }
            } else {
                throw new ElasticsearchParseException(
                    "could not parse [{}] input for watch [{}]. unexpected token [{}]",
                    TYPE,
                    watchId,
                    token
                );
            }
        }

        if (request == null) {
            throw new ElasticsearchParseException(
                "could not parse [{}] input for watch [{}]. missing require [{}] field",
                TYPE,
                watchId,
                Field.REQUEST.getPreferredName()
            );
        }

        if (expectedResponseBodyType == HttpContentType.TEXT && extract != null) {
            throw new ElasticsearchParseException(
                "could not parse [{}] input for watch [{}]. key extraction is not supported for content" + " type [{}]",
                TYPE,
                watchId,
                expectedResponseBodyType
            );
        }

        return new HttpInput(request, expectedResponseBodyType, extract);
    }

    public static Builder builder(HttpRequestTemplate httpRequest) {
        return new Builder(httpRequest);
    }

    public static class Result extends Input.Result {

        @Nullable
        private final HttpRequest request;
        final int statusCode;

        public Result(HttpRequest request, int statusCode, Payload payload) {
            super(TYPE, payload);
            this.request = request;
            this.statusCode = statusCode;
        }

        public Result(@Nullable HttpRequest request, Exception e) {
            super(TYPE, e);
            this.request = request;
            this.statusCode = -1;
        }

        public HttpRequest request() {
            return request;
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            if (request == null) {
                return builder;
            }
            builder.startObject(type);
            builder.field(Field.REQUEST.getPreferredName(), request, params);
            if (statusCode > 0) {
                builder.field(Field.STATUS_CODE.getPreferredName(), statusCode);
            }
            return builder.endObject();
        }
    }

    public static class Builder implements Input.Builder<HttpInput> {

        private final HttpRequestTemplate request;
        private final Set<String> extractKeys = new HashSet<>();
        private HttpContentType expectedResponseXContentType = null;

        private Builder(HttpRequestTemplate request) {
            this.request = request;
        }

        public Builder extractKeys(Collection<String> keys) {
            extractKeys.addAll(keys);
            return this;
        }

        public Builder extractKeys(String... keys) {
            Collections.addAll(extractKeys, keys);
            return this;
        }

        public Builder expectedResponseXContentType(HttpContentType expectedResponseXContentType) {
            this.expectedResponseXContentType = expectedResponseXContentType;
            return this;
        }

        @Override
        public HttpInput build() {
            return new HttpInput(request, expectedResponseXContentType, extractKeys.isEmpty() ? null : unmodifiableSet(extractKeys));
        }
    }

    interface Field {
        ParseField REQUEST = new ParseField("request");
        ParseField EXTRACT = new ParseField("extract");
        ParseField STATUS_CODE = new ParseField("status_code");
        ParseField RESPONSE_CONTENT_TYPE = new ParseField("response_content_type");
    }
}
