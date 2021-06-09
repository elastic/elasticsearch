/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.webhook;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;

import java.io.IOException;

public class WebhookAction implements Action {

    public static final String TYPE = "webhook";

    final HttpRequestTemplate requestTemplate;

    public WebhookAction(HttpRequestTemplate requestTemplate) {
        this.requestTemplate = requestTemplate;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public HttpRequestTemplate getRequest() {
        return requestTemplate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WebhookAction action = (WebhookAction) o;

        return requestTemplate.equals(action.requestTemplate);
    }

    @Override
    public int hashCode() {
        return requestTemplate.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return requestTemplate.toXContent(builder, params);
    }

    public static WebhookAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        try {
            HttpRequestTemplate request = HttpRequestTemplate.Parser.parse(parser);
            return new WebhookAction(request);
        } catch (ElasticsearchParseException pe) {
            throw new ElasticsearchParseException("could not parse [{}] action [{}/{}]. failed parsing http request template", pe, TYPE,
                    watchId, actionId);
        }
    }

    public static Builder builder(HttpRequestTemplate requestTemplate) {
        return new Builder(requestTemplate);
    }

    public interface Result {

        class Success extends Action.Result implements Result {

            private final HttpRequest request;
            private final HttpResponse response;

            public Success(HttpRequest request, HttpResponse response) {
                super(TYPE, Status.SUCCESS);
                this.request = request;
                this.response = response;
            }

            public HttpResponse response() {
                return response;
            }

            public HttpRequest request() {
                return request;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject(type)
                        .field(Field.REQUEST.getPreferredName(), request, params)
                        .field(Field.RESPONSE.getPreferredName(), response, params)
                        .endObject();
            }
        }

        class Failure extends Action.Result.Failure implements Result {

            private final HttpRequest request;
            private final HttpResponse response;

            public Failure(HttpRequest request, HttpResponse response) {
                this(request, response, "received [{}] status code", response.status());
            }

            private Failure(HttpRequest request, HttpResponse response, String reason, Object... args) {
                super(TYPE, reason, args);
                this.request = request;
                this.response = response;
            }

            public HttpResponse response() {
                return response;
            }

            public HttpRequest request() {
                return request;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                super.toXContent(builder, params);
                return builder.startObject(type)
                        .field(Field.REQUEST.getPreferredName(), request, params)
                        .field(Field.RESPONSE.getPreferredName(), response, params)
                        .endObject();
            }
        }

        class Simulated extends Action.Result implements Result {

            private final HttpRequest request;

            public Simulated(HttpRequest request) {
                super(TYPE, Status.SIMULATED);
                this.request = request;
            }

            public HttpRequest request() {
                return request;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject(type)
                        .field(Field.REQUEST.getPreferredName(), request, params)
                        .endObject();
            }
        }

    }

    public static class Builder implements Action.Builder<WebhookAction> {

        final HttpRequestTemplate requestTemplate;

        private Builder(HttpRequestTemplate requestTemplate) {
            this.requestTemplate = requestTemplate;
        }

        @Override
        public WebhookAction build() {
            return new WebhookAction(requestTemplate);
        }
    }

    interface Field {
        ParseField REQUEST = new ParseField("request");
        ParseField RESPONSE = new ParseField("response");
    }
}
