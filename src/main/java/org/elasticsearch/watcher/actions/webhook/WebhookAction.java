/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.HttpResponse;

import java.io.IOException;

/**
 *
 */
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

    public static WebhookAction parse(String watchId, String actionId, XContentParser parser, HttpRequestTemplate.Parser requestParser) throws IOException {
        try {
            HttpRequestTemplate request = requestParser.parse(parser);
            return new WebhookAction(request);
        } catch (HttpRequestTemplate.ParseException pe) {
            throw new WebhookActionException("could not parse [{}] action [{}/{}]. failed parsing http request template", pe, TYPE, watchId, actionId);
        }
    }

    public static Builder builder(HttpRequestTemplate requestTemplate) {
        return new Builder(requestTemplate);
    }

    public abstract static class Result extends Action.Result {

        public Result(boolean success) {
            super(TYPE, success);
        }

        public static class Executed extends Result {

            private final HttpRequest request;
            private final HttpResponse response;

            public Executed(HttpRequest request, HttpResponse response) {
                super(response.status() < 400);
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
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.REQUEST.getPreferredName(), request, params)
                        .field(Field.RESPONSE.getPreferredName(), response, params);
            }
        }

        static class Failure extends Result {

            private final String reason;

            Failure(String reason, Object... args) {
                super(false);
                this.reason = LoggerMessageFormat.format(reason, args);
            }

            public String reason() {
                return reason;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.REASON.getPreferredName(), reason);
            }
        }

        static class Simulated extends Result {

            private final HttpRequest request;

            public Simulated(HttpRequest request) {
                super(true);
                this.request = request;
            }

            public HttpRequest request() {
                return request;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.SIMULATED_REQUEST.getPreferredName(), request, params);
            }
        }

        public static Result parse(String watchId, String actionId, XContentParser parser, HttpRequest.Parser requestParser) throws IOException {
            Boolean success = null;
            String reason = null;
            HttpRequest request = null;
            HttpRequest simulatedRequest = null;
            HttpResponse response = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.REQUEST.match(currentFieldName)) {
                    try {
                        request = requestParser.parse(parser);
                    } catch (HttpRequest.Parser.ParseException pe) {
                        throw new WebhookActionException("could not parse [{}] action result [{}/{}]. failed parsing [{}] field", pe, TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (Field.SIMULATED_REQUEST.match(currentFieldName)) {
                    try {
                        simulatedRequest = requestParser.parse(parser);
                    } catch (HttpRequest.Parser.ParseException pe) {
                        throw new WebhookActionException("could not parse [{}] action result [{}/{}]. failed parsing [{}] field", pe, TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (Field.RESPONSE.match(currentFieldName)) {
                    try {
                        response = HttpResponse.parse(parser);
                    } catch (HttpResponse.ParseException pe) {
                        throw new WebhookActionException("could not parse [{}] action result [{}/{}]. failed parsing [{}] field", pe, TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (Field.REASON.match(currentFieldName)) {
                        reason = parser.text();
                    } else {
                        throw new WebhookActionException("could not parse [{}] action result [{}/{}]. unexpected string field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (Field.SUCCESS.match(currentFieldName)) {
                        success = parser.booleanValue();
                    } else {
                        throw new WebhookActionException("could not parse [{}] action result [{}/{}]. unexpected boolean field [{}]", TYPE, watchId, actionId, watchId, currentFieldName);
                    }
                } else {
                    throw new WebhookActionException("could not parse [{}] action result [{}/{}]. unexpected token [{}]", TYPE, watchId, actionId, token);
                }
            }

            if (success == null) {
                throw new WebhookActionException("could not parse [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.SUCCESS.getPreferredName());
            }

            if (simulatedRequest != null) {
                return new Simulated(simulatedRequest);
            }

            if (reason != null) {
                return new Failure(reason);
            }

            if (request == null) {
                throw new WebhookActionException("could not parse executed [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.REQUEST.getPreferredName());
            }

            if (response == null) {
                throw new WebhookActionException("could not parse executed [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.RESPONSE.getPreferredName());
            }

            return new Executed(request, response);
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

    interface Field extends Action.Field {
        ParseField REQUEST = new ParseField("request");
        ParseField SIMULATED_REQUEST = new ParseField("simulated_request");
        ParseField RESPONSE = new ParseField("response");
    }
}
