/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.Action.Result.Status;
import org.elasticsearch.watcher.actions.Action.Result.Throttled;
import org.elasticsearch.watcher.actions.logging.LoggingActionException;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.HttpResponse;

import java.io.IOException;
import java.util.Locale;

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

    public static Action.Result parseResult(String watchId, String actionId, XContentParser parser, HttpRequest.Parser requestParser) throws IOException {
        Status status = null;
        String reason = null;
        HttpRequest request = null;
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
            } else if (Field.RESPONSE.match(currentFieldName)) {
                try {
                    response = HttpResponse.parse(parser);
                } catch (HttpResponse.ParseException pe) {
                    throw new WebhookActionException("could not parse [{}] action result [{}/{}]. failed parsing [{}] field", pe, TYPE, watchId, actionId, currentFieldName);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (Field.REASON.match(currentFieldName)) {
                    reason = parser.text();
                } else if (Field.STATUS.match(currentFieldName)) {
                    try {
                        status = Status.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException iae) {
                        throw new LoggingActionException("could not parse [{}] action result [{}/{}]. unknown result status value [{}]", TYPE, watchId, actionId, parser.text());
                    }
                } else {
                    throw new WebhookActionException("could not parse [{}] action result [{}/{}]. unexpected string field [{}]", TYPE, watchId, actionId, currentFieldName);
                }
            } else {
                throw new WebhookActionException("could not parse [{}] action result [{}/{}]. unexpected token [{}]", TYPE, watchId, actionId, token);
            }
        }

        assertNotNull(status, "could not parse [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.STATUS.getPreferredName());

        switch (status) {

            case SUCCESS:
                assertNotNull(request, "could not parse successful [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.REQUEST.getPreferredName());
                assertNotNull(response, "could not parse successful [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.RESPONSE.getPreferredName());
                return new Result.Success(request, response);

            case SIMULATED:
                assertNotNull(request, "could not parse simulated [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.REQUEST.getPreferredName());
                return new Result.Simulated(request);

            case THROTTLED:
                assertNotNull(reason, "could not parse throttled [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.REASON.getPreferredName());
                return new Throttled(TYPE, reason);

            default: // Failure
                assert status == Status.FAILURE : "unhandled action result status";
                assertNotNull(reason, "could not parse failure [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.REASON.getPreferredName());
                if (request != null || response != null) {
                    assertNotNull(request, "could not parse failure [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.REQUEST.getPreferredName());
                    assertNotNull(response, "could not parse failure [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, currentFieldName, Field.RESPONSE.getPreferredName());
                    return new Result.Failure(request, response, reason);
                }
                return new Action.Result.Failure(TYPE, reason);
        }
    }

    private static void assertNotNull(Object value, String message, Object... args) {
        if (value == null) {
            throw new WebhookActionException(message, args);
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
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.REQUEST.getPreferredName(), request, params)
                        .field(Field.RESPONSE.getPreferredName(), response, params);
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
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                super.xContentBody(builder, params);
                return builder.field(Field.REQUEST.getPreferredName(), request, params)
                        .field(Field.RESPONSE.getPreferredName(), response, params);
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
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.REQUEST.getPreferredName(), request, params);
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

    interface Field extends Action.Field {
        ParseField REQUEST = new ParseField("request");
        ParseField RESPONSE = new ParseField("response");
    }
}
