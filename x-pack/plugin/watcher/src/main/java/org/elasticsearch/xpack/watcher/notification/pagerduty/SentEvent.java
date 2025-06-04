/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.actions.pagerduty.PagerDutyAction;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SentEvent implements ToXContentObject {

    final IncidentEvent event;
    @Nullable
    final HttpRequest request;
    @Nullable
    final HttpResponse response;
    @Nullable
    final String failureReason;

    public static SentEvent responded(IncidentEvent event, HttpRequest request, HttpResponse response) {
        String failureReason = resolveFailureReason(response);
        return new SentEvent(event, request, response, failureReason);
    }

    public static SentEvent error(IncidentEvent event, String reason) {
        return new SentEvent(event, null, null, reason);
    }

    private SentEvent(IncidentEvent event, HttpRequest request, HttpResponse response, String failureReason) {
        this.event = event;
        this.request = request;
        this.response = response;
        this.failureReason = failureReason;
    }

    public boolean successful() {
        return failureReason == null;
    }

    public HttpRequest getRequest() {
        return request;
    }

    public HttpResponse getResponse() {
        return response;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SentEvent sentEvent = (SentEvent) o;
        return Objects.equals(event, sentEvent.event)
            && Objects.equals(request, sentEvent.request)
            && Objects.equals(failureReason, sentEvent.failureReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, request, response, failureReason);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(XField.EVENT.getPreferredName(), event, params);
        if (successful() == false) {
            builder.field(XField.REASON.getPreferredName(), failureReason);
            if (request != null) {
                // this excludes the whole body, even though we should only exclude a small field inside of the body
                // as this makes debugging pagerduty services much harder, this should be changed to only filter for
                // body.service_key - however the body is currently just a string, making filtering much harder
                if (WatcherParams.hideSecrets(params)) {
                    try (InputStream is = HttpRequest.filterToXContent(request, builder.contentType(), params, "body")) {
                        builder.rawField(XField.REQUEST.getPreferredName(), is, builder.contentType());
                    }
                } else {
                    builder.field(XField.REQUEST.getPreferredName());
                    request.toXContent(builder, params);
                }
            }
            if (response != null) {
                builder.field(XField.RESPONSE.getPreferredName(), response, params);
            }
        }
        return builder.endObject();
    }

    private static String resolveFailureReason(HttpResponse response) {

        // if for some reason we failed to parse the body, lets fall back on the http status code.
        int status = response.status();
        if (status < 300) {
            return null;
        }

        // ok... we have an error

        // lets first try to parse the error response in the body
        // based on https://developer.pagerduty.com/documentation/rest/errors
        try (
            XContentParser parser = XContentHelper
                // EMPTY is safe here because we never call namedObject
                .createParserNotCompressed(LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG, response.body(), XContentType.JSON)
        ) {
            parser.nextToken();

            String message = null;
            List<String> errors = new ArrayList<>();

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (XField.MESSAGE.match(currentFieldName, parser.getDeprecationHandler())) {
                    message = parser.text();
                } else if (XField.CODE.match(currentFieldName, parser.getDeprecationHandler())) {
                    // we don't use this code.. so just consume the token
                } else if (XField.ERRORS.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        errors.add(parser.text());
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse pagerduty event response. unexpected field [{}]",
                        currentFieldName
                    );
                }
            }

            StringBuilder sb = new StringBuilder();
            if (message != null) {
                sb.append(message);
            }
            if (errors.isEmpty() == false) {
                sb.append(":");
                for (String error : errors) {
                    sb.append(" ").append(error).append(".");
                }
            }
            return sb.toString();
        } catch (Exception e) {
            // too bad... we couldn't parse the body... note that we don't log this error as there's no real
            // need for it. This whole error parsing is a nice to have, nothing more. On error, the http
            // response object is anyway added to the action result in the watch record (though not searchable)
        }

        return switch (status) {
            case 400 -> "Bad Request";
            case 401 -> "Unauthorized. The account service api key is invalid.";
            case 403 -> "Forbidden. The account doesn't have permission to send this trigger.";
            case 404 -> "The account used invalid HipChat APIs";
            case 408 -> "Request Timeout. The request took too long to process.";
            case 500 -> "PagerDuty Server Error. Internal error occurred while processing request.";
            default -> "Unknown Error";
        };
    }

    public interface XField {
        ParseField EVENT = PagerDutyAction.XField.EVENT;
        ParseField REASON = new ParseField("reason");
        ParseField REQUEST = new ParseField("request");
        ParseField RESPONSE = new ParseField("response");

        ParseField MESSAGE = new ParseField("message");
        ParseField CODE = new ParseField("code");
        ParseField ERRORS = new ParseField("errors");
    }
}
