/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.jira;

import org.apache.http.HttpStatus;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.actions.jira.JiraAction;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JiraIssue implements ToXContentObject {

    @Nullable final String account;
    private final Map<String, Object> fields;
    @Nullable private final HttpRequest request;
    @Nullable private final HttpResponse response;
    @Nullable private final String failureReason;

    public static JiraIssue responded(String account, Map<String, Object> fields, HttpRequest request, HttpResponse response) {
        return new JiraIssue(account, fields, request, response, resolveFailureReason(response));
    }

    JiraIssue(String account, Map<String, Object> fields, HttpRequest request, HttpResponse response, String failureReason) {
        this.account = account;
        this.fields = fields;
        this.request = request;
        this.response = response;
        this.failureReason = failureReason;
    }

    public boolean successful() {
        return failureReason == null;
    }

    public String getAccount() {
        return account;
    }

    public HttpRequest getRequest() {
        return request;
    }

    public HttpResponse getResponse() {
        return response;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public String getFailureReason() {
        return failureReason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JiraIssue issue = (JiraIssue) o;
        return Objects.equals(account, issue.account) &&
                Objects.equals(fields, issue.fields) &&
                Objects.equals(request, issue.request) &&
                Objects.equals(response, issue.response) &&
                Objects.equals(failureReason, issue.failureReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, fields, request, response, failureReason);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.ACCOUNT.getPreferredName(), account);
        if (fields != null) {
            builder.field(Field.FIELDS.getPreferredName(), fields);
        }
        if (successful() == false) {
            builder.field(Field.REASON.getPreferredName(), failureReason);
            if (request != null) {
                builder.field(Field.REQUEST.getPreferredName(), request, params);
            }
            if (response != null) {
                builder.field(Field.RESPONSE.getPreferredName(), response, params);
            }
        } else {
            try (InputStream stream = response.body().streamInput()) {
                builder.rawField(Field.RESULT.getPreferredName(), stream);
            }
        }
        return builder.endObject();
    }

    /**
     * Resolve the failure reason, when a reason can be extracted from the response body:
     * Ex:     {"errorMessages":[],"errors":{"customfield_10004":"Epic Name is required."}}
     * <p>
     * See https://docs.atlassian.com/jira/REST/cloud/ for the format of the error response body.
     */
    static String resolveFailureReason(HttpResponse response) {
        int status = response.status();
        if (status < 300) {
            return null;
        }

        StringBuilder message = new StringBuilder();
        switch (status) {
            case HttpStatus.SC_BAD_REQUEST:
                message.append("Bad Request");
                break;
            case HttpStatus.SC_UNAUTHORIZED:
                message.append("Unauthorized (authentication credentials are invalid)");
                break;
            case HttpStatus.SC_FORBIDDEN:
                message.append("Forbidden (account doesn't have permission to create this issue)");
                break;
            case HttpStatus.SC_NOT_FOUND:
                message.append("Not Found (account uses invalid JIRA REST APIs)");
                break;
            case HttpStatus.SC_REQUEST_TIMEOUT:
                message.append("Request Timeout (request took too long to process)");
                break;
            case HttpStatus.SC_INTERNAL_SERVER_ERROR:
                message.append("JIRA Server Error (internal error occurred while processing request)");
                break;
            default:
                message.append("Unknown Error");
                break;
        }

        if (response.hasContent()) {
            final List<String> errors = new ArrayList<>();
            // EMPTY is safe here because we never call namedObject
            try (InputStream stream = response.body().streamInput();
                 XContentParser parser = JsonXContent.jsonXContent
                         .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                XContentParser.Token token = parser.currentToken();
                if (token == null) {
                    token = parser.nextToken();
                }
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new ElasticsearchParseException("failed to parse jira project. expected an object, but found [{}] instead",
                            token);
                }
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (Field.ERRORS.match(currentFieldName, parser.getDeprecationHandler())) {
                        Map<String, Object> fieldErrors = parser.mapOrdered();
                        for (Map.Entry<String, Object> entry : fieldErrors.entrySet()) {
                            errors.add("Field [" + entry.getKey() + "] has error [" + String.valueOf(entry.getValue()) + "]");
                        }
                    } else if (Field.ERROR_MESSAGES.match(currentFieldName, parser.getDeprecationHandler())) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            errors.add(parser.text());
                        }
                    } else {
                        throw new ElasticsearchParseException("could not parse jira response. unexpected field [{}]", currentFieldName);
                    }
                }
            } catch (Exception e) {
                errors.add("Exception when parsing jira response [" + String.valueOf(e) + "]");
            }

            if (errors.isEmpty() == false) {
                message.append(" - ");
                for (String error : errors) {
                    message.append(error).append('\n');
                }
            }
        }
        return message.toString();
    }

    private interface Field {
        ParseField FIELDS = JiraAction.Field.FIELDS;
        ParseField ACCOUNT = new ParseField("account");
        ParseField REASON = new ParseField("reason");
        ParseField REQUEST = new ParseField("request");
        ParseField RESPONSE = new ParseField("response");
        ParseField RESULT = new ParseField("result");

        ParseField ERROR_MESSAGES = new ParseField("errorMessages");
        ParseField ERRORS = new ParseField("errors");
    }
}
