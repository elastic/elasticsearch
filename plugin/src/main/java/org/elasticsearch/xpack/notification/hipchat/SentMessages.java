/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.hipchat;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.common.http.HttpRequest;
import org.elasticsearch.xpack.common.http.HttpResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class SentMessages implements ToXContentObject, Iterable<SentMessages.SentMessage> {

    private static final ParseField ACCOUNT = new ParseField("account");
    private static final ParseField SENT_MESSAGES = new ParseField("sent_messages");

    private String accountName;
    private List<SentMessage> messages;

    public SentMessages(String accountName, List<SentMessage> messages) {
        this.accountName = accountName;
        this.messages = messages;
    }

    public String getAccountName() {
        return accountName;
    }

    @Override
    public Iterator<SentMessage> iterator() {
        return messages.iterator();
    }

    public int count() {
        return messages.size();
    }

    public List<SentMessage> asList() {
        return Collections.unmodifiableList(messages);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACCOUNT.getPreferredName(), accountName);
        builder.startArray(SENT_MESSAGES.getPreferredName());
        for (SentMessage message : messages) {
            message.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    public static class SentMessage implements ToXContentObject {

        private static final ParseField STATUS = new ParseField("status");
        private static final ParseField REQUEST = new ParseField("request");
        private static final ParseField RESPONSE = new ParseField("response");
        private static final ParseField MESSAGE = new ParseField("message");

        public enum TargetType {
            ROOM, USER;

            final String fieldName = new String(name().toLowerCase(Locale.ROOT));
        }

        final String targetName;
        final TargetType targetType;
        final HipChatMessage message;
        @Nullable final HttpRequest request;
        @Nullable final HttpResponse response;
        @Nullable final Exception exception;

        public static SentMessage responded(String targetName, TargetType targetType, HipChatMessage message, HttpRequest request,
                                            HttpResponse response) {
            return new SentMessage(targetName, targetType, message, request, response, null);
        }

        public static SentMessage error(String targetName, TargetType targetType, HipChatMessage message, Exception e) {
            return new SentMessage(targetName, targetType, message, null, null, e);
        }

        private SentMessage(String targetName, TargetType targetType, HipChatMessage message, HttpRequest request, HttpResponse response,
                            Exception exception) {
            this.targetName = targetName;
            this.targetType = targetType;
            this.message = message;
            this.request = request;
            this.response = response;
            this.exception = exception;
        }

        public HttpRequest getRequest() {
            return request;
        }

        public HttpResponse getResponse() {
            return response;
        }

        public Exception getException() {
            return exception;
        }

        public boolean isSuccess() {
            return response != null && response.status() >= 200 && response.status() < 300;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(STATUS.getPreferredName(), isSuccess() ? "success" : "failure");
            if (isSuccess() == false) {
                builder.field(STATUS.getPreferredName(), "failure");
                if (request != null) {
                    builder.field(REQUEST.getPreferredName());
                    request.toXContent(builder, params);
                }
                if (response != null) {
                    builder.field(RESPONSE.getPreferredName());
                    response.toXContent(builder, params);
                }
                if (exception != null) {
                    ElasticsearchException.generateFailureXContent(builder, params, exception, true);
                }
            }
            builder.field(targetType.fieldName, targetName);
            builder.field(MESSAGE.getPreferredName());
            message.toXContent(builder, params, false);
            return builder.endObject();
        }
    }
}
