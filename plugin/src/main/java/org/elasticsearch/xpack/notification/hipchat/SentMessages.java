/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.hipchat;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
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
        builder.field(Field.ACCOUNT, accountName);
        builder.startArray(Field.SENT_MESSAGES);
        for (SentMessage message : messages) {
            message.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    public static class SentMessage implements ToXContent {

        public enum TargetType {
            ROOM, USER;

            final String fieldName = new String(name().toLowerCase(Locale.ROOT));
        }

        final String targetName;
        final TargetType targetType;
        final HipChatMessage message;
        @Nullable final HttpRequest request;
        @Nullable final HttpResponse response;
        @Nullable final String failureReason;

        public static SentMessage responded(String targetName, TargetType targetType, HipChatMessage message, HttpRequest request,
                                            HttpResponse response) {
            String failureReason = resolveFailureReason(response);
            return new SentMessage(targetName, targetType, message, request, response, failureReason);
        }

        public static SentMessage error(String targetName, TargetType targetType, HipChatMessage message, String reason) {
            return new SentMessage(targetName, targetType, message, null, null, reason);
        }

        private SentMessage(String targetName, TargetType targetType, HipChatMessage message, HttpRequest request, HttpResponse response,
                            String failureReason) {
            this.targetName = targetName;
            this.targetType = targetType;
            this.message = message;
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

        public String getFailureReason() {
            return failureReason;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (failureReason != null) {
                builder.field(Field.STATUS, "failure");
                builder.field(Field.REASON, failureReason);
                if (request != null) {
                    builder.field(Field.REQUEST);
                    request.toXContent(builder, params);
                }
                if (response != null) {
                    builder.field(Field.RESPONSE);
                    response.toXContent(builder, params);
                }
            } else {
                builder.field(Field.STATUS, "success");
            }
            builder.field(targetType.fieldName, targetName);
            builder.field(Field.MESSAGE);
            message.toXContent(builder, params, false);
            return builder.endObject();
        }

        private static String resolveFailureReason(HttpResponse response) {
            int status = response.status();
            if (status < 300) {
                return null;
            }
            switch (status) {
                case 400:   return "Bad Request";
                case 401:   return "Unauthorized. The provided authentication token is invalid.";
                case 403:   return "Forbidden. The account doesn't have permission to send this message.";
                case 404:   // Not Found
                case 405:   // Method Not Allowed
                case 406:   return "The account used invalid HipChat APIs"; // Not Acceptable
                case 503:
                case 500:   return "HipChat Server Error.";
                default:
                    return "Unknown Error";
            }
        }
    }

    interface Field {
        String ACCOUNT = new String("account");
        String SENT_MESSAGES = new String("sent_messages");
        String STATUS = new String("status");
        String REASON = new String("reason");
        String REQUEST = new String("request");
        String RESPONSE = new String("response");
        String MESSAGE = new String("message");
    }
}
