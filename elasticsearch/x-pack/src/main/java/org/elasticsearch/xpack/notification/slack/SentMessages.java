/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.slack;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.xpack.notification.slack.message.SlackMessage;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class SentMessages implements ToXContent, Iterable<SentMessages.SentMessage> {

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

        final String to;
        final SlackMessage message;
        final @Nullable HttpRequest request;
        final @Nullable HttpResponse response;
        final @Nullable String failureReason;

        public static SentMessage responded(String to, SlackMessage message, HttpRequest request, HttpResponse response) {
            String failureReason = resolveFailureReason(response);
            return new SentMessage(to, message, request, response, failureReason);
        }

        public static SentMessage error(String to, SlackMessage message, String reason) {
            return new SentMessage(to, message, null, null, reason);
        }

        private SentMessage(String to, SlackMessage message, HttpRequest request, HttpResponse response, String failureReason) {
            this.to = to;
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
            if (to != null) {
                builder.field(Field.TO, to);
            }
            builder.field(Field.MESSAGE);
            message.toXContent(builder, params, false);
            return builder.endObject();
        }

        private static String resolveFailureReason(HttpResponse response) {
            int status = response.status();
            if (status < 300) {
                return null;
            }
            if (status > 399 && status < 500) {
                return "Bad Request";
            }
            if (status > 499) {
                return "Slack Server Error";
            }
            return "Unknown Error";
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
        String TO = new String("to");
    }
}
