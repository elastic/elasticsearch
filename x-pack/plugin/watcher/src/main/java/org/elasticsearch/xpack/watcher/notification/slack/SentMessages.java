/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.slack;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.notification.slack.message.SlackMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
        private static final ParseField TO = new ParseField("to");
        private static final ParseField MESSAGE = new ParseField("message");

        final String to;
        final SlackMessage message;
        @Nullable
        final HttpRequest request;
        @Nullable
        final HttpResponse response;
        @Nullable
        final Exception exception;

        public static SentMessage responded(String to, SlackMessage message, HttpRequest request, HttpResponse response) {
            return new SentMessage(to, message, request, response, null);
        }

        public static SentMessage error(String to, SlackMessage message, Exception e) {
            return new SentMessage(to, message, null, null, e);
        }

        private SentMessage(String to, SlackMessage message, HttpRequest request, HttpResponse response, Exception exception) {
            this.to = to;
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
            return response != null && RestStatus.isSuccessful(response.status());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(STATUS.getPreferredName(), isSuccess() ? "success" : "failure");
            if (isSuccess() == false) {
                if (request != null) {
                    if (WatcherParams.hideSecrets(params)) {
                        // this writes out the request to the byte array output stream with the correct excludes
                        // for slack
                        try (InputStream is = HttpRequest.filterToXContent(request, builder.contentType(), params, "path")) {
                            builder.rawField(REQUEST.getPreferredName(), is, builder.contentType());
                        }
                    } else {
                        builder.field(REQUEST.getPreferredName());
                        request.toXContent(builder, params);
                    }
                }
                if (response != null) {
                    builder.field(RESPONSE.getPreferredName());
                    response.toXContent(builder, params);
                }
                if (exception != null) {
                    ElasticsearchException.generateFailureXContent(builder, params, exception, true);
                }
            }
            if (to != null) {
                builder.field(TO.getPreferredName(), to);
            }
            builder.field(MESSAGE.getPreferredName());
            message.toXContent(builder, params, false);
            return builder.endObject();
        }
    }
}
