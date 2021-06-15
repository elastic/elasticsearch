/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.slack;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.notification.slack.SentMessages;
import org.elasticsearch.xpack.watcher.notification.slack.message.SlackMessage;

import java.io.IOException;
import java.util.Objects;

public class SlackAction implements Action {

    public static final String TYPE = "slack";

    final SlackMessage.Template message;
    @Nullable final String account;
    @Nullable final HttpProxy proxy;

    public SlackAction(@Nullable String account, SlackMessage.Template message, HttpProxy proxy) {
        this.account = account;
        this.message = message;
        this.proxy = proxy;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SlackAction that = (SlackAction) o;

        return Objects.equals(account, that.account) &&
               Objects.equals(message, that.message) &&
               Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, message, proxy);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (account != null) {
            builder.field(Field.ACCOUNT.getPreferredName(), account);
        }
        if (proxy != null) {
            proxy.toXContent(builder, params);
        }
        builder.field(Field.MESSAGE.getPreferredName(), message);
        return builder.endObject();
    }

    public static SlackAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String account = null;
        SlackMessage.Template message = null;
        HttpProxy proxy = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.ACCOUNT.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    account = parser.text();
                } else {
                    throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. expected [{}] to be of type string, but " +
                            "found [{}] instead", TYPE, watchId, actionId, Field.ACCOUNT.getPreferredName(), token);
                }
            } else if (Field.PROXY.match(currentFieldName, parser.getDeprecationHandler())) {
                proxy = HttpProxy.parse(parser);
            } else if (Field.MESSAGE.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    message = SlackMessage.Template.parse(parser);
                } catch (Exception e) {
                    throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. failed to parse [{}] field", e, TYPE,
                            watchId, actionId, Field.MESSAGE.getPreferredName());
                }
            } else {
                throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. unexpected token [{}]", TYPE, watchId,
                        actionId, token);
            }
        }

        if (message == null) {
            throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. missing required [{}] field", TYPE, watchId,
                    actionId, Field.MESSAGE.getPreferredName());
        }

        return new SlackAction(account, message, proxy);
    }

    public static Builder builder(String account, SlackMessage.Template message) {
        return new Builder(new SlackAction(account, message, null));
    }

    public interface Result {

        class Executed extends Action.Result implements Result {

            private final SentMessages sentMessages;

            public Executed(SentMessages sentMessages) {
                super(TYPE, status(sentMessages));
                this.sentMessages = sentMessages;
            }

            public SentMessages sentMessages() {
                return sentMessages;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.field(type, sentMessages, params);
            }

            static Status status(SentMessages sentMessages) {
                boolean hasSuccesses = false;
                boolean hasFailures = false;
                for (SentMessages.SentMessage message : sentMessages) {
                    if (message.isSuccess()) {
                        hasSuccesses = true;
                    } else {
                        hasFailures = true;
                    }
                    if (hasFailures && hasSuccesses) {
                        return Status.PARTIAL_FAILURE;
                    }
                }
                return hasFailures ? Status.FAILURE : Status.SUCCESS;
            }
        }

        class Simulated extends Action.Result implements Result {

            private final SlackMessage message;

            protected Simulated(SlackMessage message) {
                super(TYPE, Status.SIMULATED);
                this.message = message;
            }

            public SlackMessage getMessage() {
                return message;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject(type)
                        .field(Field.MESSAGE.getPreferredName(), message, params)
                        .endObject();
            }
        }
    }

    public static class Builder implements Action.Builder<SlackAction> {

        final SlackAction action;

        public Builder(SlackAction action) {
            this.action = action;
        }

        @Override
        public SlackAction build() {
            return action;
        }
    }

    public interface Field {
        ParseField ACCOUNT = new ParseField("account");
        ParseField MESSAGE = new ParseField("message");
        ParseField PROXY = new ParseField("proxy");
    }
}
