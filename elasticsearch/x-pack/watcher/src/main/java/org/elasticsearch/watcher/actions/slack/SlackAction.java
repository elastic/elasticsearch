/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.slack;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.xpack.notification.slack.SentMessages;
import org.elasticsearch.xpack.notification.slack.message.SlackMessage;

import java.io.IOException;

/**
 *
 */
public class SlackAction implements Action {

    public static final String TYPE = "slack";

    final
    @Nullable
    String account;
    final SlackMessage.Template message;

    public SlackAction(@Nullable String account, SlackMessage.Template message) {
        this.account = account;
        this.message = message;
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

        if (account != null ? !account.equals(that.account) : that.account != null) return false;
        return message.equals(that.message);
    }

    @Override
    public int hashCode() {
        int result = account != null ? account.hashCode() : 0;
        result = 31 * result + message.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (account != null) {
            builder.field(Field.ACCOUNT.getPreferredName(), account);
        }
        builder.field(Field.MESSAGE.getPreferredName(), message);
        return builder.endObject();
    }

    public static SlackAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String account = null;
        SlackMessage.Template message = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.ACCOUNT)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    account = parser.text();
                } else {
                    throw new ElasticsearchParseException("failed to parse [{}] action [{}/{}]. expected [{}] to be of type string, but " +
                            "found [{}] instead", TYPE, watchId, actionId, Field.ACCOUNT.getPreferredName(), token);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.MESSAGE)) {
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

        return new SlackAction(account, message);
    }

    public static Builder builder(String account, SlackMessage.Template message) {
        return new Builder(new SlackAction(account, message));
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
                    if (message.successful()) {
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
    }
}
