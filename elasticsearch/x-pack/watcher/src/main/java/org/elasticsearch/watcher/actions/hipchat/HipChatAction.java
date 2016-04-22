/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.hipchat;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.xpack.notification.hipchat.HipChatMessage;
import org.elasticsearch.xpack.notification.hipchat.SentMessages;
import org.elasticsearch.watcher.support.text.TextTemplate;

import java.io.IOException;

/**
 *
 */
public class HipChatAction implements Action {

    public static final String TYPE = "hipchat";

    final @Nullable String account;
    final HipChatMessage.Template message;

    public HipChatAction(@Nullable String account, HipChatMessage.Template message) {
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

        HipChatAction that = (HipChatAction) o;

        if (!account.equals(that.account)) return false;
        return message.equals(that.message);
    }

    @Override
    public int hashCode() {
        int result = account.hashCode();
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

    public static HipChatAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String account = null;
        HipChatMessage.Template message = null;

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
                    message = HipChatMessage.Template.parse(parser);
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

        return new HipChatAction(account, message);
    }

    public static Builder builder(String account, TextTemplate body) {
        return new Builder(account, body);
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

            private final HipChatMessage message;

            protected Simulated(HipChatMessage message) {
                super(TYPE, Status.SIMULATED);
                this.message = message;
            }

            public HipChatMessage getMessage() {
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

    public static class Builder implements Action.Builder<HipChatAction> {

        final String account;
        final HipChatMessage.Template.Builder messageBuilder;

        public Builder(String account, TextTemplate body) {
            this.account = account;
            this.messageBuilder = new HipChatMessage.Template.Builder(body);
        }

        public Builder addRooms(TextTemplate... rooms) {
            messageBuilder.addRooms(rooms);
            return this;
        }

        public Builder addRooms(TextTemplate.Builder... rooms) {
            TextTemplate[] templates = new TextTemplate[rooms.length];
            for (int i = 0; i < rooms.length; i++) {
                templates[i] = rooms[i].build();
            }
            return addRooms(templates);
        }

        public Builder addRooms(String... rooms) {
            TextTemplate[] templates = new TextTemplate[rooms.length];
            for (int i = 0; i < rooms.length; i++) {
                templates[i] = TextTemplate.inline(rooms[i]).build();
            }
            return addRooms(templates);
        }


        public Builder addUsers(TextTemplate... users) {
            messageBuilder.addUsers(users);
            return this;
        }

        public Builder addUsers(TextTemplate.Builder... users) {
            TextTemplate[] templates = new TextTemplate[users.length];
            for (int i = 0; i < users.length; i++) {
                templates[i] = users[i].build();
            }
            return addUsers(templates);
        }

        public Builder addUsers(String... users) {
            TextTemplate[] templates = new TextTemplate[users.length];
            for (int i = 0; i < users.length; i++) {
                templates[i] = TextTemplate.inline(users[i]).build();
            }
            return addUsers(templates);
        }

        public Builder setFrom(String from) {
            messageBuilder.setFrom(from);
            return this;
        }

        public Builder setFormat(HipChatMessage.Format format) {
            messageBuilder.setFormat(format);
            return this;
        }

        public Builder setColor(TextTemplate color) {
            messageBuilder.setColor(color);
            return this;
        }

        public Builder setColor(TextTemplate.Builder color) {
            return setColor(color.build());
        }

        public Builder setColor(HipChatMessage.Color color) {
            return setColor(color.asTemplate());
        }

        public Builder setNotify(boolean notify) {
            messageBuilder.setNotify(notify);
            return this;
        }

        @Override
        public HipChatAction build() {
            return new HipChatAction(account, messageBuilder.build());
        }
    }

    public interface Field {
        ParseField ACCOUNT = new ParseField("account");
        ParseField MESSAGE = new ParseField("message");
    }
}
