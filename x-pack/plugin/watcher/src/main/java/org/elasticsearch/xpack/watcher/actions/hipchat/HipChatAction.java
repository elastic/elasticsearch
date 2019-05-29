/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.hipchat;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatMessage;
import org.elasticsearch.xpack.watcher.notification.hipchat.SentMessages;

import java.io.IOException;
import java.util.Objects;

/**
 * @deprecated Hipchat actions will be removed in Elasticsearch 7.0 since Hipchat is defunct
 */
@Deprecated
public class HipChatAction implements Action {

    public static final String TYPE = "hipchat";

    @Nullable final String account;
    @Nullable final HttpProxy proxy;
    final HipChatMessage.Template message;

    public HipChatAction(@Nullable String account, HipChatMessage.Template message, @Nullable HttpProxy proxy) {
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

        HipChatAction that = (HipChatAction) o;

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

    public static HipChatAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String account = null;
        HipChatMessage.Template message = null;
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

        return new HipChatAction(account, message, proxy);
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
        private HttpProxy proxy;

        public Builder(String account, TextTemplate body) {
            this.account = account;
            this.messageBuilder = new HipChatMessage.Template.Builder(body);
        }

        public Builder addRooms(TextTemplate... rooms) {
            messageBuilder.addRooms(rooms);
            return this;
        }

        public Builder addRooms(String... rooms) {
            TextTemplate[] templates = new TextTemplate[rooms.length];
            for (int i = 0; i < rooms.length; i++) {
                templates[i] = new TextTemplate(rooms[i]);
            }
            return addRooms(templates);
        }


        public Builder addUsers(TextTemplate... users) {
            messageBuilder.addUsers(users);
            return this;
        }

        public Builder addUsers(String... users) {
            TextTemplate[] templates = new TextTemplate[users.length];
            for (int i = 0; i < users.length; i++) {
                templates[i] = new TextTemplate(users[i]);
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

        public Builder setColor(HipChatMessage.Color color) {
            return setColor(color.asTemplate());
        }

        public Builder setNotify(boolean notify) {
            messageBuilder.setNotify(notify);
            return this;
        }

        public Builder setProxy(HttpProxy proxy) {
            this.proxy = proxy;
            return this;
        }

        @Override
        public HipChatAction build() {
            return new HipChatAction(account, messageBuilder.build(), proxy);
        }
    }

    public interface Field {
        ParseField ACCOUNT = new ParseField("account");
        ParseField MESSAGE = new ParseField("message");
        ParseField PROXY = new ParseField("proxy");
    }
}
