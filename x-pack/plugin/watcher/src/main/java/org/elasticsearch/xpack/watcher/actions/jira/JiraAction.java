/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.jira;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.notification.jira.JiraIssue;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class JiraAction implements Action {

    public static final String TYPE = "jira";

    @Nullable
    final String account;
    @Nullable
    final HttpProxy proxy;
    final Map<String, Object> fields;

    public JiraAction(@Nullable String account, Map<String, Object> fields, HttpProxy proxy) {
        this.account = account;
        this.fields = fields;
        this.proxy = proxy;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public String getAccount() {
        return account;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JiraAction that = (JiraAction) o;
        return Objects.equals(account, that.account) && Objects.equals(fields, that.fields) && Objects.equals(proxy, that.proxy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, fields, proxy);
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
        builder.field(Field.FIELDS.getPreferredName(), fields);
        return builder.endObject();
    }

    public static JiraAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String account = null;
        HttpProxy proxy = null;
        Map<String, Object> fields = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.ACCOUNT.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    account = parser.text();
                } else {
                    throw new ElasticsearchParseException(
                        "failed to parse [{}] action [{}/{}]. expected [{}] to be of type string, but " + "found [{}] instead",
                        TYPE,
                        watchId,
                        actionId,
                        Field.ACCOUNT.getPreferredName(),
                        token
                    );
                }
            } else if (Field.PROXY.match(currentFieldName, parser.getDeprecationHandler())) {
                proxy = HttpProxy.parse(parser);
            } else if (Field.FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    fields = parser.map();
                } catch (Exception e) {
                    throw new ElasticsearchParseException(
                        "failed to parse [{}] action [{}/{}]. failed to parse [{}] field",
                        e,
                        TYPE,
                        watchId,
                        actionId,
                        Field.FIELDS.getPreferredName()
                    );
                }
            } else {
                throw new ElasticsearchParseException(
                    "failed to parse [{}] action [{}/{}]. unexpected token [{}/{}]",
                    TYPE,
                    watchId,
                    actionId,
                    token,
                    currentFieldName
                );
            }
        }
        if (fields == null) {
            fields = Collections.emptyMap();
        }
        return new JiraAction(account, fields, proxy);
    }

    public static class Executed extends Action.Result {

        private final JiraIssue result;

        public Executed(JiraIssue result) {
            super(TYPE, result.successful() ? Status.SUCCESS : Status.FAILURE);
            this.result = result;
        }

        public JiraIssue getResult() {
            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(type, result, params);
        }
    }

    static class Simulated extends Action.Result {

        private final Map<String, Object> fields;

        protected Simulated(Map<String, Object> fields) {
            super(TYPE, Status.SIMULATED);
            this.fields = fields;
        }

        public Map<String, Object> getFields() {
            return fields;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject(type).field(Field.FIELDS.getPreferredName(), fields).endObject();
        }
    }

    public static class Builder implements Action.Builder<JiraAction> {

        final JiraAction action;

        public Builder(JiraAction action) {
            this.action = action;
        }

        @Override
        public JiraAction build() {
            return action;
        }
    }

    public static Builder builder(String account, Map<String, Object> fields) {
        return new Builder(new JiraAction(account, fields, null));
    }

    public interface Field {
        ParseField ACCOUNT = new ParseField("account");
        ParseField PROXY = new ParseField("proxy");
        ParseField FIELDS = new ParseField("fields");
    }
}
