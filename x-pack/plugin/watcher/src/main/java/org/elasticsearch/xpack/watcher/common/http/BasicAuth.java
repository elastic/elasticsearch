/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.common.secret.Secret;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherXContentParser;

import java.io.IOException;
import java.util.Objects;

public class BasicAuth implements ToXContentObject {

    public static final String TYPE = "basic";

    final String username;
    final Secret password;

    public BasicAuth(String username, char[] password) {
        this(username, new Secret(password));
    }

    public BasicAuth(String username, Secret password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicAuth other = (BasicAuth) o;

        return Objects.equals(username, other.username) && Objects.equals(password, other.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, password);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.USERNAME.getPreferredName(), username);
        // if the password is null, do not render it out, so we have the possibility to call toXContent when we want to update a watch
        // if the password is not null, ensure we never return the original password value, unless it is encrypted with the CryptoService
        if (password != null) {
            if (WatcherParams.hideSecrets(params) && password.value().startsWith(CryptoService.ENCRYPTED_TEXT_PREFIX) == false) {
                builder.field(Field.PASSWORD.getPreferredName(), WatcherXContentParser.REDACTED_PASSWORD);
            } else {
                builder.field(Field.PASSWORD.getPreferredName(), password.value());
            }
        }
        return builder.endObject();
    }

    public static BasicAuth parseInner(XContentParser parser) throws IOException {
        String username = null;
        Secret password = null;

        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (Field.USERNAME.getPreferredName().equals(fieldName)) {
                    username = parser.text();
                } else if (Field.PASSWORD.getPreferredName().equals(fieldName)) {
                    password = WatcherXContentParser.secretOrNull(parser);
                } else {
                    throw new ElasticsearchParseException("unsupported field [" + fieldName + "]");
                }
            } else {
                throw new ElasticsearchParseException("unsupported token [" + token + "]");
            }
        }

        if (username == null) {
            throw new ElasticsearchParseException("username is a required option");
        }

        return new BasicAuth(username, password);
    }

    public static BasicAuth parse(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        BasicAuth auth = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                auth = BasicAuth.parseInner(parser);
            }
        }
        return auth;
    }

    interface Field {
        ParseField USERNAME = new ParseField("username");
        ParseField PASSWORD = new ParseField("password");
    }
}
