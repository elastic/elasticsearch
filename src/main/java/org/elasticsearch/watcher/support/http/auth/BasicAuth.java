/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http.auth;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.util.Locale;

/**
 */
public class BasicAuth extends HttpAuth {

    public static final String TYPE = "basic";

    private final String username;
    private final String password;

    private final String basicAuth;

    public BasicAuth(String username, String password) throws UnsupportedEncodingException {
        this.username = username;
        this.password = password;
        basicAuth = "Basic " + Base64.encodeBytes(String.format(Locale.ROOT, "%s:%s", username, password).getBytes("utf-8"));
    }

    public String type() {
        return TYPE;
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.USERNAME_FIELD.getPreferredName(), username);
        builder.field(Parser.PASSWORD_FIELD.getPreferredName(), password);
        return builder.endObject();
    }

    public void update(HttpURLConnection connection) {
        connection.setRequestProperty("Authorization", basicAuth);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicAuth basicAuth = (BasicAuth) o;

        if (!password.equals(basicAuth.password)) return false;
        if (!username.equals(basicAuth.username)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = username.hashCode();
        result = 31 * result + password.hashCode();
        return result;
    }

    public static class Parser implements HttpAuth.Parser<BasicAuth> {

        static final ParseField USERNAME_FIELD = new ParseField("username");
        static final ParseField PASSWORD_FIELD = new ParseField("password");

        public String type() {
            return TYPE;
        }

        public BasicAuth parse(XContentParser parser) throws IOException {
            String username = null;
            String password = null;

            String fieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (USERNAME_FIELD.getPreferredName().equals(fieldName)) {
                        username = parser.text();
                    } else if (PASSWORD_FIELD.getPreferredName().equals(fieldName)) {
                        password = parser.text();
                    } else {
                        throw new ElasticsearchParseException("unsupported field [" + fieldName + "]");
                    }
                } else {
                    throw new ElasticsearchParseException("unsupported token [" + token + "]");
                }
            }

            if (username == null) {
                throw new HttpAuthException("username is a required option");
            }
            if (password == null) {
                throw new HttpAuthException("password is a required option");
            }

            return new BasicAuth(username, password);
        }
    }
}
