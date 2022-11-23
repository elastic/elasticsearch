/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.xcontent;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XContentUtils {

    private XContentUtils() {}

    /**
     * Ensures that we're currently on the start of an object, or that the next token is a start of an object.
     *
     * @throws ElasticsearchParseException if the current or the next token is a {@code START_OBJECT}
     */
    public static void verifyObject(XContentParser parser) throws IOException, ElasticsearchParseException {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return;
        }
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("expected an object, but found token [{}]", parser.currentToken());
        }
    }

    public static String[] readStringArray(XContentParser parser, boolean allowNull) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            if (allowNull) {
                return null;
            }
            throw new ElasticsearchParseException(
                "could not parse [{}] field. expected a string array but found null value instead",
                parser.currentName()
            );
        }
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException(
                "could not parse [{}] field. expected a string array but found [{}] value instead",
                parser.currentName(),
                parser.currentToken()
            );
        }

        List<String> list = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.VALUE_STRING) {
                list.add(parser.text());
            } else {
                throw new ElasticsearchParseException(
                    "could not parse [{}] field. expected a string array but one of the value in the " + "array is [{}]",
                    parser.currentName(),
                    token
                );
            }
        }
        return list.toArray(new String[list.size()]);
    }

    /**
     * Adds information about the permissions that a background service will run as to the X-Content representation
     * of its configuration:
     * - If the permissions are based on a user's roles at the time the config was created then the list of these
     *   roles is added.
     * - If the permissions come from an API key then the ID and name of the API key are added.
     * - If the permissions come from a service account then the name of the service account is added.
     * @param builder The {@link XContentBuilder} that the extra fields will be added to.
     * @param headers Security headers that were stored to determine which permissions a background service
     *                will run as. If <code>null</code> or no authentication key entry is present then no
     *                fields are added.
     */
    public static void addAuthorizationInfo(final XContentBuilder builder, final Map<String, String> headers) throws IOException {
        if (headers == null) {
            return;
        }
        String authKey = headers.get(AuthenticationField.AUTHENTICATION_KEY);
        if (authKey == null) {
            return;
        }
        builder.startObject("authorization");
        Subject authenticationSubject = AuthenticationContextSerializer.decode(authKey).getEffectiveSubject();
        switch (authenticationSubject.getType()) {
            case USER -> builder.array(User.Fields.ROLES.getPreferredName(), authenticationSubject.getUser().roles());
            case API_KEY -> {
                builder.startObject("api_key");
                Map<String, Object> metadata = authenticationSubject.getMetadata();
                builder.field("id", metadata.get(AuthenticationField.API_KEY_ID_KEY));
                Object name = metadata.get(AuthenticationField.API_KEY_NAME_KEY);
                if (name != null) {
                    builder.field("name", name);
                }
                builder.endObject();
            }
            case SERVICE_ACCOUNT -> builder.field("service_account", authenticationSubject.getUser().principal());
        }
        builder.endObject();
    }

    public static void maybeAddErrorDetails(XContentBuilder builder, Map<String, Exception> errors) throws IOException {
        if (false == errors.isEmpty()) {
            builder.startObject("errors");
            {
                builder.field("count", errors.size());
                builder.startObject("details");
                for (Map.Entry<String, Exception> idWithException : errors.entrySet()) {
                    builder.startObject(idWithException.getKey());
                    ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, idWithException.getValue());
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }
}
