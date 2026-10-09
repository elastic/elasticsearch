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
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY;

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
        return list.toArray(String[]::new);
    }

    /**
     * Adds information about the permissions that a background service will run as to the X-Content representation
     * of its configuration:
     * - If the permissions are based on a user's roles at the time the config was created then the list of these
     *   roles is added.
     * - If the permissions come from an API key then the ID and name of the API key are added.
     * - If the permissions come from a service account then the name of the service account is added, along with its
     *   assigned roles if it is user-managed rather than built-in.
     * - If the permissions come from a cloud API key or a cloud service account then its ID and assigned roles are
     *   added under a key naming the kind.
     * - If the permissions come from a cross cluster access subject then the querying cluster's API key is added
     *   together with the remote subject's own authorization, rendered by these same rules.
     * Any subject whose roles are capped by the cloud identity provider also reports the names of the capping roles,
     * because the assigned roles alone would overstate its permissions.
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
        Subject authenticationSubject;
        try {
            authenticationSubject = AuthenticationContextSerializer.decode(authKey).getEffectiveSubject();
        } catch (Exception e) {
            // The exception will have been logged by AuthenticationContextSerializer.decode() so don't log it again here.
            return;
        }
        builder.startObject("authorization");
        addSubjectInfo(builder, authenticationSubject);
        builder.endObject();
    }

    /**
     * Renders the UIAM cloud API key id as {@code authorization.cloud_api_key.id} — the shared GET-response
     * shape used by datafeeds and transforms during UIAM switchover. Only the id is emitted; the secret is
     * never included.
     */
    public static void addCloudApiKeyAuthorization(final XContentBuilder builder, final String cloudApiKeyId) throws IOException {
        builder.startObject("authorization");
        builder.startObject("cloud_api_key");
        builder.field("id", cloudApiKeyId);
        builder.endObject();
        builder.endObject();
    }

    private static void addSubjectInfo(XContentBuilder builder, Subject subject) throws IOException {
        switch (subject.getType()) {
            case USER -> {
                builder.array(User.Fields.ROLES.getPreferredName(), subject.getUser().roles());
                addCloudLimitedByRoles(builder, subject);
            }
            case API_KEY -> addApiKeyInfo(builder, subject);
            case SERVICE_ACCOUNT -> {
                builder.field("service_account", subject.getUser().principal());
                // A built-in account's privileges are fixed by its definition and resolved from the principal alone, so the
                // principal is the whole authorization. A user-managed account is instead authorized from the role names
                // snapshotted onto its user, which the principal does not reveal, so they have to be reported alongside it.
                if (subject.isUserManagedServiceAccount()) {
                    builder.array(User.Fields.ROLES.getPreferredName(), subject.getUser().roles());
                }
            }
            case CROSS_CLUSTER_ACCESS -> {
                builder.startObject("cross_cluster_access");
                {
                    addApiKeyInfo(builder, subject);
                    builder.startObject("remote_authorization");
                    final var innerAuthentication = (Authentication) subject.getMetadata().get(CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY);
                    assert innerAuthentication != null && false == innerAuthentication.isCrossClusterAccess();
                    addSubjectInfo(builder, innerAuthentication.getEffectiveSubject());
                    builder.endObject();
                }
                builder.endObject();
            }
            case CLOUD_API_KEY -> {
                builder.startObject("cloud_api_key");
                Map<String, Object> metadata = subject.getUser().metadata();
                builder.field("id", subject.getUser().principal());
                Object name = metadata.get(AuthenticationField.API_KEY_NAME_KEY);
                if (name instanceof String) {
                    builder.field("name", name);
                }
                builder.field("internal", metadata.get(AuthenticationField.API_KEY_INTERNAL_KEY));
                builder.array(User.Fields.ROLES.getPreferredName(), subject.getUser().roles());
                addCloudLimitedByRoles(builder, subject);
                builder.endObject();
            }
            case CLOUD_SERVICE_ACCOUNT -> {
                builder.startObject("cloud_service_account");
                builder.field("id", subject.getUser().principal());
                builder.array(User.Fields.ROLES.getPreferredName(), subject.getUser().roles());
                addCloudLimitedByRoles(builder, subject);
                builder.endObject();
            }
        }
    }

    /**
     * Reports the cloud identity provider's cap on a subject's assigned roles, when it has one. Authorization ANDs the
     * cap with the assigned roles, so listing only the latter would overstate what the background service can do.
     */
    private static void addCloudLimitedByRoles(XContentBuilder builder, Subject subject) throws IOException {
        final List<String> limitedByRoleNames = subject.getCloudLimitedByRoleNames();
        if (limitedByRoleNames != null) {
            builder.array(User.Fields.LIMITED_BY_ROLES.getPreferredName(), limitedByRoleNames.toArray(String[]::new));
        }
    }

    private static void addApiKeyInfo(XContentBuilder builder, Subject authenticationSubject) throws IOException {
        builder.startObject("api_key");
        Map<String, Object> metadata = authenticationSubject.getMetadata();
        builder.field("id", metadata.get(AuthenticationField.API_KEY_ID_KEY));
        Object name = metadata.get(AuthenticationField.API_KEY_NAME_KEY);
        if (name instanceof String) {
            builder.field("name", name);
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
