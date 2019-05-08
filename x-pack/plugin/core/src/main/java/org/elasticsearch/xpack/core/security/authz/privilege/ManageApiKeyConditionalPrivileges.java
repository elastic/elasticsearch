/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public final class ManageApiKeyConditionalPrivileges implements ConditionalClusterPrivilege {

    private static final String CREATE_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/create";
    private static final String GET_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/get";
    private static final String INVALIDATE_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/invalidate";

    public static final String WRITEABLE_NAME = "manage-api-key-privileges";

    private final Set<String> realms;
    private final Predicate<String> realmsPredicate;
    private final Set<String> users;
    private final Predicate<String> usersPredicate;
    private final ClusterPrivilege privilege;
    private final BiPredicate<TransportRequest, Authentication> requestPredicate;

    interface Fields {
        ParseField MANAGE = new ParseField("manage");

        ParseField CREATE = new ParseField("create");
        ParseField GET = new ParseField("get");
        ParseField INVALIDATE = new ParseField("invalidate");

        ParseField USERS = new ParseField("users");
        ParseField REALMS = new ParseField("realms");
    }

    public ManageApiKeyConditionalPrivileges(boolean createAllowed, boolean getAllowed, boolean invalidateAllowed, Set<String> realms,
            Set<String> users) {
        final Set<String> patterns = new HashSet<>();
        if (createAllowed) {
            patterns.add(CREATE_API_KEY_PATTERN);
        }
        if (getAllowed) {
            patterns.add(GET_API_KEY_PATTERN);
        }
        if (invalidateAllowed) {
            patterns.add(INVALIDATE_API_KEY_PATTERN);
        }
        this.privilege = ClusterPrivilege.get(patterns);

        this.realms = (realms == null) ? Collections.emptySet() : Set.copyOf(realms);
        this.realmsPredicate = Automatons.predicate(this.realms);
        this.users = (users == null) ? Collections.emptySet() : Set.copyOf(users);
        this.usersPredicate = Automatons.predicate(this.users);

        this.requestPredicate = (request, authentication) -> {
            if (request instanceof CreateApiKeyRequest && privilege.predicate().test(CREATE_API_KEY_PATTERN)) {
                return true;
            } else if (request instanceof GetApiKeyRequest && privilege.predicate().test(GET_API_KEY_PATTERN)) {
                final GetApiKeyRequest getApiKeyRequest = (GetApiKeyRequest) request;
                if (this.realms.isEmpty() && this.users.isEmpty()) {
                    return checkIfUserIsOwnerOfApiKeys(authentication, getApiKeyRequest.getApiKeyId(), getApiKeyRequest.getUserName());
                } else {
                    return checkIfAccessAllowed(realms, getApiKeyRequest.getRealmName(), realmsPredicate)
                            && checkIfAccessAllowed(users, getApiKeyRequest.getUserName(), usersPredicate);
                }
            } else if (request instanceof InvalidateApiKeyRequest && privilege.predicate().test(INVALIDATE_API_KEY_PATTERN)) {
                final InvalidateApiKeyRequest invalidateApiKeyRequest = (InvalidateApiKeyRequest) request;
                if (this.realms.isEmpty() && this.users.isEmpty()) {
                    return checkIfUserIsOwnerOfApiKeys(authentication, invalidateApiKeyRequest.getId(),
                            invalidateApiKeyRequest.getUserName());
                } else {
                    return checkIfAccessAllowed(realms, invalidateApiKeyRequest.getRealmName(), realmsPredicate)
                            && checkIfAccessAllowed(users, invalidateApiKeyRequest.getUserName(), usersPredicate);
                }
            }
            return false;
        };
    }

    private boolean checkIfUserIsOwnerOfApiKeys(Authentication authentication, String apiKeyId, String username) {
        if (authentication.getAuthenticatedBy().getType().equals("_es_api_key")) {
            // API key id from authentication must match the id from request
            String authenticatedApiKeyId = (String) authentication.getMetadata().get("_security_api_key_id");
            if (Strings.hasText(apiKeyId)) {
                return apiKeyId.equals(authenticatedApiKeyId);
            }
        } else {
            String authenticatedUserPrincipal = authentication.getUser().principal();
            if (Strings.hasText(username)) {
                return username.equals(authenticatedUserPrincipal);
            }
        }
        return false;
    }

    private static boolean checkIfAccessAllowed(Set<String> names, String requestName, Predicate<String> predicate) {
        return (Strings.hasText(requestName) == false) ? names.contains("*") : predicate.test(requestName);
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(privilege.predicate().test(CREATE_API_KEY_PATTERN));
        out.writeBoolean(privilege.predicate().test(GET_API_KEY_PATTERN));
        out.writeBoolean(privilege.predicate().test(INVALIDATE_API_KEY_PATTERN));
        out.writeCollection(this.realms, StreamOutput::writeString);
        out.writeCollection(this.users, StreamOutput::writeString);
    }

    public static ManageApiKeyConditionalPrivileges createFrom(StreamInput in) throws IOException {
        final boolean allowCreate = in.readBoolean();
        final boolean allowGet = in.readBoolean();
        final boolean allowInvalidate = in.readBoolean();
        final Set<String> realms = in.readSet(StreamInput::readString);
        final Set<String> users = in.readSet(StreamInput::readString);
        return new ManageApiKeyConditionalPrivileges(allowCreate, allowGet, allowInvalidate, realms, users);
    }

    @Override
    public Category getCategory() {
        return Category.API_KEYS;
    }

    @Override
    public ClusterPrivilege getPrivilege() {
        return privilege;
    }

    @Override
    public BiPredicate<TransportRequest, Authentication> getRequestPredicate() {
        return requestPredicate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(privilege, users, realms);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ManageApiKeyConditionalPrivileges that = (ManageApiKeyConditionalPrivileges) o;
        return Objects.equals(this.privilege, that.privilege) && Objects.equals(this.realms, that.realms)
                && Objects.equals(this.users, that.users);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field(Fields.MANAGE.getPreferredName(),
                Map.of(Fields.CREATE.getPreferredName(), privilege.predicate().test(CREATE_API_KEY_PATTERN), Fields.GET.getPreferredName(),
                        privilege.predicate().test(GET_API_KEY_PATTERN), Fields.INVALIDATE.getPreferredName(),
                        privilege.predicate().test(INVALIDATE_API_KEY_PATTERN), Fields.REALMS.getPreferredName(), this.realms,
                        Fields.USERS.getPreferredName(), this.users));

        return builder;
    }

    public static ManageApiKeyConditionalPrivileges parse(XContentParser parser) throws IOException {
        expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
        expectFieldName(parser, Fields.MANAGE);
        expectedToken(parser.nextToken(), parser, XContentParser.Token.START_OBJECT);

        boolean createAllowed = false;
        boolean getAllowed = false;
        boolean invalidateAllowed = false;
        String[] realms = null;
        String[] users = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            expectedToken(parser.currentToken(), parser, XContentParser.Token.FIELD_NAME);
            String fieldName = parser.currentName();
            if (Fields.CREATE.match(fieldName, parser.getDeprecationHandler())) {
                parser.nextToken();
                createAllowed = parser.booleanValue();
            } else if (Fields.GET.match(fieldName, parser.getDeprecationHandler())) {
                parser.nextToken();
                getAllowed = parser.booleanValue();
            } else if (Fields.INVALIDATE.match(fieldName, parser.getDeprecationHandler())) {
                parser.nextToken();
                invalidateAllowed = parser.booleanValue();
            } else if (Fields.REALMS.match(fieldName, parser.getDeprecationHandler())) {
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
                realms = XContentUtils.readStringArray(parser, false);
            } else if (Fields.USERS.match(fieldName, parser.getDeprecationHandler())) {
                expectedToken(parser.nextToken(), parser, XContentParser.Token.START_ARRAY);
                users = XContentUtils.readStringArray(parser, false);
            }

        }
        return new ManageApiKeyConditionalPrivileges(createAllowed, getAllowed, invalidateAllowed, Set.of(realms), Set.of(users));
    }

    private static void expectedToken(XContentParser.Token read, XContentParser parser, XContentParser.Token expected) {
        if (read != expected) {
            throw new XContentParseException(parser.getTokenLocation(),
                    "failed to parse privilege. expected [" + expected + "] but found [" + read + "] instead");
        }
    }

    private static void expectFieldName(XContentParser parser, ParseField... fields) throws IOException {
        final String fieldName = parser.currentName();
        if (Arrays.stream(fields).anyMatch(pf -> pf.match(fieldName, parser.getDeprecationHandler())) == false) {
            throw new XContentParseException(parser.getTokenLocation(),
                    "failed to parse privilege. expected " + (fields.length == 1 ? "field name" : "one of") + " ["
                            + Strings.arrayToCommaDelimitedString(fields) + "] but found [" + fieldName + "] instead");
        }
    }
}
