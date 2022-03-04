/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response when requesting zero or more users.
 * Returns a List of {@link User} objects
 */
public class GetUsersResponse {

    private final Map<String, User> users;
    private final Map<String, User> enabledUsers;

    GetUsersResponse(final Map<String, User> users, final Map<String, User> enabledUsers) {
        this.users = Map.copyOf(users);
        this.enabledUsers = Map.copyOf(enabledUsers);
    }

    public List<User> getUsers() {
        return List.copyOf(users.values());
    }

    public List<User> getEnabledUsers() {
        return List.copyOf(enabledUsers.values());
    }

    public static GetUsersResponse fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser);
        final List<User> users = new ArrayList<>();
        final List<User> enabledUsers = new ArrayList<>();
        Token token;
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(Token.FIELD_NAME, token, parser);
            ParsedUser parsedUser = USER_PARSER.parse(parser, parser.currentName());
            users.add(parsedUser.user);
            if (parsedUser.enabled) {
                enabledUsers.add(parsedUser.user);
            }
        }
        return new GetUsersResponse(toMap(users), toMap(enabledUsers));
    }

    static Map<String, User> toMap(final Collection<User> users) {
        return users.stream().collect(Collectors.toUnmodifiableMap(User::getUsername, Function.identity()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof GetUsersResponse) == false) return false;
        GetUsersResponse that = (GetUsersResponse) o;
        return Objects.equals(users, that.users);
    }

    @Override
    public int hashCode() {
        return Objects.hash(users);
    }

    public static final ParseField USERNAME = new ParseField("username");
    public static final ParseField ROLES = new ParseField("roles");
    public static final ParseField FULL_NAME = new ParseField("full_name");
    public static final ParseField EMAIL = new ParseField("email");
    public static final ParseField METADATA = new ParseField("metadata");
    public static final ParseField ENABLED = new ParseField("enabled");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ParsedUser, String> USER_PARSER = new ConstructingObjectParser<>(
        "user_info",
        true,
        (constructorObjects) -> {
            int i = 0;
            final String username = (String) constructorObjects[i++];
            final List<String> roles = (List<String>) constructorObjects[i++];
            final Map<String, Object> metadata = (Map<String, Object>) constructorObjects[i++];
            final Boolean enabled = (Boolean) constructorObjects[i++];
            final String fullName = (String) constructorObjects[i++];
            final String email = (String) constructorObjects[i++];
            return new ParsedUser(username, roles, metadata, enabled, fullName, email);
        }
    );

    static {
        USER_PARSER.declareString(constructorArg(), USERNAME);
        USER_PARSER.declareStringArray(constructorArg(), ROLES);
        USER_PARSER.declareObject(constructorArg(), (parser, c) -> parser.map(), METADATA);
        USER_PARSER.declareBoolean(constructorArg(), ENABLED);
        USER_PARSER.declareStringOrNull(optionalConstructorArg(), FULL_NAME);
        USER_PARSER.declareStringOrNull(optionalConstructorArg(), EMAIL);
    }

    protected static final class ParsedUser {
        protected User user;
        protected boolean enabled;

        public ParsedUser(
            String username,
            List<String> roles,
            Map<String, Object> metadata,
            Boolean enabled,
            @Nullable String fullName,
            @Nullable String email
        ) {
            String checkedUsername = Objects.requireNonNull(username, "`username` is required, cannot be null");
            List<String> checkedRoles = List.copyOf(
                Objects.requireNonNull(roles, "`roles` is required, cannot be null. Pass an empty list instead.")
            );
            Map<String, Object> checkedMetadata = Collections.unmodifiableMap(
                Objects.requireNonNull(metadata, "`metadata` is required, cannot be null. Pass an empty map instead.")
            );
            this.user = new User(checkedUsername, checkedRoles, checkedMetadata, fullName, email);
            this.enabled = enabled;
        }
    }
}
