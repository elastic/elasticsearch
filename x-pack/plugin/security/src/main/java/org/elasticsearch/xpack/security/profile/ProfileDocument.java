/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public record ProfileDocument(
    String uid,
    boolean enabled,
    long lastSynchronized,
    ProfileDocumentUser user,
    Access access,
    BytesReference applicationData
) implements ToXContentObject {

    public record ProfileDocumentUser(
        String username,
        Authentication.RealmRef realm,
        String email,
        String fullName,
        String displayName,
        boolean active
    ) implements ToXContent {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("user");
            builder.field("username", username);
            builder.startObject("realm");
            builder.field("name", realm.getName());
            builder.field("type", realm.getType());
            builder.field("node_name", realm.getNodeName());
            builder.endObject();
            if (email != null) {
                builder.field("email", email);
            }
            if (fullName != null) {
                builder.field("full_name", fullName);
            }
            if (displayName != null) {
                builder.field("display_name", displayName);
            }
            builder.field("active", active);
            builder.endObject();
            return builder;
        }

        public Profile.ProfileUser toProfileUser(@Nullable String realmDomain) {
            return new Profile.ProfileUser(username, realm.getName(), realmDomain, email, fullName, displayName, active);
        }
    }

    public record Access(List<String> roles, Map<String, Object> applications) implements ToXContent {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("access");
            builder.field("roles", roles);
            builder.field("applications", applications);
            builder.endObject();
            return builder;
        }

        public Profile.Access toProfileAccess() {
            return new Profile.Access(roles, applications);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("uid", uid);
        builder.field("enabled", enabled);
        builder.field("last_synchronized", lastSynchronized);
        user.toXContent(builder, params);
        access.toXContent(builder, params);
        if (applicationData != null) {
            builder.field("application_data", applicationData);
        } else {
            builder.startObject("application_data").endObject();
        }
        builder.endObject();
        return builder;
    }

    static ProfileDocument fromSubject(Subject subject) {
        final String uid = "u_" + UUIDs.randomBase64UUID();
        final User subjectUser = subject.getUser();
        return new ProfileDocument(
            uid,
            true,
            Instant.now().toEpochMilli(),
            new ProfileDocumentUser(
                subjectUser.principal(),
                subject.getRealm(),
                subjectUser.email(),
                subjectUser.fullName(),
                null,
                subjectUser.enabled()
            ),
            new Access(List.of(subjectUser.roles()), Map.of()),
            null
        );
    }

    public static ProfileDocument fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    static final ConstructingObjectParser<ProfileDocumentUser, Void> PROFILE_USER_PARSER = new ConstructingObjectParser<>(
        "user_profile_document_user",
        false,
        (args, v) -> new ProfileDocumentUser(
            (String) args[0],
            (Authentication.RealmRef) args[1],
            (String) args[2],
            (String) args[3],
            (String) args[4],
            (Boolean) args[5]
        )
    );

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Access, Void> ACCESS_PARSER = new ConstructingObjectParser<>(
        "user_profile_document_access",
        false,
        (args, v) -> new Access((List<String>) args[0], (Map<String, Object>) args[1])
    );

    static final ConstructingObjectParser<ProfileDocument, Void> PROFILE_PARSER = new ConstructingObjectParser<>(
        "user_profile_document",
        false,
        (args, v) -> new ProfileDocument(
            (String) args[0],
            (boolean) args[1],
            (long) args[2],
            (ProfileDocumentUser) args[3],
            (Access) args[4],
            (BytesReference) args[5]
        )
    );

    static final ConstructingObjectParser<ProfileDocument, Void> PARSER = new ConstructingObjectParser<>(
        "user_profile_document_container",
        true,
        (args, v) -> (ProfileDocument) args[0]
    );

    static {
        PROFILE_USER_PARSER.declareString(constructorArg(), new ParseField("username"));
        PROFILE_USER_PARSER.declareObject(
            constructorArg(),
            (p, c) -> Authentication.REALM_REF_PARSER.parse(p, null),
            new ParseField("realm")
        );
        PROFILE_USER_PARSER.declareString(optionalConstructorArg(), new ParseField("email"));
        PROFILE_USER_PARSER.declareString(optionalConstructorArg(), new ParseField("full_name"));
        PROFILE_USER_PARSER.declareString(optionalConstructorArg(), new ParseField("display_name"));
        PROFILE_USER_PARSER.declareBoolean(constructorArg(), new ParseField("active"));
        ACCESS_PARSER.declareStringArray(constructorArg(), new ParseField("roles"));
        ACCESS_PARSER.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField("applications"));

        PROFILE_PARSER.declareString(constructorArg(), new ParseField("uid"));
        PROFILE_PARSER.declareBoolean(constructorArg(), new ParseField("enabled"));
        PROFILE_PARSER.declareLong(constructorArg(), new ParseField("last_synchronized"));
        PROFILE_PARSER.declareObject(constructorArg(), (p, c) -> PROFILE_USER_PARSER.parse(p, null), new ParseField("user"));
        PROFILE_PARSER.declareObject(constructorArg(), (p, c) -> ACCESS_PARSER.parse(p, null), new ParseField("access"));
        ObjectParserHelper<ProfileDocument, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(PROFILE_PARSER, constructorArg(), new ParseField("application_data"));

        PARSER.declareObject(constructorArg(), (p, c) -> PROFILE_PARSER.parse(p, null), new ParseField("user_profile"));
    }
}
