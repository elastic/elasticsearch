/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public record ProfileDocument(
    String uid,
    boolean enabled,
    long lastSynchronized,
    ProfileDocumentUser user,
    Access access,
    BytesReference applicationData
) {

    public record ProfileDocumentUser(String username, Authentication.RealmRef realm, String email, String fullName, String displayName) {

        public Profile.ProfileUser toProfileUser(@Nullable String realmDomain) {
            return new Profile.ProfileUser(username, realm.getName(), realmDomain, email, fullName, displayName);
        }
    }

    public record Access(List<String> roles, Map<String, Object> applications) {
        public Profile.Access toProfileAccess() {
            return new Profile.Access(roles, applications);
        }
    }

    public static ProfileDocument fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    static final ConstructingObjectParser<ProfileDocumentUser, Void> PROFILE_USER_PARSER = new ConstructingObjectParser<>(
        "profile_document_user",
        false,
        (args, v) -> new ProfileDocumentUser(
            (String) args[0],
            (Authentication.RealmRef) args[1],
            (String) args[2],
            (String) args[3],
            (String) args[4]
        )
    );

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Access, Void> ACCESS_PARSER = new ConstructingObjectParser<>(
        "profile_access",
        false,
        (args, v) -> new Access((List<String>) args[0], (Map<String, Object>) args[1])
    );

    static final ConstructingObjectParser<ProfileDocument, Void> PARSER = new ConstructingObjectParser<>(
        "profile_document",
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

    static {
        PROFILE_USER_PARSER.declareString(constructorArg(), new ParseField("username"));
        PROFILE_USER_PARSER.declareObject(
            constructorArg(),
            (p, c) -> Authentication.REALM_REF_PARSER.parse(p, null),
            new ParseField("realm")
        );
        PROFILE_USER_PARSER.declareString(constructorArg(), new ParseField("email"));
        PROFILE_USER_PARSER.declareString(constructorArg(), new ParseField("full_name"));
        PROFILE_USER_PARSER.declareString(constructorArg(), new ParseField("display_name"));
        ACCESS_PARSER.declareStringArray(constructorArg(), new ParseField("roles"));
        ACCESS_PARSER.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField("applications"));

        PARSER.declareString(constructorArg(), new ParseField("uid"));
        PARSER.declareBoolean(constructorArg(), new ParseField("enabled"));
        PARSER.declareLong(constructorArg(), new ParseField("last_synchronized"));
        PARSER.declareObject(constructorArg(), (p, c) -> PROFILE_USER_PARSER.parse(p, null), new ParseField("user"));
        PARSER.declareObject(constructorArg(), (p, c) -> ACCESS_PARSER.parse(p, null), new ParseField("access"));
        ObjectParserHelper<ProfileDocument, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(PARSER, constructorArg(), new ParseField("application_data"));
    }
}
