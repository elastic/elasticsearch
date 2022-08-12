/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.security.authc.Authentication.REALM_REF_PARSER;

public record ProfileDocument(
    String uid,
    boolean enabled,
    long lastSynchronized,
    ProfileDocumentUser user,
    Map<String, Object> labels,
    BytesReference applicationData
) implements ToXContentObject {

    public record ProfileDocumentUser(String username, List<String> roles, Authentication.RealmRef realm, String email, String fullName)
        implements
            ToXContent {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("user");
            builder.field("username", username);
            builder.field("roles", roles);
            builder.field("realm", realm);
            builder.field("email", email);
            builder.field("full_name", fullName);
            builder.endObject();
            return builder;
        }

        public Profile.ProfileUser toProfileUser() {
            final String domainName = realm.getDomain() != null ? realm.getDomain().name() : null;
            return new Profile.ProfileUser(username, roles, realm.getName(), domainName, email, fullName);
        }

        public Subject toSubject() {
            return new Subject(new User(username, roles.toArray(String[]::new), fullName, email, Map.of(), true), realm);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("uid", uid);
        builder.field("enabled", enabled);
        builder.field("last_synchronized", lastSynchronized);
        user.toXContent(builder, params);

        if (params.paramAsBoolean("include_labels", true) && labels != null) {
            builder.field("labels", labels);
        } else {
            builder.startObject("labels").endObject();
        }
        if (params.paramAsBoolean("include_data", true) && applicationData != null) {
            builder.field("application_data", applicationData);
        } else {
            builder.startObject("application_data").endObject();
        }
        builder.endObject();
        return builder;
    }

    static ProfileDocument fromSubject(Subject subject) {
        final String baseUid = computeBaseUidForSubject(subject);
        return fromSubjectWithUid(subject, baseUid + "_0"); // initial differentiator is 0
    }

    static ProfileDocument fromSubjectWithUid(Subject subject, String uid) {
        assert uid.startsWith(computeBaseUidForSubject(subject) + "_") || uid.startsWith("u_" + subject.getUser().principal() + "_");
        final User subjectUser = subject.getUser();
        return new ProfileDocument(
            uid,
            true,
            Instant.now().toEpochMilli(),
            new ProfileDocumentUser(
                subjectUser.principal(),
                Arrays.asList(subjectUser.roles()),
                subject.getRealm(),
                subjectUser.email(),
                subjectUser.fullName()
            ),
            Map.of(),
            null
        );
    }

    static String computeBaseUidForSubject(Subject subject) {
        final MessageDigest digest = MessageDigests.sha256();
        digest.update(subject.getUser().principal().getBytes(StandardCharsets.UTF_8));
        return "u_" + Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    }

    public static ProfileDocument fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ProfileDocumentUser, Void> PROFILE_DOC_USER_PARSER = new ConstructingObjectParser<>(
        "user_profile_document_user",
        false,
        (args, v) -> new ProfileDocumentUser(
            (String) args[0],
            (List<String>) args[1],
            (Authentication.RealmRef) args[2],
            (String) args[3],
            (String) args[4]
        )
    );

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ProfileDocument, Void> PROFILE_DOC_PARSER = new ConstructingObjectParser<>(
        "user_profile_document",
        false,
        (args, v) -> new ProfileDocument(
            (String) args[0],
            (boolean) args[1],
            (long) args[2],
            (ProfileDocumentUser) args[3],
            (Map<String, Object>) args[4],
            (BytesReference) args[5]
        )
    );

    static final ConstructingObjectParser<ProfileDocument, Void> PARSER = new ConstructingObjectParser<>(
        "user_profile_document_container",
        true,
        (args, v) -> (ProfileDocument) args[0]
    );

    static {
        PROFILE_DOC_USER_PARSER.declareString(constructorArg(), new ParseField("username"));
        PROFILE_DOC_USER_PARSER.declareStringArray(constructorArg(), new ParseField("roles"));
        PROFILE_DOC_USER_PARSER.declareObject(constructorArg(), (p, c) -> REALM_REF_PARSER.parse(p, c), new ParseField("realm"));
        PROFILE_DOC_USER_PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("email"));
        PROFILE_DOC_USER_PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("full_name"));

        PROFILE_DOC_PARSER.declareString(constructorArg(), new ParseField("uid"));
        PROFILE_DOC_PARSER.declareBoolean(constructorArg(), new ParseField("enabled"));
        PROFILE_DOC_PARSER.declareLong(constructorArg(), new ParseField("last_synchronized"));
        PROFILE_DOC_PARSER.declareObject(constructorArg(), (p, c) -> PROFILE_DOC_USER_PARSER.parse(p, null), new ParseField("user"));
        PROFILE_DOC_PARSER.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField("labels"));
        ObjectParserHelper.declareRawObject(PROFILE_DOC_PARSER, constructorArg(), new ParseField("application_data"));

        PARSER.declareObject(constructorArg(), (p, c) -> PROFILE_DOC_PARSER.parse(p, null), new ParseField("user_profile"));
    }
}
