/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.hamcrest.Matchers.is;

public class DelegatePkiAuthenticationResponseTests extends AbstractXContentTestCase<DelegatePkiAuthenticationResponse> {

    public void testSerialization() throws Exception {
        DelegatePkiAuthenticationResponse response = createTestInstance();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                DelegatePkiAuthenticationResponse serialized = new DelegatePkiAuthenticationResponse(input);
                assertThat(response.getAccessToken(), is(serialized.getAccessToken()));
                assertThat(response.getExpiresIn(), is(serialized.getExpiresIn()));
                assertThat(response.getAuthentication(), is(serialized.getAuthentication()));
                assertThat(response, is(serialized));
            }
        }
    }

    @Override
    protected DelegatePkiAuthenticationResponse createTestInstance() {
        return new DelegatePkiAuthenticationResponse(
            randomAlphaOfLengthBetween(0, 10),
            TimeValue.parseTimeValue(randomTimeValue(), getClass().getSimpleName() + ".expiresIn"),
            createAuthentication()
        );
    }

    @Override
    protected DelegatePkiAuthenticationResponse doParseInstance(XContentParser parser) throws IOException {
        return DelegatePkiAuthenticationResponseTests.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    private static final ParseField ACCESS_TOKEN_FIELD = new ParseField("access_token");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField EXPIRES_IN_FIELD = new ParseField("expires_in");
    private static final ParseField AUTHENTICATION = new ParseField("authentication");

    public static final ConstructingObjectParser<DelegatePkiAuthenticationResponse, Void> PARSER = new ConstructingObjectParser<>(
        "delegate_pki_response",
        true,
        a -> {
            final String accessToken = (String) a[0];
            final String type = (String) a[1];
            if (false == "Bearer".equals(type)) {
                throw new IllegalArgumentException("Unknown token type [" + type + "], only [Bearer] type permitted");
            }
            final Long expiresIn = (Long) a[2];
            final Authentication authentication = (Authentication) a[3];

            return new DelegatePkiAuthenticationResponse(accessToken, TimeValue.timeValueSeconds(expiresIn), authentication);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ACCESS_TOKEN_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), EXPIRES_IN_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseAuthentication(p), AUTHENTICATION);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Authentication, Void> AUTH_PARSER = new ConstructingObjectParser<>(
        "authentication",
        true,
        a -> new Authentication(
            new User(
                (String) a[0],
                ((ArrayList<String>) a[1]).toArray(new String[0]),
                (String) a[2],
                (String) a[3],
                (Map<String, Object>) a[4],
                (boolean) a[5]
            ),
            (Authentication.RealmRef) a[6],
            (Authentication.RealmRef) a[7],
            Version.CURRENT,
            Authentication.AuthenticationType.valueOf(a[8].toString().toUpperCase(Locale.ROOT)),
            (Map<String, Object>) a[4]
        )
    );
    static {
        final ConstructingObjectParser<Authentication.RealmRef, Void> realmInfoParser = new ConstructingObjectParser<>(
            "realm_info",
            true,
            a -> new Authentication.RealmRef((String) a[0], (String) a[1], "node_name")
        );
        realmInfoParser.declareString(ConstructingObjectParser.constructorArg(), User.Fields.REALM_NAME);
        realmInfoParser.declareString(ConstructingObjectParser.constructorArg(), User.Fields.REALM_TYPE);
        AUTH_PARSER.declareString(ConstructingObjectParser.constructorArg(), User.Fields.USERNAME);
        AUTH_PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), User.Fields.ROLES);
        AUTH_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), User.Fields.FULL_NAME);
        AUTH_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), User.Fields.EMAIL);
        AUTH_PARSER.declareObject(ConstructingObjectParser.constructorArg(), (parser, c) -> parser.map(), User.Fields.METADATA);
        AUTH_PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), User.Fields.ENABLED);
        AUTH_PARSER.declareObject(ConstructingObjectParser.constructorArg(), realmInfoParser, User.Fields.AUTHENTICATION_REALM);
        AUTH_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), realmInfoParser, User.Fields.LOOKUP_REALM);
        AUTH_PARSER.declareString(ConstructingObjectParser.constructorArg(), User.Fields.AUTHENTICATION_TYPE);
    }

    public static Authentication parseAuthentication(final XContentParser parser) throws IOException {
        return AUTH_PARSER.apply(parser, null);
    }

    public static Authentication createAuthentication() {
        final String username = randomAlphaOfLengthBetween(1, 4);
        final String[] roles = generateRandomStringArray(4, 4, false, true);
        final Map<String, Object> metadata;
        metadata = new HashMap<>();
        if (randomBoolean()) {
            metadata.put("string", null);
        } else {
            metadata.put("string", randomAlphaOfLengthBetween(0, 4));
        }
        if (randomBoolean()) {
            metadata.put("string_list", null);
        } else {
            metadata.put("string_list", Arrays.asList(generateRandomStringArray(4, 4, false, true)));
        }
        final String fullName = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 4));
        final String email = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 4));
        final String authenticationRealmName = randomAlphaOfLength(5);
        final String authenticationRealmType = randomFrom("file", "native", "ldap", "active_directory", "saml", "kerberos");
        final String lookupRealmName = randomAlphaOfLength(5);
        final String lookupRealmType = randomFrom("file", "native", "ldap", "active_directory", "saml", "kerberos");
        final String nodeName = "node_name";
        final Authentication.AuthenticationType authenticationType = randomFrom(Authentication.AuthenticationType.values());
        if (Authentication.AuthenticationType.API_KEY.equals(authenticationType)) {
            metadata.put(AuthenticationField.API_KEY_ID_KEY, randomAlphaOfLengthBetween(1, 10));
            metadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10));
        }
        return new Authentication(
            new User(username, roles, fullName, email, metadata, true),
            new Authentication.RealmRef(authenticationRealmName, authenticationRealmType, nodeName),
            new Authentication.RealmRef(lookupRealmName, lookupRealmType, nodeName),
            Version.CURRENT,
            authenticationType,
            metadata
        );
    }
}
