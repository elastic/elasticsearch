/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class AuthenticateResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            this::createTestInstance,
            this::toXContent,
            AuthenticateResponse::fromXContent)
            .supportsUnknownFields(true)
            //metadata  and token are a series of kv pairs, so we dont want to add random fields here for test equality
            .randomFieldsExcludeFilter(f -> f.startsWith("metadata") || f.equals("token"))
            .test();
    }

    public void testEqualsAndHashCode() {
        final AuthenticateResponse response = createTestInstance();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(response, this::copy,
            this::mutate);
    }

    protected AuthenticateResponse createTestInstance() {
        final String username = randomAlphaOfLengthBetween(1, 4);
        final List<String> roles = Arrays.asList(generateRandomStringArray(4, 4, false, true));
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
        final boolean enabled = randomBoolean();
        final String authenticationRealmName = randomAlphaOfLength(5);
        final String authenticationRealmType = randomFrom(
            "service_account");
        final AuthenticateResponse.RealmInfo authenticationRealm =
            new AuthenticateResponse.RealmInfo(authenticationRealmName, authenticationRealmType);

        final AuthenticateResponse.RealmInfo lookupRealm;
        final Map<String, Object> tokenInfo;
        if ("service_account".equals(authenticationRealmType)) {
            lookupRealm = authenticationRealm;
            tokenInfo = Map.of("name", randomAlphaOfLengthBetween(3, 8), "type", randomAlphaOfLengthBetween(3, 8));
        } else {
            final String lookupRealmName = randomAlphaOfLength(5);
            final String lookupRealmType = randomFrom("file", "native", "ldap", "active_directory", "saml", "kerberos");
            lookupRealm = new AuthenticateResponse.RealmInfo(lookupRealmName, lookupRealmType);
            tokenInfo = null;
        }

        final String authenticationType = randomFrom("realm", "api_key", "token", "anonymous", "internal");

        return new AuthenticateResponse(
            new User(username, roles, metadata, fullName, email), enabled, authenticationRealm,
            lookupRealm, authenticationType, tokenInfo);
    }

    private void toXContent(AuthenticateResponse response, XContentBuilder builder) throws IOException {
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    private AuthenticateResponse copy(AuthenticateResponse response) {
        final User originalUser = response.getUser();
        final User copyUser = new User(originalUser.getUsername(), originalUser.getRoles(), originalUser.getMetadata(),
            originalUser.getFullName(), originalUser.getEmail());
        return new AuthenticateResponse(copyUser, response.enabled(), response.getAuthenticationRealm(),
            response.getLookupRealm(), response.getAuthenticationType(), Map.copyOf(response.getToken()));
    }

    private AuthenticateResponse mutate(AuthenticateResponse response) {
        final User originalUser = response.getUser();
        switch (randomIntBetween(1, 10)) {
            case 1:
                return new AuthenticateResponse(new User(originalUser.getUsername() + "wrong", originalUser.getRoles(),
                    originalUser.getMetadata(), originalUser.getFullName(), originalUser.getEmail()), response.enabled(),
                    response.getAuthenticationRealm(), response.getLookupRealm(), response.getAuthenticationType(), response.getToken());
            case 2:
                final List<String> wrongRoles = new ArrayList<>(originalUser.getRoles());
                wrongRoles.add(randomAlphaOfLengthBetween(1, 4));
                return new AuthenticateResponse(new User(originalUser.getUsername(), wrongRoles, originalUser.getMetadata(),
                    originalUser.getFullName(), originalUser.getEmail()), response.enabled(), response.getAuthenticationRealm(),
                    response.getLookupRealm(), response.getAuthenticationType(), response.getToken());
            case 3:
                final Map<String, Object> wrongMetadata = new HashMap<>(originalUser.getMetadata());
                wrongMetadata.put("wrong_string", randomAlphaOfLengthBetween(0, 4));
                return new AuthenticateResponse(new User(originalUser.getUsername(), originalUser.getRoles(), wrongMetadata,
                    originalUser.getFullName(), originalUser.getEmail()), response.enabled(), response.getAuthenticationRealm(),
                    response.getLookupRealm(), response.getAuthenticationType(), response.getToken());
            case 4:
                return new AuthenticateResponse(new User(originalUser.getUsername(), originalUser.getRoles(), originalUser.getMetadata(),
                    originalUser.getFullName() + "wrong", originalUser.getEmail()), response.enabled(),
                    response.getAuthenticationRealm(), response.getLookupRealm(), response.getAuthenticationType());
            case 5:
                return new AuthenticateResponse(new User(originalUser.getUsername(), originalUser.getRoles(), originalUser.getMetadata(),
                    originalUser.getFullName(), originalUser.getEmail() + "wrong"), response.enabled(),
                    response.getAuthenticationRealm(), response.getLookupRealm(), response.getAuthenticationType(), response.getToken());
            case 6:
                return new AuthenticateResponse(new User(originalUser.getUsername(), originalUser.getRoles(), originalUser.getMetadata(),
                    originalUser.getFullName(), originalUser.getEmail()), response.enabled() == false, response.getAuthenticationRealm(),
                    response.getLookupRealm(), response.getAuthenticationType(), response.getToken());
            case 7:
                return new AuthenticateResponse(new User(originalUser.getUsername(), originalUser.getRoles(), originalUser.getMetadata(),
                    originalUser.getFullName(), originalUser.getEmail()), response.enabled(), response.getAuthenticationRealm(),
                    new AuthenticateResponse.RealmInfo(randomAlphaOfLength(5), randomAlphaOfLength(5)),
                    response.getAuthenticationType(), response.getToken());
            case 8:
                return new AuthenticateResponse(new User(originalUser.getUsername(), originalUser.getRoles(), originalUser.getMetadata(),
                    originalUser.getFullName(), originalUser.getEmail()), response.enabled(),
                    new AuthenticateResponse.RealmInfo(randomAlphaOfLength(5), randomAlphaOfLength(5)), response.getLookupRealm(),
                    response.getAuthenticationType(), response.getToken());
            case 9:
                return new AuthenticateResponse(new User(originalUser.getUsername(), originalUser.getRoles(), originalUser.getMetadata(),
                    originalUser.getFullName(), originalUser.getEmail()), response.enabled(), response.getAuthenticationRealm(),
                    response.getLookupRealm(),
                    randomValueOtherThan(response.getAuthenticationType(),
                                         () -> randomFrom("realm", "api_key", "token", "anonymous", "internal")), response.getToken());
            default:
                return new AuthenticateResponse(new User(originalUser.getUsername(), originalUser.getRoles(), originalUser.getMetadata(),
                    originalUser.getFullName(), originalUser.getEmail()), response.enabled(), response.getAuthenticationRealm(),
                    response.getLookupRealm(),
                    response.getAuthenticationType(),
                    response.getToken() == null ?
                        Map.of("foo", "bar") :
                        randomFrom(Map.of(
                            "name", randomValueOtherThan(response.getToken().get("name"), () -> randomAlphaOfLengthBetween(3, 8)),
                            "type", randomValueOtherThan(response.getToken().get("type"), () -> randomAlphaOfLengthBetween(3, 8))
                        ), null));

        }
    }
}
