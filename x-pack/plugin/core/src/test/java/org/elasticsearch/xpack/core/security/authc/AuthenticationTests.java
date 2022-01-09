/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;

public class AuthenticationTests extends ESTestCase {

    public void testWillGetLookedUpByWhenItExists() {
        final RealmRef authenticatedBy = new RealmRef("auth_by", "auth_by_type", "node");
        final RealmRef lookedUpBy = new RealmRef("lookup_by", "lookup_by_type", "node");
        final Authentication authentication = new Authentication(new User("user"), authenticatedBy, lookedUpBy);

        assertEquals(lookedUpBy, authentication.getSourceRealm());
    }

    public void testWillGetAuthenticateByWhenLookupIsNull() {
        final RealmRef authenticatedBy = new RealmRef("auth_by", "auth_by_type", "node");
        final Authentication authentication = new Authentication(new User("user"), authenticatedBy, null);

        assertEquals(authenticatedBy, authentication.getSourceRealm());
    }

    public void testCanAccessResourcesOf() throws Exception {
        // Same user is the same
        final User user1 = randomUser();
        final RealmRef realm1 = randomRealm();
        checkCanAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm1));

        // Different username is different no matter which realm it is from
        final User user2 = randomValueOtherThanMany(u -> u.principal().equals(user1.principal()), AuthenticationTests::randomUser);
        // user 2 can be from either the same realm or a different realm
        final RealmRef realm2 = randomFrom(realm1, randomRealm());
        assertAccessResources(randomAuthentication(user1, realm2), randomAuthentication(user2, realm2), false);

        // Same username but different realm is different
        final RealmRef realm3;
        switch (randomIntBetween(0, 2)) {
            case 0: // change name
                realm3 = mutateRealm(realm1, randomAlphaOfLengthBetween(3, 8), null);
                if (realmIsSingleton(realm1)) {
                    checkCanAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm3));
                } else {
                    assertAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm3), false);
                }
                break;
            case 1: // change type
                realm3 = mutateRealm(realm1, null, randomAlphaOfLengthBetween(3, 8));
                assertAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm3), false);
                break;
            case 2: // both
                realm3 = mutateRealm(realm1, randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
                assertAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm3), false);
                break;
            default:
                assert false : "Case number out of range";
        }

        // User and its API key are not the same owner
        assertAccessResources(
            randomAuthentication(user1, realm1),
            randomApiKeyAuthentication(user1, randomAlphaOfLengthBetween(10, 20)),
            false
        );

        // Same API key ID are the same owner
        final String apiKeyId1 = randomAlphaOfLengthBetween(10, 20);
        checkCanAccessResources(randomApiKeyAuthentication(user1, apiKeyId1), randomApiKeyAuthentication(user1, apiKeyId1));

        // Two API keys (2 API key IDs) are not the same owner
        final String apiKeyId2 = randomValueOtherThan(apiKeyId1, () -> randomAlphaOfLengthBetween(10, 20));
        assertAccessResources(
            randomApiKeyAuthentication(randomFrom(user1, user2), apiKeyId1),
            randomApiKeyAuthentication(randomFrom(user1, user2), apiKeyId2),
            false
        );
    }

    public void testIsServiceAccount() {
        final User user = new User(
            randomAlphaOfLengthBetween(3, 8),
            randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8))
        );
        final Authentication.RealmRef authRealm;
        final boolean authRealmIsForServiceAccount = randomBoolean();
        if (authRealmIsForServiceAccount) {
            authRealm = new Authentication.RealmRef(
                ServiceAccountSettings.REALM_NAME,
                ServiceAccountSettings.REALM_TYPE,
                randomAlphaOfLengthBetween(3, 8)
            );
        } else {
            authRealm = new Authentication.RealmRef(
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8)
            );
        }
        final Authentication.RealmRef lookupRealm = randomFrom(
            new Authentication.RealmRef(
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8)
            ),
            null
        );
        final Authentication authentication = new Authentication(user, authRealm, lookupRealm);

        if (authRealmIsForServiceAccount) {
            assertThat(authentication.isAuthenticatedWithServiceAccount(), is(true));
        } else {
            assertThat(authentication.isAuthenticatedWithServiceAccount(), is(false));
        }
    }

    private void checkCanAccessResources(Authentication authentication0, Authentication authentication1) throws IOException {
        if (authentication0.getAuthenticationType() == authentication1.getAuthenticationType()
            || EnumSet.of(AuthenticationType.REALM, AuthenticationType.TOKEN)
                .equals(EnumSet.of(authentication0.getAuthenticationType(), authentication1.getAuthenticationType()))) {
            assertAccessResources(authentication0, authentication1, true);
        } else {
            assertAccessResources(authentication0, authentication1, false);
        }
    }

    private void assertAccessResources(Authentication authentication0, Authentication authentication1, boolean can) throws IOException {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(false)) {
            authentication0.writeToContext(threadContext);
            assertThat(securityContext.canIAccessResourcesCreatedBy(authentication1), is(can));
        }
        try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(false)) {
            authentication1.writeToContext(threadContext);
            assertThat(securityContext.canIAccessResourcesCreatedBy(authentication0), is(can));
        }
    }

    public static User randomUser() {
        return new User(randomAlphaOfLengthBetween(3, 8), randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
    }

    public static RealmRef randomRealm() {
        return new RealmRef(
            randomAlphaOfLengthBetween(3, 8),
            randomFrom(FileRealmSettings.TYPE, NativeRealmSettings.TYPE, randomAlphaOfLengthBetween(3, 8)),
            randomAlphaOfLengthBetween(3, 8)
        );
    }

    private RealmRef mutateRealm(RealmRef original, String name, String type) {
        return new RealmRef(
            name == null ? original.getName() : name,
            type == null ? original.getType() : type,
            randomBoolean() ? original.getNodeName() : randomAlphaOfLengthBetween(3, 8)
        );
    }

    public static Authentication randomAuthentication(User user, RealmRef realmRef) {
        if (user == null) {
            user = randomUser();
        }
        if (realmRef == null) {
            realmRef = randomRealm();
        }
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.CURRENT);
        final AuthenticationType authenticationType = randomValueOtherThan(
            AuthenticationType.API_KEY,
            () -> randomFrom(AuthenticationType.values())
        );
        final Map<String, Object> metadata;
        if (randomBoolean()) {
            metadata = Map.of(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        } else {
            metadata = Arrays.stream(randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)))
                .distinct()
                .collect(Collectors.toMap(s -> s, s -> randomAlphaOfLengthBetween(3, 8)));
        }
        if (randomBoolean()) { // run-as
            return new Authentication(
                new User(user.principal(), user.roles(), randomUser()),
                randomRealm(),
                realmRef,
                version,
                authenticationType,
                metadata
            );
        } else {
            return new Authentication(user, realmRef, null, version, authenticationType, metadata);
        }
    }

    public static Authentication randomApiKeyAuthentication(User user, String apiKeyId) {
        final RealmRef apiKeyRealm = new RealmRef("_es_api_key", "_es_api_key", randomAlphaOfLengthBetween(3, 8));
        final HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(AuthenticationField.API_KEY_ID_KEY, apiKeyId);
        metadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLengthBetween(1, 16));
        return new Authentication(
            user,
            apiKeyRealm,
            null,
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.CURRENT),
            AuthenticationType.API_KEY,
            metadata
        );
    }

    private boolean realmIsSingleton(RealmRef realmRef) {
        return Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE).contains(realmRef.getType());
    }
}
