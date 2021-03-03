/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.security.authz.privilege.ManageOwnApiKeyClusterPrivilege.API_KEY_ID_KEY;

public class AuthenticationTests extends ESTestCase {

    public void testWillGetLookedUpByWhenItExists() {
        final RealmRef authenticatedBy = new RealmRef("auth_by", "auth_by_type", "node");
        final RealmRef lookedUpBy = new RealmRef("lookup_by", "lookup_by_type", "node");
        final Authentication authentication = new Authentication(
            new User("user"), authenticatedBy, lookedUpBy);

        assertEquals(lookedUpBy, authentication.getSourceRealm());
    }

    public void testWillGetAuthenticateByWhenLookupIsNull() {
        final RealmRef authenticatedBy = new RealmRef("auth_by", "auth_by_type", "node");
        final Authentication authentication = new Authentication(
            new User("user"), authenticatedBy, null);

        assertEquals(authenticatedBy, authentication.getSourceRealm());
    }

    public void testSameUserAsForRealms() {
        final User user1 = randomUserOtherThan(new User[0]);
        final User user2 = randomUserOtherThan(user1);

        final RealmRef fileRealm1 = new RealmRef(
            randomAlphaOfLengthBetween(3, 8), FileRealmSettings.TYPE, randomAlphaOfLengthBetween(3, 8));
        final RealmRef fileRealm2 = new RealmRef(
            randomValueOtherThan(fileRealm1.getName(), () -> randomAlphaOfLengthBetween(3, 8)),
            FileRealmSettings.TYPE, randomAlphaOfLengthBetween(3, 8));

        final RealmRef nativeRealm1 = new RealmRef(
            randomAlphaOfLengthBetween(3, 8), NativeRealmSettings.TYPE, randomAlphaOfLengthBetween(3, 8));
        final RealmRef nativeRealm2 = new RealmRef(
            randomValueOtherThan(nativeRealm1.getName(), () -> randomAlphaOfLengthBetween(3, 8)),
            NativeRealmSettings.TYPE, randomAlphaOfLengthBetween(3, 8));

        final RealmRef realm1 = randomNonFileNativeRealmOtherThan(new RealmRef[0]);
        final RealmRef realm1Alike = new RealmRef(
            randomValueOtherThan(realm1.getName(), () -> randomAlphaOfLengthBetween(3, 8)),
            realm1.getType(), randomAlphaOfLengthBetween(3, 8));
        final RealmRef realm2 = randomNonFileNativeRealmOtherThan(realm1, realm1Alike);
        final RealmRef realm3 = randomNonFileNativeRealmOtherThan(realm1, realm1Alike, realm2);
        final RealmRef realm4 = randomNonFileNativeRealmOtherThan(realm1, realm1Alike, realm2, realm3);
        final RealmRef authRealm = randomFrom(realm2, realm3, realm4);
        final RealmRef lookupRealm = randomFrom(realm2, realm3, realm4, null);

        // Same username, realm name and type
        assertTrue(new Authentication(user1, realm1, lookupRealm).sameUserAs(new Authentication(user1, realm1, lookupRealm)));

        // Same username, same realm type, different realm name
        assertFalse(new Authentication(user1, realm1, null).sameUserAs(new Authentication(user1, realm1Alike, null)));
        assertFalse(new Authentication(user1, authRealm, realm1).sameUserAs(new Authentication(user1, authRealm, realm1Alike)));

        // File and native realms only needs realm type to match
        assertTrue(new Authentication(user1, fileRealm1, null).sameUserAs(new Authentication(user1, fileRealm2, null)));
        assertTrue(new Authentication(user1, authRealm, fileRealm1).sameUserAs(new Authentication(user1, authRealm, fileRealm2)));
        assertTrue(new Authentication(user1, nativeRealm1, null).sameUserAs(new Authentication(user1, nativeRealm2, null)));
        assertTrue(new Authentication(user1, authRealm, nativeRealm1).sameUserAs(new Authentication(user1, authRealm, nativeRealm2)));

        // Different usernames
        assertFalse(new Authentication(user1, realm1, lookupRealm).sameUserAs(new Authentication(user2, realm1, lookupRealm)));
        // Same username, totally different realm
        assertFalse(new Authentication(user1, realm1, null).sameUserAs(new Authentication(user1, realm2, null)));
        // same username, totally different run-as realm
        assertFalse(new Authentication(user1, randomFrom(realm1, realm2), realm3).sameUserAs(
            new Authentication(user1, randomFrom(realm1, realm2), realm4)));
    }

    public void testSameUserAsForApiKeys() {
        final String username1 = randomAlphaOfLengthBetween(3, 8);
        final String apiKeyId1 = randomAlphaOfLengthBetween(10, 20);
        final String apiKeyId2 = randomValueOtherThan(apiKeyId1, () -> randomAlphaOfLengthBetween(10, 20));
        // Same user, same API key ID
        assertTrue(randomApiKeyAuthentication(username1, apiKeyId1).sameUserAs(randomApiKeyAuthentication(username1, apiKeyId1)));
        // same user, different API key ID
        assertFalse(randomApiKeyAuthentication(username1, apiKeyId1).sameUserAs(randomApiKeyAuthentication(username1, apiKeyId2)));

        // User and its API key
        final User user2 = randomUserOtherThan(new User[0]);
        assertFalse(randomApiKeyAuthentication(user2.principal(), apiKeyId1).sameUserAs(
            new Authentication(user2, randomRealmOtherThan(new RealmRef[0]), null)));
    }

    public void testSameUserAsForToken() {
        final User user1 = randomUserOtherThan(new User[0]);
        final User user2 = randomUserOtherThan(user1);
        final RealmRef realm1 = randomRealmOtherThan(new RealmRef[0]);
        final RealmRef realm2 = randomRealmOtherThan(new RealmRef[0]);

        final RealmRef lookupRealm = randomFrom(realm2, null);
        assertTrue(new Authentication(user1, realm1, lookupRealm).sameUserAs(
            new Authentication(user1, realm1, lookupRealm, Version.CURRENT, AuthenticationType.TOKEN, Map.of())));
        assertFalse(new Authentication(user1, realm1, lookupRealm).sameUserAs(
            new Authentication(user2, realm1, lookupRealm, Version.CURRENT, AuthenticationType.TOKEN, Map.of())));
    }

    private User randomUserOtherThan(User... users) {
        return new User(randomValueOtherThanMany(
            username -> Arrays.stream(users).anyMatch(user -> user.principal().equals(username)),
            () -> randomAlphaOfLengthBetween(3, 8)), randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
    }

    private RealmRef randomRealmOtherThan(RealmRef ...excludedRealmRefs) {
        return randomValueOtherThanMany(
            r -> Arrays.stream(excludedRealmRefs)
                .anyMatch(excluded ->
                    (Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE).contains(excluded.getType())
                        && excluded.getType().equals(r.getType())) ||
                    (excluded.getName().equals(r.getName()) && excluded.getType().equals(r.getType()))),
            () -> new RealmRef(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)));
    }

    private RealmRef randomNonFileNativeRealmOtherThan(RealmRef ...excludedRealmRefs) {
        final RealmRef fileRealm = new RealmRef(randomAlphaOfLengthBetween(3, 8), FileRealmSettings.TYPE, randomAlphaOfLengthBetween(3, 8));
        final RealmRef nativeRealm =
            new RealmRef(randomAlphaOfLengthBetween(3, 8), NativeRealmSettings.TYPE, randomAlphaOfLengthBetween(3, 8));
        return randomRealmOtherThan(
            Stream.concat(Arrays.stream(excludedRealmRefs), Stream.of(fileRealm, nativeRealm)).toArray(RealmRef[]::new));
    }

    private Authentication randomApiKeyAuthentication(String username, String apiKeyId) {
        final User user =
            new User(username, randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
        final RealmRef apiKeyRealm = new RealmRef("_es_api_key", "_es_api_key", randomAlphaOfLengthBetween(3, 8));
        return new Authentication(user,
            apiKeyRealm,
            null,
            VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.CURRENT),
            AuthenticationType.API_KEY,
            Map.of(API_KEY_ID_KEY, apiKeyId));
    }
}
