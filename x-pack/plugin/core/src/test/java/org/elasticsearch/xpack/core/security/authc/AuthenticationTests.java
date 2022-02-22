/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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

    public void testCanAccessResourcesOf() {
        // Same user is the same
        final User user1 = randomUser();
        final RealmRef realm1 = randomRealmRef(false);
        checkCanAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm1));

        // Different username is different no matter which realm it is from
        final User user2 = randomValueOtherThanMany(u -> u.principal().equals(user1.principal()), AuthenticationTests::randomUser);
        // user 2 can be from either the same realm or a different realm
        final RealmRef realm2 = randomFrom(realm1, randomRealmRef(false));

        assertCannotAccessResources(randomAuthentication(user1, realm2), randomAuthentication(user2, realm2));

        // Same username but different realm is different
        final RealmRef realm3;
        switch (randomIntBetween(0, 2)) {
            case 0: // change name
                realm3 = mutateRealm(realm1, randomAlphaOfLengthBetween(3, 8), null);
                if (realmIsSingleton(realm1)) {
                    checkCanAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm3));
                } else {
                    assertCannotAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm3));
                }
                break;
            case 1: // change type
                realm3 = mutateRealm(realm1, null, randomAlphaOfLengthBetween(3, 8));
                assertCannotAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm3));
                break;
            case 2: // both
                realm3 = mutateRealm(realm1, randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
                assertCannotAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm3));
                break;
            default:
                assert false : "Case number out of range";
        }

        // User and its API key are not the same owner
        assertCannotAccessResources(
            randomAuthentication(user1, realm1),
            randomApiKeyAuthentication(user1, randomAlphaOfLengthBetween(10, 20))
        );

        // Same API key ID are the same owner
        final String apiKeyId1 = randomAlphaOfLengthBetween(10, 20);
        checkCanAccessResources(randomApiKeyAuthentication(user1, apiKeyId1), randomApiKeyAuthentication(user1, apiKeyId1));

        // Two API keys (2 API key IDs) are not the same owner
        final String apiKeyId2 = randomValueOtherThan(apiKeyId1, () -> randomAlphaOfLengthBetween(10, 20));
        assertCannotAccessResources(
            randomApiKeyAuthentication(randomFrom(user1, user2), apiKeyId1),
            randomApiKeyAuthentication(randomFrom(user1, user2), apiKeyId2)
        );
        final User user3 = randomValueOtherThanMany(
            u -> u.principal().equals(user1.principal()) || u.principal().equals(user2.principal()),
            AuthenticationTests::randomUser
        );

        // Same API key but run-as different user are not the same owner
        assertCannotAccessResources(
            randomApiKeyAuthentication(user1, apiKeyId1).runAs(user2, realm2),
            randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2)
        );

        // Same or different API key run-as the same user are the same owner
        checkCanAccessResources(
            randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2),
            randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2)
        );
        checkCanAccessResources(
            randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2),
            randomApiKeyAuthentication(user2, apiKeyId2).runAs(user3, realm2)
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

    public void testNonRealmAuthenticationsNoDomain() {
        final String apiKeyId = randomAlphaOfLengthBetween(10, 20);
        Authentication apiAuthentication = randomApiKeyAuthentication(randomUser(), apiKeyId);
        assertThat(apiAuthentication.isAssignedToDomain(), is(false));
        assertThat(apiAuthentication.getDomain(), nullValue());
        apiAuthentication = apiAuthentication.token();
        assertThat(apiAuthentication.isAssignedToDomain(), is(false));
        assertThat(apiAuthentication.getDomain(), nullValue());
        Authentication anonAuthentication = randomAnonymousAuthentication();
        assertThat(anonAuthentication.isAssignedToDomain(), is(false));
        assertThat(anonAuthentication.getDomain(), nullValue());
        Authentication serviceAccountAuthentication = randomServiceAccountAuthentication();
        assertThat(serviceAccountAuthentication.isAssignedToDomain(), is(false));
        assertThat(serviceAccountAuthentication.getDomain(), nullValue());
        Authentication internalAuthentication = randomInternalAuthentication();
        assertThat(internalAuthentication.isAssignedToDomain(), is(false));
        assertThat(internalAuthentication.getDomain(), nullValue());
    }

    public void testRealmAuthenticationIsAssignedToDomain() {
        Authentication realmAuthn = randomRealmAuthentication(true);
        assertThat(realmAuthn.isAssignedToDomain(), is(true));
        realmAuthn = realmAuthn.token();
        assertThat(realmAuthn.isAssignedToDomain(), is(true));
        realmAuthn = randomRealmAuthentication(false);
        assertThat(realmAuthn.isAssignedToDomain(), is(false));
        realmAuthn = realmAuthn.token();
        assertThat(realmAuthn.isAssignedToDomain(), is(false));
    }

    public void testRunAsAuthenticationWithDomain() {
        RealmRef authnRealmRef = randomRealmRef(true);
        RealmRef lookupRealmRef = randomRealmRef(true);
        // realm/token run-as
        Authentication test = Authentication.newRealmAuthentication(randomUser(), authnRealmRef);
        if (randomBoolean()) {
            test = test.token();
        }
        test = test.runAs(randomUser(), lookupRealmRef);
        if (randomBoolean()) {
            test = test.token();
        }
        assertThat(test.isAssignedToDomain(), is(true));
        assertThat(test.getDomain(), is(lookupRealmRef.getDomain()));
        test = Authentication.newRealmAuthentication(randomUser(), randomRealmRef(false));
        if (randomBoolean()) {
            test = test.token();
        }
        test = test.runAs(randomUser(), lookupRealmRef);
        if (randomBoolean()) {
            test = test.token();
        }
        assertThat(test.isAssignedToDomain(), is(true));
        assertThat(test.getDomain(), is(lookupRealmRef.getDomain()));
        test = Authentication.newRealmAuthentication(randomUser(), authnRealmRef);
        if (randomBoolean()) {
            test = test.token();
        }
        test = test.runAs(randomUser(), randomRealmRef(false));
        if (randomBoolean()) {
            test = test.token();
        }
        assertThat(test.isAssignedToDomain(), is(false));
        assertThat(test.getDomain(), nullValue());
        test = Authentication.newRealmAuthentication(randomUser(), randomRealmRef(false));
        if (randomBoolean()) {
            test = test.token();
        }
        test = test.runAs(randomUser(), lookupRealmRef);
        if (randomBoolean()) {
            test = test.token();
        }
        assertThat(test.isAssignedToDomain(), is(true));
        assertThat(test.getDomain(), is(lookupRealmRef.getDomain()));
        // api key run-as
        test = randomApiKeyAuthentication(randomUser(), randomAlphaOfLengthBetween(10, 20), Version.CURRENT);
        if (randomBoolean()) {
            test = test.token();
        }
        assertThat(test.isAssignedToDomain(), is(false));
        assertThat(test.getDomain(), nullValue());
        if (randomBoolean()) {
            test = test.runAs(randomUser(), lookupRealmRef);
            if (randomBoolean()) {
                test = test.token();
            }
            assertThat(test.isAssignedToDomain(), is(true));
            assertThat(test.getDomain(), is(lookupRealmRef.getDomain()));
        } else {
            test = test.runAs(randomUser(), randomRealmRef(false));
            if (randomBoolean()) {
                test = test.token();
            }
            assertThat(test.isAssignedToDomain(), is(false));
            assertThat(test.getDomain(), nullValue());
        }
        // service account run-as
        test = randomServiceAccountAuthentication();
        assertThat(test.isAssignedToDomain(), is(false));
        assertThat(test.getDomain(), nullValue());
        if (randomBoolean()) {
            test = test.runAs(randomUser(), lookupRealmRef);
            if (randomBoolean()) {
                test = test.token();
            }
            assertThat(test.isAssignedToDomain(), is(true));
            assertThat(test.getDomain(), is(lookupRealmRef.getDomain()));
        } else {
            test = test.runAs(randomUser(), randomRealmRef(false));
            if (randomBoolean()) {
                test = test.token();
            }
            assertThat(test.isAssignedToDomain(), is(false));
            assertThat(test.getDomain(), nullValue());
        }
    }

    public void testDomainSerialize() throws Exception {
        Authentication test = randomRealmAuthentication(true);
        boolean runAs = randomBoolean();
        if (runAs) {
            test = test.runAs(randomUser(), randomRealmRef(true));
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            test.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            Authentication testBack = new Authentication(in);
            assertThat(test.getDomain(), is(testBack.getDomain()));
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_8_0_0);
            test.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_8_0_0);
            Authentication testBack = new Authentication(in);
            assertThat(testBack.getDomain(), nullValue());
            assertThat(testBack.isAssignedToDomain(), is(false));
        }
    }

    private void checkCanAccessResources(Authentication authentication0, Authentication authentication1) {
        if (authentication0.getAuthenticationType() == authentication1.getAuthenticationType()
            || EnumSet.of(AuthenticationType.REALM, AuthenticationType.TOKEN)
                .equals(EnumSet.of(authentication0.getAuthenticationType(), authentication1.getAuthenticationType()))) {
            assertTrue(authentication0.canAccessResourcesOf(authentication1));
            assertTrue(authentication1.canAccessResourcesOf(authentication0));
        } else {
            assertCannotAccessResources(authentication0, authentication1);
        }
    }

    private void assertCannotAccessResources(Authentication authentication0, Authentication authentication1) {
        assertFalse(authentication0.canAccessResourcesOf(authentication1));
        assertFalse(authentication1.canAccessResourcesOf(authentication0));
    }

    public static User randomUser() {
        return new User(randomAlphaOfLengthBetween(3, 8), randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
    }

    public static RealmRef randomRealmRef(boolean underDomain) {
        final Supplier<String> randomRealmTypeSupplier = () -> randomFrom(
            FileRealmSettings.TYPE,
            NativeRealmSettings.TYPE,
            LdapRealmSettings.AD_TYPE,
            LdapRealmSettings.LDAP_TYPE,
            JwtRealmSettings.TYPE,
            OpenIdConnectRealmSettings.TYPE,
            SamlRealmSettings.TYPE,
            KerberosRealmSettings.TYPE,
            randomAlphaOfLengthBetween(3, 8)
        );
        if (underDomain) {
            final Set<RealmConfig.RealmIdentifier> domainRealms = Set.of(
                randomArray(
                    1,
                    4,
                    RealmConfig.RealmIdentifier[]::new,
                    () -> new RealmConfig.RealmIdentifier(
                        randomRealmTypeSupplier.get(),
                        randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT)
                    )
                )
            );
            RealmDomain domain = new RealmDomain("domain", domainRealms);
            RealmConfig.RealmIdentifier realmIdentifier = randomFrom(domainRealms);
            return new RealmRef(realmIdentifier.getName(), realmIdentifier.getType(), randomAlphaOfLengthBetween(3, 8), domain);
        } else {
            return new RealmRef(randomAlphaOfLengthBetween(3, 8), randomRealmTypeSupplier.get(), randomAlphaOfLengthBetween(3, 8), null);
        }
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
            realmRef = randomRealmRef(false);
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
                randomRealmRef(false),
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
        return randomApiKeyAuthentication(user, apiKeyId, VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.CURRENT));
    }

    public static Authentication randomApiKeyAuthentication(User user, String apiKeyId, Version version) {
        final HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(AuthenticationField.API_KEY_ID_KEY, apiKeyId);
        metadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLengthBetween(1, 16));
        metadata.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, new BytesArray("{}"));
        metadata.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, new BytesArray("""
            {"x":{"cluster":["all"],"indices":[{"names":["index*"],"privileges":["all"]}]}}"""));
        return Authentication.newApiKeyAuthentication(AuthenticationResult.success(user, metadata), randomAlphaOfLengthBetween(3, 8))
            .maybeRewriteForOlderVersion(version);
    }

    public static Authentication randomServiceAccountAuthentication() {
        return Authentication.newServiceAccountAuthentication(
            new User(randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8)),
            randomAlphaOfLengthBetween(3, 8),
            Map.of(
                "_token_name",
                randomAlphaOfLength(8),
                "_token_source",
                randomFrom(TokenInfo.TokenSource.values()).name().toLowerCase(Locale.ROOT)
            )
        );
    }

    public static Authentication randomRealmAuthentication(boolean underDomain) {
        return Authentication.newRealmAuthentication(randomUser(), randomRealmRef(underDomain));
    }

    public static Authentication randomInternalAuthentication() {
        String nodeName = randomAlphaOfLengthBetween(3, 8);
        return randomFrom(
            Authentication.newInternalAuthentication(
                randomFrom(SystemUser.INSTANCE, XPackUser.INSTANCE, XPackSecurityUser.INSTANCE, AsyncSearchUser.INSTANCE),
                Version.CURRENT,
                nodeName
            ),
            Authentication.newInternalFallbackAuthentication(SystemUser.INSTANCE, nodeName)
        );
    }

    public static Authentication randomAnonymousAuthentication() {
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "anon_role").build();
        String nodeName = randomAlphaOfLengthBetween(3, 8);
        return Authentication.newAnonymousAuthentication(new AnonymousUser(settings), nodeName);
    }

    private boolean realmIsSingleton(RealmRef realmRef) {
        return Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE).contains(realmRef.getType());
    }
}
