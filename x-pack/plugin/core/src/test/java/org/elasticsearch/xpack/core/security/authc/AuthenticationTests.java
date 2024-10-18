/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo;
import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfoTests.randomRoleDescriptorsIntersection;
import static org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions.ROLE_REMOTE_CLUSTER_PRIVS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class AuthenticationTests extends ESTestCase {

    public void testIsFailedRunAs() {
        final Authentication failedAuthentication = randomRealmAuthentication(randomBoolean()).runAs(randomUser(), null);
        assertTrue(failedAuthentication.isRunAs());
        assertTrue(failedAuthentication.isFailedRunAs());

        final Authentication authentication = AuthenticationTestHelper.builder().realm().runAs().build();
        assertTrue(authentication.isRunAs());
        assertFalse(authentication.isFailedRunAs());
    }

    public void testCanAccessResourcesOf() {
        // Same user is the same
        final User user1 = randomUser();
        final RealmRef realm1 = randomRealmRef(false);
        assertCanAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm1));

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
                    assertCanAccessResources(randomAuthentication(user1, realm1), randomAuthentication(user1, realm3));
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
        assertCanAccessResources(randomApiKeyAuthentication(user1, apiKeyId1), randomApiKeyAuthentication(user1, apiKeyId1));

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
        assertCanAccessResources(
            randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2),
            randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2)
        );
        assertCanAccessResources(
            randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2),
            randomApiKeyAuthentication(user2, apiKeyId2).runAs(user3, realm2)
        );
    }

    public void testCrossClusterAccessCanAccessResourceOf() throws IOException {
        final String apiKeyId1 = randomAlphaOfLengthBetween(10, 20);
        final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo1 = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(
            AuthenticationTestHelper.builder().realm().build()
        );
        final Authentication authentication = AuthenticationTestHelper.builder()
            .crossClusterAccess(apiKeyId1, crossClusterAccessSubjectInfo1)
            .build(false);
        final User apiKeyUser1 = authentication.getEffectiveSubject().getUser();

        // 1. Same cross cluster access authentication, allow access (this is only scenario resource share is allowed)
        assertCanAccessResources(authentication, authentication);

        // 2. The cross cluster access authentication is not the same as its nested QC user authentication
        // This also covers the case where FC happens to have a user that looks the same as the QC user
        assertCannotAccessResources(authentication, crossClusterAccessSubjectInfo1.getAuthentication());
        assertThat(
            crossClusterAccessSubjectInfo1.getAuthentication(),
            is(authentication.getEffectiveSubject().getMetadata().get(AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY))
        );

        // 3. The same API key can be used with the REST interface
        assertCannotAccessResources(authentication, AuthenticationTestHelper.builder().apiKey(apiKeyId1).build());

        // 4. The same QC user can use a different cross cluster access API key
        final String apiKeyId2 = randomValueOtherThan(apiKeyId1, () -> randomAlphaOfLengthBetween(10, 20));
        assertCannotAccessResources(
            authentication,
            AuthenticationTestHelper.builder().crossClusterAccess(apiKeyId2, crossClusterAccessSubjectInfo1).build(false)
        );

        // 5. The same API key but for a different QC user base on its own canAccessResourcesOf
        final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo2 = randomValueOtherThanMany(
            ra -> crossClusterAccessSubjectInfo1.getAuthentication().canAccessResourcesOf(ra.getAuthentication()),
            AuthenticationTestHelper::randomCrossClusterAccessSubjectInfo
        );
        assertCannotAccessResources(
            authentication,
            AuthenticationTestHelper.builder().user(apiKeyUser1).crossClusterAccess(apiKeyId1, crossClusterAccessSubjectInfo2).build(false)
        );

        // 6. Allow access if QC authentication is a run-as equivalent
        final Authentication qcAuthentication1 = crossClusterAccessSubjectInfo1.getAuthentication();
        final Authentication qcAuthentication2;
        if (qcAuthentication1.isRunAs()) {
            qcAuthentication2 = Authentication.newRealmAuthentication(
                qcAuthentication1.getEffectiveSubject().getUser(),
                qcAuthentication1.getEffectiveSubject().getRealm()
            );
        } else {
            qcAuthentication2 = AuthenticationTestHelper.builder()
                .runAs()
                .user(qcAuthentication1.getEffectiveSubject().getUser())
                .realmRef(qcAuthentication1.getEffectiveSubject().getRealm())
                .build();
        }
        // random the role descriptor intersection because it does not matter for resource sharing check
        final Authentication authentication2 = AuthenticationTestHelper.builder()
            .user(apiKeyUser1)
            .crossClusterAccess(apiKeyId1, new CrossClusterAccessSubjectInfo(qcAuthentication2, randomRoleDescriptorsIntersection()))
            .build(false);
        assertCanAccessResources(authentication, authentication2);

        // 7. QC username variation
        final String qcUsername1 = qcAuthentication1.getEffectiveSubject().getUser().principal();
        assertCannotAccessResources(
            authentication,
            AuthenticationTestHelper.builder()
                .user(apiKeyUser1)
                .crossClusterAccess(
                    apiKeyId1,
                    new CrossClusterAccessSubjectInfo(
                        AuthenticationTestHelper.builder()
                            .user(randomValueOtherThanMany(u -> u.principal().equals(qcUsername1), AuthenticationTests::randomUser))
                            .realmRef(qcAuthentication1.getEffectiveSubject().getRealm())
                            .build(),
                        randomRoleDescriptorsIntersection()
                    )
                )
                .build(false)
        );

        // 8. QC user realm variations
        final RealmRef realm1 = qcAuthentication1.getEffectiveSubject().getRealm();
        final Authentication authenticationWithSameQcUserDifferentRealm;
        final CheckedFunction<RealmRef, Authentication, IOException> authenticationForRealm = realmRef -> AuthenticationTestHelper.builder()
            .user(apiKeyUser1)
            .crossClusterAccess(
                apiKeyId1,
                new CrossClusterAccessSubjectInfo(
                    AuthenticationTestHelper.builder().user(qcAuthentication1.getEffectiveSubject().getUser()).realmRef(realmRef).build(),
                    randomRoleDescriptorsIntersection()
                )
            )
            .build(false);
        switch (randomIntBetween(0, 2)) {
            case 0: // change name
                authenticationWithSameQcUserDifferentRealm = authenticationForRealm.apply(
                    mutateRealm(realm1, randomAlphaOfLengthBetween(3, 8), null)
                );
                if (realmIsSingleton(realm1)) {
                    assertCanAccessResources(authentication, authenticationWithSameQcUserDifferentRealm);
                } else {
                    assertCannotAccessResources(authentication, authenticationWithSameQcUserDifferentRealm);
                }
                break;
            case 1: // change type
                authenticationWithSameQcUserDifferentRealm = authenticationForRealm.apply(
                    mutateRealm(realm1, null, randomAlphaOfLengthBetween(3, 8))
                );
                assertCannotAccessResources(authentication, authenticationWithSameQcUserDifferentRealm);
                break;
            case 2: // both
                authenticationWithSameQcUserDifferentRealm = authenticationForRealm.apply(
                    mutateRealm(realm1, randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8))
                );
                assertCannotAccessResources(authentication, authenticationWithSameQcUserDifferentRealm);
                break;
            default:
                assert false : "Case number out of range";
        }

    }

    private static Authentication randomCrossClusterAccessAuthentication(
        String crossClusterApiKeyId,
        User user,
        Authentication authentication
    ) {
        return AuthenticationTestHelper.builder()
            .crossClusterAccess(crossClusterApiKeyId, randomCrossClusterAccessSubjectInfo(authentication))
            .user(user)
            .build(false);
    }

    public void testCrossClusterAccessCanAccessResourceOfWithApiKey() {
        final User user1 = randomUser();
        final RealmRef realm1 = randomRealmRef(false);

        // Different username is different no matter which realm it is from
        final User user2 = randomValueOtherThanMany(u -> u.principal().equals(user1.principal()), AuthenticationTests::randomUser);
        // user 2 can be from either the same realm or a different realm
        final RealmRef realm2 = randomFrom(realm1, randomRealmRef(false));

        final String apiKeyId1 = randomAlphaOfLengthBetween(10, 20);
        // User is irrelevant
        final User crossClusterUser1 = randomFrom(user1, user2, randomUser());
        final String crossClusterApiKeyId1 = randomAlphaOfLengthBetween(10, 20);

        // Same cross cluster access authentication with the same API key is allowed.
        assertCanAccessResources(
            randomCrossClusterAccessAuthentication(crossClusterApiKeyId1, crossClusterUser1, randomApiKeyAuthentication(user1, apiKeyId1)),
            randomCrossClusterAccessAuthentication(crossClusterApiKeyId1, crossClusterUser1, randomApiKeyAuthentication(user1, apiKeyId1))
        );

        // Cluster access authentication with different API credentials keys is not allowed.
        final String crossClusterApiKeyId2 = randomValueOtherThan(crossClusterApiKeyId1, () -> randomAlphaOfLengthBetween(10, 20));
        assertCannotAccessResources(
            randomCrossClusterAccessAuthentication(crossClusterApiKeyId1, crossClusterUser1, randomApiKeyAuthentication(user1, apiKeyId1)),
            randomCrossClusterAccessAuthentication(crossClusterApiKeyId2, crossClusterUser1, randomApiKeyAuthentication(user1, apiKeyId1))
        );

        // Cross cluster access with a user and its API key is not the same owner, hence not allowed.
        assertCannotAccessResources(
            randomCrossClusterAccessAuthentication(crossClusterApiKeyId1, crossClusterUser1, randomAuthentication(user1, realm1)),
            randomCrossClusterAccessAuthentication(crossClusterApiKeyId1, crossClusterUser1, randomApiKeyAuthentication(user1, apiKeyId1))
        );

        // Cross cluster access with two different API keys (regardless if the same user is owner) is not allowed.
        final String apiKeyId2 = randomValueOtherThanMany(id -> id.equals(apiKeyId1), () -> randomAlphaOfLengthBetween(10, 20));
        assertCannotAccessResources(
            randomCrossClusterAccessAuthentication(
                crossClusterApiKeyId1,
                crossClusterUser1,
                randomApiKeyAuthentication(randomFrom(user1, user2), apiKeyId1)
            ),
            randomCrossClusterAccessAuthentication(
                crossClusterApiKeyId1,
                crossClusterUser1,
                randomApiKeyAuthentication(randomFrom(user1, user2), apiKeyId2)
            )
        );

        // Cross cluster access using same API key but run-as different users is not allowed.
        final User user3 = randomValueOtherThanMany(
            u -> u.principal().equals(user1.principal()) || u.principal().equals(user2.principal()),
            AuthenticationTests::randomUser
        );
        assertCannotAccessResources(
            randomCrossClusterAccessAuthentication(
                crossClusterApiKeyId1,
                crossClusterUser1,
                randomApiKeyAuthentication(user1, apiKeyId1).runAs(user2, realm2)
            ),
            randomCrossClusterAccessAuthentication(
                crossClusterApiKeyId1,
                crossClusterUser1,
                randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2)
            )
        );

        // Cross cluster access using same or different API key which run-as the same user (user3) is allowed.
        assertCanAccessResources(
            randomCrossClusterAccessAuthentication(
                crossClusterApiKeyId1,
                crossClusterUser1,
                randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2)
            ),
            randomCrossClusterAccessAuthentication(
                crossClusterApiKeyId1,
                crossClusterUser1,
                randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2)
            )
        );
        assertCanAccessResources(
            randomCrossClusterAccessAuthentication(
                crossClusterApiKeyId1,
                crossClusterUser1,
                randomApiKeyAuthentication(user1, apiKeyId1).runAs(user3, realm2)
            ),
            randomCrossClusterAccessAuthentication(
                crossClusterApiKeyId1,
                crossClusterUser1,
                randomApiKeyAuthentication(user2, apiKeyId2).runAs(user3, realm2)
            )
        );
    }

    public void testTokenAccessResourceOf() {
        final User user = randomUser();
        final RealmRef realmRef = randomRealmRef(false);
        Authentication original = Authentication.newRealmAuthentication(user, realmRef);
        Authentication token = original.token();
        assertCanAccessResources(original, token);
        assertCanAccessResources(original, token.token());
        assertCanAccessResources(token, token.token());

        Authentication original2 = randomApiKeyAuthentication(user, randomAlphaOfLengthBetween(10, 20));
        Authentication token2 = original2.token();
        assertCanAccessResources(original2, token2);
        assertCanAccessResources(original2, token2.token());
        assertCanAccessResources(token2, token2.token());

        assertCannotAccessResources(original, original2);
        assertCannotAccessResources(original, token2);
        assertCannotAccessResources(token, original2);
        assertCannotAccessResources(token, token2);
        assertCannotAccessResources(token.token(), token2);
        assertCannotAccessResources(token, token2.token());

        Authentication original3 = Authentication.newAnonymousAuthentication(
            new AnonymousUser(Settings.EMPTY),
            randomAlphaOfLengthBetween(3, 8)
        );
        Authentication token3 = original3.token();
        assertCanAccessResources(original3, token3);
        assertCanAccessResources(original3, token3.token());
        assertCanAccessResources(token3, token3.token());
    }

    public void testRunAsAccessResourceOf() {
        final User user = randomUser();
        final User otherUser = randomValueOtherThan(user, () -> randomUser());
        final RealmRef realmRef = randomRealmRef(false);
        final RealmRef otherRealmRef = randomValueOtherThan(realmRef, () -> randomRealmRef(false));
        Authentication original = Authentication.newRealmAuthentication(user, realmRef);

        // can
        Authentication runAs1 = Authentication.newRealmAuthentication(otherUser, otherRealmRef).runAs(user, realmRef);
        assertCanAccessResources(original, runAs1);
        assertCanAccessResources(original.token(), runAs1);
        assertCanAccessResources(original, runAs1.token());
        assertCanAccessResources(original.token(), runAs1.token());

        Authentication runAs2 = randomApiKeyAuthentication(otherUser, randomAlphaOfLength(8)).runAs(user, realmRef);
        assertCanAccessResources(original, runAs2);
        assertCanAccessResources(original.token(), runAs2);
        assertCanAccessResources(original, runAs2.token());
        assertCanAccessResources(original.token(), runAs2.token());

        assertCanAccessResources(runAs1, runAs2);
        assertCanAccessResources(runAs1.token(), runAs2);
        assertCanAccessResources(runAs1, runAs2.token());
        assertCanAccessResources(runAs1.token(), runAs2.token());

        // cannot
        Authentication runAs3 = original.runAs(otherUser, realmRef);
        assertCannotAccessResources(original, runAs3);
        assertCannotAccessResources(original.token(), runAs3);
        assertCannotAccessResources(original, runAs3.token());
        assertCannotAccessResources(original.token(), runAs3.token());

        Authentication runAs4 = original.runAs(user, otherRealmRef);
        if (FileRealmSettings.TYPE.equals(realmRef.getType()) && FileRealmSettings.TYPE.equals(otherRealmRef.getType())
            || NativeRealmSettings.TYPE.equals(realmRef.getType()) && NativeRealmSettings.TYPE.equals(otherRealmRef.getType())) {
            assertCanAccessResources(original, runAs4);
            assertCanAccessResources(original.token(), runAs4);
            assertCanAccessResources(original, runAs4.token());
            assertCanAccessResources(original.token(), runAs4.token());
        } else {
            assertCannotAccessResources(original, runAs4);
            assertCannotAccessResources(original.token(), runAs4);
            assertCannotAccessResources(original, runAs4.token());
            assertCannotAccessResources(original.token(), runAs4.token());
        }
    }

    public void testIsServiceAccount() {
        final boolean isServiceAccount = randomBoolean();
        final Authentication authentication;
        if (isServiceAccount) {
            authentication = AuthenticationTestHelper.builder().serviceAccount().build();
        } else {
            authentication = randomValueOtherThanMany(
                authc -> "_service_account".equals(authc.getAuthenticatingSubject().getRealm().getName()),
                () -> AuthenticationTestHelper.builder().build()
            );
        }

        if (isServiceAccount) {
            assertThat(authentication.isServiceAccount(), is(true));
        } else {
            assertThat(authentication.isServiceAccount(), is(false));
        }
    }

    public void testIsCrossClusterAccess() {
        final boolean isCrossClusterAccess = randomBoolean();
        final Authentication authentication;
        if (isCrossClusterAccess) {
            authentication = AuthenticationTestHelper.builder().crossClusterAccess().build();
        } else {
            authentication = randomValueOtherThanMany(
                authc -> AuthenticationField.CROSS_CLUSTER_ACCESS_REALM_TYPE.equals(authc.getAuthenticatingSubject().getRealm().getType()),
                () -> AuthenticationTestHelper.builder().build()
            );
        }

        if (isCrossClusterAccess) {
            assertThat(authentication.isCrossClusterAccess(), is(true));
            // Also validate that this does not clash with API keys
            assertThat(authentication.isApiKey(), is(false));
            assertThat(authentication.isAuthenticatedAsApiKey(), is(false));
        } else {
            assertThat(authentication.isCrossClusterAccess(), is(false));
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
        Authentication crossClusterAccessAuthentication = AuthenticationTestHelper.builder().crossClusterAccess().build();
        assertThat(crossClusterAccessAuthentication.isAssignedToDomain(), is(false));
        assertThat(crossClusterAccessAuthentication.getDomain(), nullValue());
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
        test = test.runAs(randomUser(), lookupRealmRef);
        if (randomBoolean()) {
            test = test.token();
        }
        assertThat(test.isAssignedToDomain(), is(true));
        assertThat(test.getDomain(), is(lookupRealmRef.getDomain()));
        test = Authentication.newRealmAuthentication(randomUser(), randomRealmRef(false));
        test = test.runAs(randomUser(), lookupRealmRef);
        if (randomBoolean()) {
            test = test.token();
        }
        assertThat(test.isAssignedToDomain(), is(true));
        assertThat(test.getDomain(), is(lookupRealmRef.getDomain()));
        test = Authentication.newRealmAuthentication(randomUser(), authnRealmRef);
        test = test.runAs(randomUser(), randomRealmRef(false));
        if (randomBoolean()) {
            test = test.token();
        }
        assertThat(test.isAssignedToDomain(), is(false));
        assertThat(test.getDomain(), nullValue());
        test = Authentication.newRealmAuthentication(randomUser(), randomRealmRef(false));
        test = test.runAs(randomUser(), lookupRealmRef);
        if (randomBoolean()) {
            test = test.token();
        }
        assertThat(test.isAssignedToDomain(), is(true));
        assertThat(test.getDomain(), is(lookupRealmRef.getDomain()));
        // api key run-as
        test = randomApiKeyAuthentication(randomUser(), randomAlphaOfLengthBetween(10, 20), TransportVersion.current());
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
        // service account cannot run-as
        test = randomServiceAccountAuthentication();
        assertThat(test.isAssignedToDomain(), is(false));
        assertThat(test.getDomain(), nullValue());
    }

    public void testCanAccessAcrossDomain() {
        RealmRef accessRealmRef = randomRealmRef(true);
        User accessUser = randomUser();
        Authentication accessAuthentication = Authentication.newRealmAuthentication(accessUser, accessRealmRef);

        RealmDomain otherDomain = randomDomain(randomBoolean());
        RealmConfig.RealmIdentifier otherRealmIdentifier = randomFrom(otherDomain.realms());
        RealmRef otherRealmRef = new RealmRef(
            otherRealmIdentifier.getName(),
            otherRealmIdentifier.getType(),
            randomAlphaOfLengthBetween(3, 8),
            otherDomain
        );

        // same user other realm
        Authentication otherRealmAuthentication = Authentication.newRealmAuthentication(accessUser, otherRealmRef);
        Authentication otherRealmRunAsAuthentication = Authentication.newRealmAuthentication(randomUser(), randomRealmRef(randomBoolean()))
            .runAs(accessUser, otherRealmRef);
        if ((accessRealmRef.getDomain().realms().contains(otherRealmIdentifier))
            || (otherRealmIdentifier.getType().equals(FileRealmSettings.TYPE)
                && accessRealmRef.getDomain().realms().stream().anyMatch(r -> FileRealmSettings.TYPE.equals(r.getType())))
            || (otherRealmIdentifier.getType().equals(NativeRealmSettings.TYPE)
                && accessRealmRef.getDomain().realms().stream().anyMatch(r -> NativeRealmSettings.TYPE.equals(r.getType())))) {
            assertThat(accessAuthentication.canAccessResourcesOf(otherRealmAuthentication), is(true));
            assertThat(accessAuthentication.canAccessResourcesOf(otherRealmAuthentication.token()), is(true));
            assertThat(accessAuthentication.canAccessResourcesOf(otherRealmRunAsAuthentication), is(true));
            assertThat(accessAuthentication.canAccessResourcesOf(otherRealmRunAsAuthentication.token()), is(true));
        } else {
            assertThat(accessAuthentication.canAccessResourcesOf(otherRealmAuthentication), is(false));
            assertThat(accessAuthentication.canAccessResourcesOf(otherRealmAuthentication.token()), is(false));
            assertThat(accessAuthentication.canAccessResourcesOf(otherRealmRunAsAuthentication), is(false));
            assertThat(accessAuthentication.canAccessResourcesOf(otherRealmRunAsAuthentication.token()), is(false));
        }
        // different user
        User differentUsername = new User(
            randomValueOtherThan(accessUser.principal(), () -> randomAlphaOfLengthBetween(3, 8)),
            randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8))
        );
        Authentication otherUserAuthentication = Authentication.newRealmAuthentication(
            differentUsername,
            randomFrom(otherRealmRef, accessRealmRef)
        );
        assertThat(accessAuthentication.canAccessResourcesOf(otherUserAuthentication), is(false));
        assertThat(accessAuthentication.canAccessResourcesOf(otherUserAuthentication.token()), is(false));
        // different user when run-as
        Authentication otherRunasAuthentication = accessAuthentication.runAs(differentUsername, randomFrom(otherRealmRef, accessRealmRef));
        assertThat(accessAuthentication.canAccessResourcesOf(otherRunasAuthentication), is(false));
        assertThat(accessAuthentication.canAccessResourcesOf(otherRunasAuthentication.token()), is(false));

        // authn API Key run-as
        User runAsUser = randomUser();
        RealmRef runAsRealmRef = randomRealmRef(true);
        Authentication runAsAuthentication = randomApiKeyAuthentication(randomUser(), randomAlphaOfLength(8)).runAs(
            runAsUser,
            runAsRealmRef
        );
        otherRealmAuthentication = Authentication.newRealmAuthentication(runAsUser, otherRealmRef);
        if ((runAsRealmRef.getDomain().realms().contains(otherRealmIdentifier))
            || (otherRealmIdentifier.getType().equals(FileRealmSettings.TYPE)
                && runAsRealmRef.getDomain().realms().stream().anyMatch(r -> FileRealmSettings.TYPE.equals(r.getType())))
            || (otherRealmIdentifier.getType().equals(NativeRealmSettings.TYPE)
                && runAsRealmRef.getDomain().realms().stream().anyMatch(r -> NativeRealmSettings.TYPE.equals(r.getType())))) {
            assertThat(runAsAuthentication.canAccessResourcesOf(otherRealmAuthentication), is(true));
            assertThat(runAsAuthentication.canAccessResourcesOf(otherRealmAuthentication.token()), is(true));
        } else {
            assertThat(runAsAuthentication.canAccessResourcesOf(otherRealmAuthentication), is(false));
            assertThat(runAsAuthentication.canAccessResourcesOf(otherRealmAuthentication.token()), is(false));
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
            out.setTransportVersion(TransportVersions.V_8_0_0);
            test.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setTransportVersion(TransportVersions.V_8_0_0);
            Authentication testBack = new Authentication(in);
            assertThat(testBack.getDomain(), nullValue());
            assertThat(testBack.isAssignedToDomain(), is(false));
        }
    }

    public void testCrossClusterAccessAuthentication() throws IOException {
        final String crossClusterAccessApiKeyId = ESTestCase.randomAlphaOfLength(20);
        final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo = randomCrossClusterAccessSubjectInfo();
        final Authentication authentication = AuthenticationTestHelper.builder()
            .crossClusterAccess(crossClusterAccessApiKeyId, crossClusterAccessSubjectInfo)
            .build(false);

        assertThat(authentication.getAuthenticationType(), equalTo(Authentication.AuthenticationType.API_KEY));
        assertThat(authentication.getEffectiveSubject(), sameInstance(authentication.getAuthenticatingSubject()));
        assertThat(
            authentication.getEffectiveSubject().getUser(),
            equalTo(AuthenticationTestHelper.stripRoles(authentication.getEffectiveSubject().getUser()))
        );
        assertThat(authentication.isCrossClusterAccess(), is(true));
        assertThat(
            authentication.getAuthenticatingSubject().getRealm(),
            equalTo(
                new RealmRef(
                    AuthenticationField.CROSS_CLUSTER_ACCESS_REALM_NAME,
                    AuthenticationField.CROSS_CLUSTER_ACCESS_REALM_TYPE,
                    authentication.getAuthenticatingSubject().getRealm().getNodeName()
                )
            )
        );
        assertThat(
            authentication.getAuthenticatingSubject().getMetadata(),
            hasEntry(AuthenticationField.API_KEY_ID_KEY, crossClusterAccessApiKeyId)
        );
        assertThat(
            authentication.getAuthenticatingSubject().getMetadata(),
            hasEntry(AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY, crossClusterAccessSubjectInfo.getAuthentication())
        );
        assertThat(
            authentication.getAuthenticatingSubject().getMetadata(),
            hasEntry(
                AuthenticationField.CROSS_CLUSTER_ACCESS_ROLE_DESCRIPTORS_KEY,
                crossClusterAccessSubjectInfo.getRoleDescriptorsBytesList()
            )
        );

        // Serialization round-trip
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            authentication.writeTo(out);
            final StreamInput in = out.bytes().streamInput();
            final Authentication deserialized = new Authentication(in);
            assertThat(deserialized, equalTo(authentication));
        }
    }

    public void testSupportsRunAs() {
        final Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            settingsBuilder.putList(AnonymousUser.ROLES_SETTING.getKey(), randomList(1, 3, () -> randomAlphaOfLengthBetween(3, 8)));
        }
        final AnonymousUser anonymousUser = new AnonymousUser(settingsBuilder.build());

        // Both realm authentication and a token for it can run-as
        assertThat(AuthenticationTestHelper.builder().realm().build(false).supportsRunAs(anonymousUser), is(true));
        assertThat(AuthenticationTestHelper.builder().realm().build(false).token().supportsRunAs(anonymousUser), is(true));
        // but not when it is already a run-as
        assertThat(AuthenticationTestHelper.builder().realm().build(true).supportsRunAs(anonymousUser), is(false));
        assertThat(AuthenticationTestHelper.builder().realm().build(true).token().supportsRunAs(anonymousUser), is(false));

        // API Key or its token both can run-as
        assertThat(AuthenticationTestHelper.builder().apiKey().build(false).supportsRunAs(anonymousUser), is(true));
        assertThat(AuthenticationTestHelper.builder().apiKey().build(false).token().supportsRunAs(anonymousUser), is(true));
        // But not when it already run-as another user
        assertThat(AuthenticationTestHelper.builder().apiKey().runAs().build().supportsRunAs(anonymousUser), is(false));

        // Service account cannot run-as
        assertThat(AuthenticationTestHelper.builder().serviceAccount().build().supportsRunAs(anonymousUser), is(false));

        // internal user cannot run-as
        assertThat(AuthenticationTestHelper.builder().internal().build().supportsRunAs(anonymousUser), is(false));

        // Neither anonymous user nor its token can run-as
        assertThat(AuthenticationTestHelper.builder().anonymous(anonymousUser).build().supportsRunAs(anonymousUser), is(false));
        assertThat(AuthenticationTestHelper.builder().anonymous(anonymousUser).build().token().supportsRunAs(anonymousUser), is(false));

        // Remote access cannot run-as
        assertThat(AuthenticationTestHelper.builder().crossClusterAccess().build().supportsRunAs(anonymousUser), is(false));
    }

    private void assertCanAccessResources(Authentication authentication0, Authentication authentication1) {
        assertTrue(authentication0.canAccessResourcesOf(authentication1));
        assertTrue(authentication1.canAccessResourcesOf(authentication0));
    }

    public void testToXContentWithApiKey() throws IOException {
        final String apiKeyId = randomAlphaOfLength(20);
        final Authentication authentication1 = randomApiKeyAuthentication(randomUser(), apiKeyId);
        final String apiKeyName = (String) authentication1.getAuthenticatingSubject()
            .getMetadata()
            .get(AuthenticationField.API_KEY_NAME_KEY);
        runWithAuthenticationToXContent(
            authentication1,
            m -> assertThat(
                m,
                hasEntry("api_key", apiKeyName != null ? Map.of("id", apiKeyId, "name", apiKeyName) : Map.of("id", apiKeyId))
            )
        );

        final Authentication authentication2 = authentication1.runAs(randomUser(), randomRealmRef(false));
        runWithAuthenticationToXContent(authentication2, m -> assertThat(m, not(hasKey("api_key"))));
    }

    public void testToXContentWithCrossClusterAccess() throws IOException {
        final String apiKeyId = randomAlphaOfLength(20);
        final Authentication authentication = AuthenticationTestHelper.builder()
            .crossClusterAccess(apiKeyId, randomCrossClusterAccessSubjectInfo())
            .build(false);
        final String apiKeyName = (String) authentication.getAuthenticatingSubject()
            .getMetadata()
            .get(AuthenticationField.API_KEY_NAME_KEY);
        runWithAuthenticationToXContent(
            authentication,
            m -> assertThat(
                m,
                hasEntry("api_key", apiKeyName != null ? Map.of("id", apiKeyId, "name", apiKeyName) : Map.of("id", apiKeyId))
            )
        );
    }

    public void testToXContentWithServiceAccount() throws IOException {
        final Authentication authentication1 = randomServiceAccountAuthentication();
        final String tokenName = (String) authentication1.getAuthenticatingSubject()
            .getMetadata()
            .get(ServiceAccountSettings.TOKEN_NAME_FIELD);
        final String tokenType = ServiceAccountSettings.REALM_TYPE
            + "_"
            + authentication1.getAuthenticatingSubject().getMetadata().get(ServiceAccountSettings.TOKEN_SOURCE_FIELD);
        runWithAuthenticationToXContent(
            authentication1,
            m -> assertThat(m, hasEntry("token", Map.of("name", tokenName, "type", tokenType)))
        );
    }

    public void testBwcWithStoredAuthenticationHeaders() throws IOException {
        // Version 6.6.1
        final String headerV6 = "p/HxAgANZWxhc3RpYy1hZG1pbgEJc3VwZXJ1c2VyCgAAAAEABG5vZGUFZmlsZTEEZmlsZQA=";
        final Authentication authenticationV6 = AuthenticationContextSerializer.decode(headerV6);
        assertThat(authenticationV6.getEffectiveSubject().getTransportVersion(), equalTo(TransportVersion.fromId(6_06_01_99)));
        assertThat(authenticationV6.getEffectiveSubject().getUser(), equalTo(new User("elastic-admin", "superuser")));
        assertThat(authenticationV6.getAuthenticationType(), equalTo(Authentication.AuthenticationType.REALM));
        assertThat(authenticationV6.isRunAs(), is(false));
        assertThat(authenticationV6.encode(), equalTo(headerV6));

        // Rewrite for a different version
        final TransportVersion newVersion = TransportVersionUtils.randomCompatibleVersion(random());
        final Authentication rewrittenAuthentication = authenticationV6.maybeRewriteForOlderVersion(newVersion);
        assertThat(rewrittenAuthentication.getEffectiveSubject().getTransportVersion(), equalTo(newVersion));
        assertThat(rewrittenAuthentication.getEffectiveSubject().getUser(), equalTo(authenticationV6.getEffectiveSubject().getUser()));
        assertThat(rewrittenAuthentication.getAuthenticationType(), equalTo(Authentication.AuthenticationType.REALM));
        assertThat(rewrittenAuthentication.isRunAs(), is(false));

        // Version 7.2.1
        final String headerV7 = "p72sAwANZWxhc3RpYy1hZG1pbgENX2VzX3Rlc3Rfcm9vdAoAAAABAARub2RlBWZpbGUxBGZpbGUAAAoA";
        final Authentication authenticationV7 = AuthenticationContextSerializer.decode(headerV7);
        assertThat(authenticationV7.getEffectiveSubject().getTransportVersion(), equalTo(TransportVersion.fromId(7_02_01_99)));
        assertThat(authenticationV7.getEffectiveSubject().getUser(), equalTo(new User("elastic-admin", "_es_test_root")));
        assertThat(authenticationV7.getAuthenticationType(), equalTo(Authentication.AuthenticationType.REALM));
        assertThat(authenticationV7.isRunAs(), is(false));
        assertThat(authenticationV7.encode(), equalTo(headerV7));
    }

    public void testMaybeRewriteForOlderVersionWithCrossClusterAccessThrowsOnUnsupportedVersion() {
        final Authentication authentication = randomBoolean()
            ? AuthenticationTestHelper.builder().crossClusterAccess().build()
            : AuthenticationTestHelper.builder().build();

        final TransportVersion versionBeforeCrossClusterAccessRealm = TransportVersionUtils.getPreviousVersion(
            RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY
        );
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_7_17_0,  // the minimum compatible version of 8.x
            versionBeforeCrossClusterAccessRealm
        );

        if (authentication.isCrossClusterAccess()) {
            final var ex = expectThrows(IllegalArgumentException.class, () -> authentication.maybeRewriteForOlderVersion(version));
            assertThat(
                ex.getMessage(),
                containsString(
                    "versions of Elasticsearch before ["
                        + RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY.toReleaseVersion()
                        + "] can't handle cross cluster access authentication and attempted to rewrite for ["
                        + version.toReleaseVersion()
                        + "]"
                )
            );
        } else {
            // Assert that rewriting took place; the details of rewriting logic are checked in other tests
            assertThat(authentication.maybeRewriteForOlderVersion(version), not(equalTo(authentication)));
        }
    }

    public void testMaybeRewriteForOlderVersionWithCrossClusterAccessRewritesAuthenticationInMetadata() throws IOException {
        final TransportVersion crossClusterAccessRealmVersion =
            RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY;
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            crossClusterAccessRealmVersion,
            TransportVersion.current()
        );
        final Authentication innerAuthentication = AuthenticationTestHelper.builder().transportVersion(version).build();
        final Authentication authentication = AuthenticationTestHelper.builder()
            .crossClusterAccess(
                randomAlphaOfLength(20),
                new CrossClusterAccessSubjectInfo(innerAuthentication, RoleDescriptorsIntersection.EMPTY)
            )
            .build();
        final TransportVersion maybeOldVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            crossClusterAccessRealmVersion,
            version
        );

        final Authentication actual = authentication.maybeRewriteForOlderVersion(maybeOldVersion);

        final Authentication innerActualAuthentication = (Authentication) actual.getAuthenticatingSubject()
            .getMetadata()
            .get(AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY);
        assertThat(innerActualAuthentication, equalTo(innerAuthentication.maybeRewriteForOlderVersion(maybeOldVersion)));
    }

    public void testMaybeRewriteMetadataForCrossClusterAccessAuthentication() throws IOException {
        final Authentication innerAuthentication = AuthenticationTestHelper.builder().build();
        final Authentication authentication = AuthenticationTestHelper.builder()
            .crossClusterAccess(
                randomAlphaOfLength(20),
                new CrossClusterAccessSubjectInfo(innerAuthentication, RoleDescriptorsIntersection.EMPTY)
            )
            .build();
        // pick a version before that of the authentication instance to force a rewrite
        final TransportVersion olderVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.MINIMUM_COMPATIBLE,
            TransportVersionUtils.getPreviousVersion(authentication.getEffectiveSubject().getTransportVersion())
        );

        final Map<String, Object> rewrittenMetadata = Authentication.maybeRewriteMetadataForCrossClusterAccessAuthentication(
            olderVersion,
            authentication
        );

        final Authentication innerRewritten = (Authentication) rewrittenMetadata.get(
            AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY
        );
        assertThat(innerRewritten, equalTo(innerAuthentication.maybeRewriteForOlderVersion(olderVersion)));
    }

    public void testCopyWithFilteredMetadataFields() {
        final Map<String, Object> metadataToKeep = ESTestCase.randomMap(
            0,
            10,
            () -> new Tuple<>(ESTestCase.randomAlphaOfLength(20), ESTestCase.randomAlphaOfLength(10))
        );
        final Map<String, Object> allMetadata = new HashMap<>(metadataToKeep);
        allMetadata.putAll(
            ESTestCase.randomMap(0, 10, () -> new Tuple<>(ESTestCase.randomAlphaOfLength(30), ESTestCase.randomAlphaOfLength(10)))
        );
        // Using service account since it allows us to easily specific metadata, but will not fail instantiation if any service-account
        // specific metadata is missing
        final Authentication authentication = AuthenticationTestHelper.builder().serviceAccount().metadata(Map.copyOf(allMetadata)).build();

        final Set<String> fieldsToKeep = metadataToKeep.keySet();
        final Map<String, Object> actualMetadata = authentication.copyWithFilteredMetadataFields(fieldsToKeep)
            .getAuthenticatingSubject()
            .getMetadata();

        assertThat(actualMetadata, equalTo(metadataToKeep));

        // Test removing all metadata
        assertThat(authentication.copyWithEmptyMetadata().getAuthenticatingSubject().getMetadata(), is(anEmptyMap()));
    }

    public void testMaybeRewriteForOlderVersionErasesDomainForVersionsBeforeDomains() {
        final TransportVersion olderVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_7_17_0,
            TransportVersionUtils.getPreviousVersion(Authentication.VERSION_REALM_DOMAINS)
        );
        final Authentication authentication = AuthenticationTestHelper.builder()
            .realm() // randomize to test both when realm is null on the original auth and non-null, instead of setting `underDomain`
            .transportVersion(TransportVersionUtils.randomVersionBetween(random(), Authentication.VERSION_REALM_DOMAINS, null))
            .build();
        assertThat(authentication.getEffectiveSubject().getTransportVersion().after(olderVersion), is(true));

        final Authentication actual = authentication.maybeRewriteForOlderVersion(olderVersion);

        assertThat(actual.getEffectiveSubject().getTransportVersion(), equalTo(olderVersion));
        assertThat(actual.getAuthenticatingSubject().getRealm().getDomain(), nullValue());
        assertThat(actual.getEffectiveSubject().getRealm().getDomain(), nullValue());
    }

    public void testMaybeRewriteForOlderVersionDoesNotEraseDomainForVersionsAfterDomains() {
        final TransportVersion olderVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            Authentication.VERSION_REALM_DOMAINS,
            // Don't include CURRENT, so we always have at least one newer version available below
            TransportVersionUtils.getPreviousVersion()
        );
        TransportVersion transportVersion = TransportVersionUtils.randomVersionBetween(random(), olderVersion, null);
        final Authentication authentication = AuthenticationTestHelper.builder()
            .realm() // randomize to test both when realm is null on the original auth and non-null, instead of setting `underDomain`
            // Use CURRENT to force newer version in case randomVersionBetween above picks olderVersion
            .transportVersion(transportVersion.equals(olderVersion) ? TransportVersion.current() : transportVersion)
            .build();

        final Authentication actual = authentication.maybeRewriteForOlderVersion(olderVersion);

        assertThat(
            actual.getAuthenticatingSubject().getRealm().getDomain(),
            equalTo(authentication.getAuthenticatingSubject().getRealm().getDomain())
        );
        assertThat(
            actual.getEffectiveSubject().getRealm().getDomain(),
            equalTo(authentication.getEffectiveSubject().getRealm().getDomain())
        );
    }

    public void testToCrossClusterAccess() {
        final User creator = randomUser();
        final String apiKeyId = randomAlphaOfLength(42);
        final Authentication apiKeyAuthentication = AuthenticationTestHelper.builder().apiKey(apiKeyId).user(creator).build(false);
        final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo = randomCrossClusterAccessSubjectInfo();

        final Authentication actualAuthentication = apiKeyAuthentication.toCrossClusterAccess(crossClusterAccessSubjectInfo);

        assertThat(actualAuthentication.isCrossClusterAccess(), is(true));
        assertThat(actualAuthentication.getEffectiveSubject().getUser(), equalTo(AuthenticationTestHelper.stripRoles(creator)));
        assertThat(actualAuthentication.getAuthenticatingSubject().getMetadata(), hasEntry(AuthenticationField.API_KEY_ID_KEY, apiKeyId));
        assertThat(
            actualAuthentication.getAuthenticatingSubject().getMetadata(),
            hasEntry(AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY, crossClusterAccessSubjectInfo.getAuthentication())
        );
        assertThat(
            actualAuthentication.getAuthenticatingSubject().getMetadata(),
            hasEntry(
                AuthenticationField.CROSS_CLUSTER_ACCESS_ROLE_DESCRIPTORS_KEY,
                crossClusterAccessSubjectInfo.getRoleDescriptorsBytesList()
            )
        );
    }

    public void testMaybeRewriteRealmRef() {
        final RealmRef realmRefWithDomain = AuthenticationTests.randomRealmRef(true);
        assertThat(realmRefWithDomain.getDomain(), notNullValue());

        assertThat(
            Authentication.maybeRewriteRealmRef(
                TransportVersionUtils.randomVersionBetween(
                    random(),
                    null,
                    TransportVersionUtils.getPreviousVersion(Authentication.VERSION_REALM_DOMAINS)
                ),
                realmRefWithDomain
            ).getDomain(),
            nullValue()
        );

        assertThat(
            Authentication.maybeRewriteRealmRef(
                TransportVersionUtils.randomVersionBetween(random(), Authentication.VERSION_REALM_DOMAINS, null),
                realmRefWithDomain
            ),
            equalTo(realmRefWithDomain)
        );
    }

    public void testMaybeRewriteMetadataForApiKeyRoleDescriptorsWithRemoteIndices() {
        final String apiKeyId = randomAlphaOfLengthBetween(1, 10);
        final String apiKeyName = randomAlphaOfLengthBetween(1, 10);
        final Map<String, Object> metadata = Map.ofEntries(
            entry(AuthenticationField.API_KEY_ID_KEY, apiKeyId),
            entry(AuthenticationField.API_KEY_NAME_KEY, apiKeyName),
            entry(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, new BytesArray("""
                {"base_role":{"cluster":["all"],
                "remote_indices":{"names":["logs-*"],"privileges":["read"],"clusters":["my_cluster*","other_cluster"]}}
                }""")),
            entry(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, new BytesArray("""
                {"limited_by_role":{"cluster":["*"],
                "remote_indices":{"names":["logs-*-*"],"privileges":["write"],"clusters":["my_cluster*"]}}
                }"""))
        );

        final Authentication original = AuthenticationTestHelper.builder()
            .apiKey()
            .metadata(metadata)
            .transportVersion(RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY)
            .build();

        // pick a version before that of the authentication instance to force a rewrite
        final TransportVersion olderVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            Authentication.VERSION_API_KEY_ROLES_AS_BYTES,
            TransportVersionUtils.getPreviousVersion(original.getEffectiveSubject().getTransportVersion())
        );

        final Map<String, Object> rewrittenMetadata = original.maybeRewriteForOlderVersion(olderVersion)
            .getEffectiveSubject()
            .getMetadata();
        assertThat(rewrittenMetadata.keySet(), equalTo(original.getAuthenticatingSubject().getMetadata().keySet()));
        assertThat(
            ((BytesReference) rewrittenMetadata.get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY)).toBytesRef(),
            equalTo(new BytesArray("""
                {"base_role":{"cluster":["all"]}}""").toBytesRef())
        );
        assertThat(
            ((BytesReference) rewrittenMetadata.get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)).toBytesRef(),
            equalTo(new BytesArray("""
                {"limited_by_role":{"cluster":["*"]}}""").toBytesRef())
        );
    }

    public void testMaybeRewriteMetadataForApiKeyRoleDescriptorsWithRemoteCluster() {
        final String apiKeyId = randomAlphaOfLengthBetween(1, 10);
        final String apiKeyName = randomAlphaOfLengthBetween(1, 10);
        final Map<String, Object> metadata = Map.ofEntries(
            entry(AuthenticationField.API_KEY_ID_KEY, apiKeyId),
            entry(AuthenticationField.API_KEY_NAME_KEY, apiKeyName),
            entry(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, new BytesArray("""
                {"base_role":{"cluster":["all"],
                 "remote_cluster":[{"privileges":["monitor_enrich"],"clusters":["*"]}]
                }}""")),
            entry(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, new BytesArray("""
                {"limited_by_role":{"cluster":["*"],
                 "remote_cluster":[{"privileges":["monitor_enrich"],"clusters":["*"]}]
                }}"""))
        );

        final Authentication original = AuthenticationTestHelper.builder()
            .apiKey()
            .metadata(metadata)
            .transportVersion(ROLE_REMOTE_CLUSTER_PRIVS)
            .build();

        // pick a version before that of the authentication instance to force a rewrite
        final TransportVersion olderVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            Authentication.VERSION_API_KEY_ROLES_AS_BYTES,
            TransportVersionUtils.getPreviousVersion(original.getEffectiveSubject().getTransportVersion())
        );

        final Map<String, Object> rewrittenMetadata = original.maybeRewriteForOlderVersion(olderVersion)
            .getEffectiveSubject()
            .getMetadata();
        assertThat(rewrittenMetadata.keySet(), equalTo(original.getAuthenticatingSubject().getMetadata().keySet()));
        assertThat(
            ((BytesReference) rewrittenMetadata.get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY)).toBytesRef(),
            equalTo(new BytesArray("""
                {"base_role":{"cluster":["all"]}}""").toBytesRef())
        );
        assertThat(
            ((BytesReference) rewrittenMetadata.get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)).toBytesRef(),
            equalTo(new BytesArray("""
                {"limited_by_role":{"cluster":["*"]}}""").toBytesRef())
        );
    }

    public void testMaybeRemoveRemoteIndicesFromRoleDescriptors() {
        final boolean includeClusterPrivileges = randomBoolean();
        final BytesReference roleWithoutRemoteIndices = new BytesArray(Strings.format("""
            {"user_role":{%s}}""", includeClusterPrivileges ? """
            "cluster":["all"]""" : ""));

        // role without remote indices should stay the same
        assertThat(
            roleWithoutRemoteIndices.toBytesRef(),
            equalTo(Authentication.maybeRemoveRemoteIndicesFromRoleDescriptors(roleWithoutRemoteIndices).toBytesRef())
        );

        // role with remote indices should be filtered
        assertThat(
            roleWithoutRemoteIndices.toBytesRef(),
            equalTo(Authentication.maybeRemoveRemoteIndicesFromRoleDescriptors(new BytesArray(Strings.format("""
                {"user_role":{%s
                "remote_indices":{"names":["logs-*"],"privileges":["read"],"clusters":["my_cluster*","other_cluster"]}}
                }""", includeClusterPrivileges ? """
                "cluster":["all"],""" : ""))).toBytesRef())
        );

        // check null value
        assertThat(null, equalTo(Authentication.maybeRemoveRemoteIndicesFromRoleDescriptors(null)));

        // and an empty map
        final BytesReference empty = randomBoolean() ? new BytesArray("""
            {}""") : new BytesArray("");
        assertThat(empty.toBytesRef(), equalTo(Authentication.maybeRemoveRemoteIndicesFromRoleDescriptors(empty).toBytesRef()));
    }

    private void runWithAuthenticationToXContent(Authentication authentication, Consumer<Map<String, Object>> consumer) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            authentication.toXContent(builder, ToXContent.EMPTY_PARAMS);
            consumer.accept(XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2());
        }
    }

    private void assertCannotAccessResources(Authentication authentication0, Authentication authentication1) {
        assertFalse(authentication0.canAccessResourcesOf(authentication1));
        assertFalse(authentication1.canAccessResourcesOf(authentication0));
    }

    private RealmRef mutateRealm(RealmRef original, String name, String type) {
        return new RealmRef(
            name == null ? original.getName() : name,
            type == null ? original.getType() : type,
            randomBoolean() ? original.getNodeName() : randomAlphaOfLengthBetween(3, 8)
        );
    }

    public static User randomUser() {
        return AuthenticationTestHelper.randomUser();
    }

    public static RealmRef randomRealmRef(boolean underDomain) {
        return AuthenticationTestHelper.randomRealmRef(underDomain);
    }

    public static RealmDomain randomDomain(boolean includeInternal) {
        return AuthenticationTestHelper.randomDomain(includeInternal);
    }

    public static RealmRef randomRealmRef(boolean underDomain, boolean includeInternal) {
        return AuthenticationTestHelper.randomRealmRef(underDomain, includeInternal);
    }

    /**
     * Randomly create an authentication that has either realm or token authentication type.
     * The authentication can have any version from 7.0.0 (or from 8.2.0, if the realm has a domain) to current and random metadata.
     */
    public static Authentication randomAuthentication(User user, RealmRef realmRef) {
        return randomAuthentication(user, realmRef, randomBoolean());
    }

    public static Authentication randomAuthentication(User user, RealmRef realmRef, boolean isRunAs) {
        if (user == null) {
            user = randomUser();
        }
        if (realmRef == null) {
            realmRef = randomRealmRef(false);
        }
        // If the realm is expected to have a domain, we need a version that's at least compatible with domains
        final TransportVersion minVersion = realmRef.getDomain() != null ? Authentication.VERSION_REALM_DOMAINS : TransportVersions.V_7_0_0;
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(random(), minVersion, TransportVersion.current());
        final Map<String, Object> metadata;
        if (randomBoolean()) {
            metadata = Map.of(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        } else {
            metadata = Arrays.stream(randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)))
                .distinct()
                .collect(Collectors.toMap(s -> s, s -> randomAlphaOfLengthBetween(3, 8)));
        }
        return AuthenticationTestHelper.builder().user(user).realmRef(realmRef).transportVersion(version).metadata(metadata).build(isRunAs);
    }

    public static Authentication randomApiKeyAuthentication(User user, String apiKeyId) {
        return randomApiKeyAuthentication(
            user,
            apiKeyId,
            TransportVersionUtils.randomVersionBetween(random(), TransportVersions.V_7_0_0, TransportVersion.current())
        );
    }

    public static Authentication randomApiKeyAuthentication(User user, String apiKeyId, TransportVersion version) {
        return randomApiKeyAuthentication(
            user,
            apiKeyId,
            AuthenticationField.API_KEY_CREATOR_REALM_NAME,
            AuthenticationField.API_KEY_CREATOR_REALM_TYPE,
            version
        );
    }

    public static Authentication randomApiKeyAuthentication(
        User user,
        String apiKeyId,
        String creatorRealmName,
        String creatorRealmType,
        TransportVersion version
    ) {
        return AuthenticationTestHelper.builder()
            .apiKey(apiKeyId)
            .user(user)
            .transportVersion(version)
            .metadata(
                Map.of(
                    AuthenticationField.API_KEY_CREATOR_REALM_NAME,
                    creatorRealmName,
                    AuthenticationField.API_KEY_CREATOR_REALM_TYPE,
                    creatorRealmType
                )
            )
            .build();
    }

    public static Authentication randomServiceAccountAuthentication() {
        return AuthenticationTestHelper.builder().serviceAccount().build();
    }

    public static Authentication randomRealmAuthentication(boolean underDomain) {
        return AuthenticationTestHelper.builder().realm(underDomain).build(false);
    }

    public static Authentication randomInternalAuthentication() {
        return AuthenticationTestHelper.builder().internal().build();
    }

    public static Authentication randomAnonymousAuthentication() {
        return AuthenticationTestHelper.builder().anonymous().build();
    }

    private boolean realmIsSingleton(RealmRef realmRef) {
        return Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE).contains(realmRef.getType());
    }
}
