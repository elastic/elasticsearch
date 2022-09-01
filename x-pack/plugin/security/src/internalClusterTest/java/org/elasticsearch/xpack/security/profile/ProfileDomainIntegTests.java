/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.INTERNAL_SECURITY_PROFILE_INDEX_8;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ProfileDomainIntegTests extends AbstractProfileIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // Register both file and native realms under the same domain
        builder.put("xpack.security.authc.domains.my_domain.realms", "file,index");
        return builder.build();
    }

    public void testActivateProfileUnderDomain() {
        // Activate 1st time with the file realm user
        final Profile profile1 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        assertThat(profile1.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile1.user().realmName(), equalTo("file"));
        assertThat(profile1.user().domainName(), equalTo("my_domain"));
        assertThat(profile1.user().email(), nullValue());
        assertThat(profile1.user().fullName(), nullValue());

        // Get the profile back by ID
        assertThat(getProfile(profile1.uid(), Set.of()), equalTo(profile1));

        // Activate 2nd time with the native realm user and it should get the same profile
        // because they are under the same domain. User fields are updated to the native user's info
        final Profile profile2 = doActivateProfile(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD);
        assertThat(profile2.uid(), equalTo(profile1.uid()));
        assertThat(profile2.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile2.user().realmName(), equalTo("index"));
        assertThat(profile2.user().domainName(), equalTo("my_domain"));
        assertThat(profile2.user().email(), equalTo(RAC_USER_NAME + "@example.com"));
        assertThat(profile2.user().fullName(), nullValue());
        assertThat(profile2.user().roles(), containsInAnyOrder(RAC_ROLE, NATIVE_RAC_ROLE));

        // Activate 3rd time with the file realm user again and it should get the same profile
        // User fields are updated to the file user's info again
        final Profile profile3 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        assertThat(profile3.uid(), equalTo(profile1.uid()));
        assertThat(profile3.user().realmName(), equalTo("file"));
        assertThat(profile3.user().domainName(), equalTo("my_domain"));
        assertThat(profile3.user().email(), nullValue());
        assertThat(profile3.user().fullName(), nullValue());
        assertThat(profile3.user().roles(), containsInAnyOrder(RAC_ROLE));

        // Update native rac user
        final PutUserRequest putUserRequest1 = new PutUserRequest();
        putUserRequest1.username(RAC_USER_NAME);
        putUserRequest1.roles(RAC_ROLE, "superuser");
        putUserRequest1.email(null);
        putUserRequest1.fullName("Native RAC User");
        assertThat(client().execute(PutUserAction.INSTANCE, putUserRequest1).actionGet().created(), is(false));

        // Activate again with the native RAC user to the same profile
        final Profile profile4 = doActivateProfile(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD);
        assertThat(profile4.uid(), equalTo(profile1.uid()));
        assertThat(profile4.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile4.user().realmName(), equalTo("index"));
        assertThat(profile4.user().domainName(), equalTo("my_domain"));
        assertThat(profile4.user().email(), nullValue());
        assertThat(profile4.user().fullName(), equalTo("Native RAC User"));
        assertThat(profile4.user().roles(), containsInAnyOrder(RAC_ROLE, "superuser"));

        // Get by ID immediately should get the same document and content as the response to activate
        assertThat(getProfile(profile1.uid(), Set.of()), equalTo(profile4));
    }

    public void testGetProfileByAuthenticationUnderDomain() {
        final ProfileService profileService = getInstanceFromRandomNode(ProfileService.class);

        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final RealmConfig.RealmIdentifier realmIdentifier1 = new RealmConfig.RealmIdentifier("realm_type_1", "realm_name_1");
        final RealmConfig.RealmIdentifier realmIdentifier2 = new RealmConfig.RealmIdentifier("realm_type_2", "realm_name_2");

        // Domain name does not matter
        final String domainName = randomFrom("domainA", randomAlphaOfLengthBetween(5, 12));
        // The recorded realm is realm_name_1, domain realms must contain the recorded realm
        final Set<RealmConfig.RealmIdentifier> domainRealms = randomBoolean()
            ? Set.of(realmIdentifier1, realmIdentifier2)
            : Set.of(realmIdentifier1);
        final RealmDomain realmDomain = new RealmDomain(domainName, domainRealms);

        final RealmConfig.RealmIdentifier authenticationRealmIdentifier = randomFrom(domainRealms);

        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("Foo"))
            .realmRef(
                new Authentication.RealmRef(
                    authenticationRealmIdentifier.getName(),
                    authenticationRealmIdentifier.getType(),
                    nodeName,
                    realmDomain
                )
            )
            .build(false);
        final Subject subject = authentication.getEffectiveSubject();

        // Profile does not exist yet
        final PlainActionFuture<ProfileService.VersionedDocument> future1 = new PlainActionFuture<>();
        profileService.searchVersionedDocumentForSubject(subject, future1);
        assertThat(future1.actionGet(), nullValue());

        // Index the document so it can be found
        // The document is created with realm_name_1 under domainA (member realms are realm_name_1 and realm_name_2)
        final String uid2 = indexDocument();
        final PlainActionFuture<ProfileService.VersionedDocument> future2 = new PlainActionFuture<>();
        profileService.searchVersionedDocumentForSubject(subject, future2);
        final ProfileService.VersionedDocument versionedDocument = future2.actionGet();
        assertThat(versionedDocument, notNullValue());
        assertThat(versionedDocument.doc().uid(), equalTo(uid2));

        // Index it again to trigger duplicate exception
        final String uid3 = indexDocument();
        final PlainActionFuture<ProfileService.VersionedDocument> future3 = new PlainActionFuture<>();
        profileService.searchVersionedDocumentForSubject(subject, future3);
        final ElasticsearchException e3 = expectThrows(ElasticsearchException.class, future3::actionGet);

        assertThat(
            e3.getMessage(),
            containsString(
                "multiple [2] profiles [" + Stream.of(uid2, uid3).sorted().collect(Collectors.joining(",")) + "] found for user [Foo]"
            )
        );
    }

    public void testGetProfileByAuthenticationDomainless() {
        final ProfileService profileService = getInstanceFromRandomNode(ProfileService.class);
        // The document is created with realm_name_1 under domainA (member realms are realm_name_1 and realm_name_2)
        final String uid1 = indexDocument();
        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final RealmConfig.RealmIdentifier realmIdentifier1 = new RealmConfig.RealmIdentifier("realm_type_1", "realm_name_1");
        final RealmConfig.RealmIdentifier realmIdentifier2 = new RealmConfig.RealmIdentifier("realm_type_2", "realm_name_2");

        // Scenario 1
        // The recorded realm_name_1 is no longer part of a domain.
        // Authentication for this realm still works for retrieving the same profile document
        final Authentication authentication1 = AuthenticationTestHelper.builder()
            .user(new User("Foo"))
            .realmRef(new Authentication.RealmRef(realmIdentifier1.getName(), realmIdentifier1.getType(), nodeName))
            .build(false);
        final Subject subject1 = authentication1.getEffectiveSubject();

        final PlainActionFuture<ProfileService.VersionedDocument> future1 = new PlainActionFuture<>();
        profileService.searchVersionedDocumentForSubject(subject1, future1);
        final ProfileService.VersionedDocument versionedDocument1 = future1.actionGet();
        assertThat(versionedDocument1, notNullValue());
        assertThat(versionedDocument1.doc().uid(), equalTo(uid1));

        // Scenario 2
        // The recorded realm_name_1 is no longer part of a domain.
        // Authentication for realm_name_2 (which is still part of domainA) does not work for retrieving the profile document
        final RealmDomain realmDomain1 = new RealmDomain("domainA", Set.of(realmIdentifier2));
        final Authentication authentication2 = AuthenticationTestHelper.builder()
            .user(new User("Foo"))
            .realmRef(new Authentication.RealmRef(realmIdentifier2.getName(), realmIdentifier2.getType(), nodeName, realmDomain1))
            .build(false);
        final Subject subject2 = authentication2.getEffectiveSubject();

        final PlainActionFuture<ProfileService.VersionedDocument> future2 = new PlainActionFuture<>();
        profileService.searchVersionedDocumentForSubject(subject2, future2);
        assertThat(future2.actionGet(), nullValue());

        // Scenario 3
        // Both recorded realm_name_1 and the authentication realm_name_2 are no longer part of a domain.
        final Authentication authentication3 = AuthenticationTestHelper.builder()
            .user(new User("Foo"))
            .realmRef(new Authentication.RealmRef(realmIdentifier2.getName(), realmIdentifier2.getType(), nodeName))
            .build(false);
        final Subject subject3 = authentication3.getEffectiveSubject();

        final PlainActionFuture<ProfileService.VersionedDocument> future3 = new PlainActionFuture<>();
        profileService.searchVersionedDocumentForSubject(subject3, future3);
        assertThat(future3.actionGet(), nullValue());
    }

    public void testGetProfileByAuthenticationWillNotCheckRealmNameForFileOrNativeRealm() {
        // File and native realms are under the same domain, activate the profile from either the realm
        final Profile profile1;
        if (randomBoolean()) {
            profile1 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        } else {
            profile1 = doActivateProfile(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD);
        }
        final ProfileService profileService = getInstanceFromRandomNode(ProfileService.class);

        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final String realmType = randomBoolean() ? FileRealmSettings.TYPE : NativeRealmSettings.TYPE;
        final RealmDomain realmDomain = new RealmDomain(
            randomAlphaOfLengthBetween(3, 8),
            Set.of(
                new RealmConfig.RealmIdentifier(realmType, realmName),
                new RealmConfig.RealmIdentifier(
                    FileRealmSettings.TYPE.equals(realmType) ? NativeRealmSettings.TYPE : FileRealmSettings.TYPE,
                    randomAlphaOfLengthBetween(3, 8)
                )
            )
        );
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef(
            realmName,
            realmType,
            randomAlphaOfLengthBetween(3, 8),
            realmDomain
        );
        final Authentication authentication1 = Authentication.newRealmAuthentication(new User(RAC_USER_NAME), authenticatedBy);

        final PlainActionFuture<Profile> future1 = new PlainActionFuture<>();
        profileService.activateProfile(authentication1, future1);
        assertThat(future1.actionGet().uid(), equalTo(profile1.uid()));
    }

    public void testConcurrentCreationOfNewProfiles() throws InterruptedException {
        // Ensure the index exists because racing on creating and writing to the index could fail with UnavailableShardsException
        indexDocument();

        final String username = randomAlphaOfLengthBetween(5, 12);

        final boolean existingCollision = randomBoolean();
        final String existingUid;
        // Manually create a collision document
        if (existingCollision) {
            final Authentication authentication = assembleAuthentication(username, randomRealmRef());
            final PlainActionFuture<Profile> future = new PlainActionFuture<>();
            getInstanceFromRandomNode(ProfileService.class).activateProfile(authentication, future);
            existingUid = future.actionGet().uid();
            assertThat(existingUid, endsWith("_0"));
            final UpdateRequest updateRequest = client().prepareUpdate(SECURITY_PROFILE_ALIAS, "profile_" + existingUid).setDoc("""
                {
                  "user_profile": {
                    "user": { "username": "%s" }
                  }
                }
                """.formatted("not-" + username), XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).request();
            client().update(updateRequest).actionGet();
            logger.info("manually creating a collision document: [{}]", existingUid);
        } else {
            existingUid = null;
        }

        // All the same user, should create a single profile
        final Thread[] threads = new Thread[randomIntBetween(5, 10)];
        final CountDownLatch readyLatch = new CountDownLatch(threads.length);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final Set<String> allUids = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    final Authentication authentication = assembleAuthentication(username, randomRealmRef());
                    final ProfileService profileService = getInstanceFromRandomNode(ProfileService.class);
                    final PlainActionFuture<Profile> future = new PlainActionFuture<>();
                    profileService.activateProfile(authentication, future);
                    readyLatch.countDown();
                    startLatch.await();
                    try {
                        final String uid = future.actionGet().uid();
                        logger.info("created profile [{}] for authentication [{}]", uid, authentication);
                        allUids.add(uid);
                    } catch (VersionConflictEngineException e) {
                        // Updating existing profile can error with version conflict. This is the current way
                        // of handling racing in updating existing profile.
                        // For this test, it is acceptable to either get the same profile document
                        // or getting a version conflict for optimistic control (NOT document already exists)
                        assertThat(e.getMessage(), containsString("version conflict, required seqNo"));
                    }
                } catch (Exception e) {
                    logger.error(e);
                    fail("caught error when creating new profile: " + e);
                }
            });
            threads[i].start();
        }
        if (readyLatch.await(20, TimeUnit.SECONDS)) {
            startLatch.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
            // Exactly one profile is created
            assertThat("All created profile uids: " + allUids, allUids, hasSize(1));
            final String uid = allUids.iterator().next();
            if (existingCollision) {
                assertThat(uid, endsWith("_1"));
                assertThat(uid.substring(0, uid.length() - 2), equalTo(existingUid.substring(0, existingUid.length() - 2)));
            } else {
                assertThat(uid, endsWith("_0"));
            }
            final Profile profile1 = getProfile(uid, Set.of());
            assertThat(profile1.uid(), equalTo(uid));
            assertThat(profile1.user().username(), equalTo(username));
        } else {
            fail("Not all threads are ready after waiting");
        }
    }

    public void testConcurrentActivateUpdates() throws InterruptedException {
        final Authentication.RealmRef realmRef = randomRealmRef();
        final User originalUser = new User(randomAlphaOfLengthBetween(5, 12));
        final Authentication originalAuthentication = Authentication.newRealmAuthentication(originalUser, realmRef);

        final ProfileService profileService = getInstanceFromRandomNode(ProfileService.class);
        final PlainActionFuture<Profile> originalFuture = new PlainActionFuture<>();
        profileService.activateProfile(originalAuthentication, originalFuture);
        final Profile originalProfile = originalFuture.actionGet();

        final Thread[] threads = new Thread[randomIntBetween(5, 10)];
        final CountDownLatch readyLatch = new CountDownLatch(threads.length);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final Set<Profile> updatedProfiles = ConcurrentHashMap.newKeySet();
        final Authentication updatedAuthentication = Authentication.newRealmAuthentication(
            new User(originalUser.principal(), "foo"),
            realmRef
        );
        // All concurrent activations should succeed because we handle version conflict error and check whether update
        // can be skipped. In this case, they can be skipped because all updates are for the same content.
        // Due to the concurrency nature, there is no guarantee whether an update can succeed or succeed with error handling.
        // We can only be sure that at least one of them will succeed.
        // Other updates may succeed or they may succeed with the error handling. So we cannot assert that the document
        // is only updated once. What we can assert is that they will all be successful (one way or another).
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    final PlainActionFuture<Profile> future = new PlainActionFuture<>();
                    profileService.activateProfile(updatedAuthentication, future);
                    readyLatch.countDown();
                    startLatch.await();
                    final Profile updatedProfile = future.actionGet();
                    assertThat(updatedProfile.uid(), equalTo(originalProfile.uid()));
                    assertThat(updatedProfile.user().roles(), contains("foo"));
                    updatedProfiles.add(updatedProfile);
                } catch (Exception e) {
                    logger.error(e);
                    fail("caught error when activating existing profile: " + e);
                }
            });
            threads[i].start();
        }

        if (readyLatch.await(20, TimeUnit.SECONDS)) {
            startLatch.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
        }

        assertThat(updatedProfiles, not(emptyIterable()));
        final Profile updatedProfile = updatedProfiles.stream().max(Comparator.comparingLong(Profile::lastSynchronized)).orElseThrow();
        // Update again, this time it should simply skip due to grace period of 30 seconds
        final PlainActionFuture<Profile> future = new PlainActionFuture<>();
        profileService.activateProfile(updatedAuthentication, future);
        assertThat(future.actionGet(), equalTo(updatedProfile));
    }

    public void testDifferentiator() {
        String lastUid = null;
        final int differentiatorLimit = 10;
        final int otherRacUserIndex = randomIntBetween(0, differentiatorLimit - 1);
        for (int i = 0; i < differentiatorLimit; i++) {
            String currentUid = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING).uid();
            assertThat(currentUid, endsWith("_" + i));
            if (lastUid != null) {
                // Base uid is identical
                assertThat(currentUid.substring(0, currentUid.length() - 2), equalTo(lastUid.substring(0, lastUid.length() - 2)));
            }
            final String newUsername = i == otherRacUserIndex ? OTHER_RAC_USER_NAME : "some-other-name-" + randomAlphaOfLength(8);
            // Manually update the username to create hash collision
            final UpdateRequest updateRequest = client().prepareUpdate(SECURITY_PROFILE_ALIAS, "profile_" + currentUid).setDoc("""
                {
                  "user_profile": {
                    "user": { "username": "%s" }
                  }
                }
                """.formatted(newUsername), XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).request();
            client().update(updateRequest).actionGet();
            if (newUsername.equals(OTHER_RAC_USER_NAME)) {
                // The manually updated profile document can still be activated by the other rac user
                assertThat(doActivateProfile(OTHER_RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING).uid(), equalTo(currentUid));
            }
            lastUid = currentUid;
        }

        final ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING)
        );
        assertThat(e.getMessage(), containsString("differentiator value is too high"));
    }

    public void testBackoffDepletion() {
        final Subject subject = new Subject(new User(randomAlphaOfLengthBetween(5, 12)), randomRealmRef());
        final ProfileDocument profileDocument = ProfileDocument.fromSubject(subject);

        final ProfileService profileService = getInstanceFromRandomNode(ProfileService.class);
        final PlainActionFuture<Profile> future = new PlainActionFuture<>();
        profileService.getOrCreateProfileWithBackoff(subject, profileDocument, List.of(TimeValue.timeValueMillis(50)).iterator(), future);

        final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("failed to retrieving profile [" + profileDocument.uid() + "] after all retries"));
    }

    public void testProfileDocumentPassCanAccessResourceCheck() {
        Authentication authentication = Authentication.newRealmAuthentication(AuthenticationTests.randomUser(), randomRealmRef());
        if (randomBoolean()) {
            authentication = authentication.token();
        } else {
            authentication = authentication.runAs(AuthenticationTests.randomUser(), randomRealmRef());
            if (randomBoolean()) {
                authentication = authentication.token();
            }
        }
        final Subject subject = authentication.getEffectiveSubject();
        final ProfileService profileService = getInstanceFromRandomNode(ProfileService.class);
        final PlainActionFuture<Profile> future1 = new PlainActionFuture<>();
        profileService.activateProfile(authentication, future1);
        final String uid = future1.actionGet().uid();

        client().execute(RefreshAction.INSTANCE, new RefreshRequest(INTERNAL_SECURITY_PROFILE_INDEX_8)).actionGet();
        final PlainActionFuture<ProfileService.VersionedDocument> future2 = new PlainActionFuture<>();
        profileService.searchVersionedDocumentForSubject(subject, future2);
        final ProfileDocument profileDocument = future2.actionGet().doc();
        assertThat(profileDocument.uid(), equalTo(uid));
        assertThat(subject.canAccessResourcesOf(profileDocument.user().toSubject()), is(true));
    }

    private Authentication.RealmRef randomRealmRef() {
        Authentication.RealmRef realmRef = AuthenticationTests.randomRealmRef(false);
        if (randomBoolean()) {
            realmRef = new Authentication.RealmRef(
                realmRef.getName(),
                realmRef.getType(),
                realmRef.getNodeName(),
                new RealmDomain(
                    "my_domain",
                    Set.of(
                        new RealmConfig.RealmIdentifier("file", "file"),
                        new RealmConfig.RealmIdentifier("native", "index"),
                        new RealmConfig.RealmIdentifier(realmRef.getType(), realmRef.getName())
                    )
                )
            );
        }
        return realmRef;
    }

    private String indexDocument() {
        final String uid = randomAlphaOfLength(20);
        indexDocument(uid);
        return uid;
    }

    private void indexDocument(String uid) {
        String source = ProfileServiceTests.getSampleProfileDocumentSource(
            uid,
            "Foo",
            List.of("role1", "role2"),
            Instant.now().toEpochMilli()
        );
        client().prepareIndex(randomFrom(INTERNAL_SECURITY_PROFILE_INDEX_8, SECURITY_PROFILE_ALIAS))
            .setId("profile_" + uid)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
            .setSource(source, XContentType.JSON)
            .get();
    }

    private Authentication assembleAuthentication(String username, Authentication.RealmRef realmRef) {
        final RealmConfig.RealmIdentifier realmIdentifier = realmRef.getDomain() == null
            ? new RealmConfig.RealmIdentifier(realmRef.getType(), realmRef.getName())
            : randomFrom(realmRef.getDomain().realms());

        return Authentication.newRealmAuthentication(
            new User(username),
            new Authentication.RealmRef(
                realmIdentifier.getName(),
                realmIdentifier.getType(),
                randomAlphaOfLengthBetween(3, 8),
                realmRef.getDomain()
            )
        );
    }
}
