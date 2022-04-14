/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProfileServiceTests extends ESTestCase {

    public static final String SAMPLE_PROFILE_DOCUMENT_TEMPLATE = """
        {
          "user_profile":  {
            "uid": "%s",
            "enabled": true,
            "user": {
              "username": "Foo",
              "roles": [
                "role1",
                "role2"
              ],
              "realm": {
                "name": "realm_name_1",
                "type": "realm_type_1",
                "domain": {
                  "name": "domainA",
                  "realms": [
                    { "name": "realm_name_1", "type": "realm_type_1" },
                    { "name": "realm_name_2", "type": "realm_type_2" }
                  ]
                },
                "node_name": "node1"
              },
              "email": "foo@example.com",
              "full_name": "User Foo"
            },
            "last_synchronized": %s,
            "labels": {
            },
            "application_data": {
              "app1": { "name": "app1" },
              "app2": { "name": "app2" }
            }
          }
        }
        """;
    private ThreadPool threadPool;
    private Client client;
    private SecurityIndexManager profileIndex;
    private ProfileService profileService;

    @Before
    public void prepare() {
        threadPool = Mockito.spy(
            new TestThreadPool(
                "api key service tests",
                new FixedExecutorBuilder(
                    Settings.EMPTY,
                    SECURITY_CRYPTO_THREAD_POOL_NAME,
                    1,
                    1000,
                    "xpack.security.crypto.thread_pool",
                    false
                )
            )
        );
        this.client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        this.profileIndex = SecurityMocks.mockSecurityIndexManager(SECURITY_PROFILE_ALIAS);
        this.profileService = new ProfileService(Settings.EMPTY, Clock.systemUTC(), client, profileIndex, threadPool);
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testGetProfileByUid() {
        final String uid = randomAlphaOfLength(20);
        doAnswer(invocation -> {
            final GetRequest getRequest = (GetRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            final ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocation.getArguments()[2];
            client.get(getRequest, listener);
            return null;
        }).when(client).execute(eq(GetAction.INSTANCE), any(GetRequest.class), anyActionListener());

        final long lastSynchronized = Instant.now().toEpochMilli();
        mockGetRequest(uid, lastSynchronized);

        final PlainActionFuture<Profile> future = new PlainActionFuture<>();

        final Set<String> dataKeys = randomFrom(Set.of("app1"), Set.of("app2"), Set.of("app1", "app2"), Set.of());

        profileService.getProfile(uid, dataKeys, future);
        final Profile profile = future.actionGet();

        final Map<String, Object> applicationData = new HashMap<>();

        if (dataKeys != null && dataKeys.contains("app1")) {
            applicationData.put("app1", Map.of("name", "app1"));
        }

        if (dataKeys != null && dataKeys.contains("app2")) {
            applicationData.put("app2", Map.of("name", "app2"));
        }

        assertThat(
            profile,
            equalTo(
                new Profile(
                    uid,
                    true,
                    lastSynchronized,
                    new Profile.ProfileUser("Foo", List.of("role1", "role2"), "realm_name_1", "domainA", "foo@example.com", "User Foo"),
                    Map.of(),
                    applicationData,
                    new Profile.VersionControl(1, 0)
                )
            )
        );
    }

    public void testActivateProfileShouldFailIfSubjectTypeIsNotUser() {
        final Authentication authentication;
        if (randomBoolean()) {
            final User user = new User(randomAlphaOfLengthBetween(5, 8));
            authentication = AuthenticationTests.randomApiKeyAuthentication(user, randomAlphaOfLength(20));
        } else {
            authentication = AuthenticationTests.randomServiceAccountAuthentication();
        }

        final PlainActionFuture<Profile> future1 = new PlainActionFuture<>();
        profileService.activateProfile(authentication, future1);
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, future1::actionGet);
        assertThat(e1.getMessage(), containsString("profile is supported for user only"));
    }

    public void testActivateProfileShouldFailForInternalUser() {
        final User user = randomFrom(SystemUser.INSTANCE, XPackUser.INSTANCE, XPackSecurityUser.INSTANCE, AsyncSearchUser.INSTANCE);
        final Authentication.RealmRef realmRef = new Authentication.RealmRef(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLength(5)
        );
        final Authentication authentication = new Authentication(user, realmRef, null);

        final PlainActionFuture<Profile> future1 = new PlainActionFuture<>();
        profileService.activateProfile(authentication, future1);
        final IllegalStateException e1 = expectThrows(IllegalStateException.class, future1::actionGet);
        assertThat(e1.getMessage(), containsString("profile should not be created for internal user"));
    }

    public void testFailureForParsingDifferentiator() throws IOException {
        final PlainActionFuture<Profile> future1 = new PlainActionFuture<>();
        profileService.incrementDifferentiatorAndCreateNewProfile(
            mock(Subject.class),
            randomProfileDocument(randomAlphaOfLength(20)),
            future1
        );
        final ElasticsearchException e1 = expectThrows(ElasticsearchException.class, future1::actionGet);
        assertThat(e1.getMessage(), containsString("does not contain any underscore character"));

        final PlainActionFuture<Profile> future2 = new PlainActionFuture<>();
        profileService.incrementDifferentiatorAndCreateNewProfile(
            mock(Subject.class),
            randomProfileDocument(randomAlphaOfLength(20) + "_"),
            future2
        );
        final ElasticsearchException e2 = expectThrows(ElasticsearchException.class, future2::actionGet);
        assertThat(e2.getMessage(), containsString("does not contain a differentiator"));

        final PlainActionFuture<Profile> future3 = new PlainActionFuture<>();
        profileService.incrementDifferentiatorAndCreateNewProfile(
            mock(Subject.class),
            randomProfileDocument(randomAlphaOfLength(20) + "_" + randomAlphaOfLengthBetween(1, 3)),
            future3
        );
        final ElasticsearchException e3 = expectThrows(ElasticsearchException.class, future3::actionGet);
        assertThat(e3.getMessage(), containsString("differentiator is not a number"));
    }

    private void mockGetRequest(String uid, long lastSynchronized) {
        final String source = SAMPLE_PROFILE_DOCUMENT_TEMPLATE.formatted(uid, lastSynchronized);

        SecurityMocks.mockGetRequest(client, SECURITY_PROFILE_ALIAS, "profile_" + uid, new BytesArray(source));
    }

    private ProfileDocument randomProfileDocument(String uid) {
        return new ProfileDocument(
            uid,
            true,
            randomLong(),
            new ProfileDocument.ProfileDocumentUser(
                randomAlphaOfLengthBetween(3, 8),
                List.of(),
                AuthenticationTests.randomRealmRef(randomBoolean()),
                "foo@example.com",
                null
            ),
            Map.of(),
            null
        );
    }
}
