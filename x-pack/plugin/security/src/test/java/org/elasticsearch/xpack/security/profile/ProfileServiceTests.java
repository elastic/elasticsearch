/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequestTests;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.util.concurrent.ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
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
        when(client.prepareSearch(SECURITY_PROFILE_ALIAS)).thenReturn(
            new SearchRequestBuilder(client, SearchAction.INSTANCE).setIndices(SECURITY_PROFILE_ALIAS)
        );
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
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
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
        final Authentication authentication = AuthenticationTestHelper.builder().internal().build();

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

    public void testBuildSearchRequest() {
        final String name = randomAlphaOfLengthBetween(0, 8);
        final int size = randomIntBetween(0, Integer.MAX_VALUE);
        final SuggestProfilesRequest.Hint hint = SuggestProfilesRequestTests.randomHint();
        final SuggestProfilesRequest suggestProfilesRequest = new SuggestProfilesRequest(Set.of(), name, size, hint);

        final SearchRequest searchRequest = profileService.buildSearchRequest(suggestProfilesRequest);
        final SearchSourceBuilder searchSourceBuilder = searchRequest.source();

        assertThat(
            searchSourceBuilder.sorts(),
            equalTo(List.of(new ScoreSortBuilder(), new FieldSortBuilder("user_profile.last_synchronized").order(SortOrder.DESC)))
        );
        assertThat(searchSourceBuilder.size(), equalTo(size));

        assertThat(searchSourceBuilder.query(), instanceOf(BoolQueryBuilder.class));

        final BoolQueryBuilder query = (BoolQueryBuilder) searchSourceBuilder.query();
        if (Strings.hasText(name)) {
            assertThat(
                query.must(),
                equalTo(
                    List.of(
                        QueryBuilders.multiMatchQuery(
                            name,
                            "user_profile.user.username",
                            "user_profile.user.username._2gram",
                            "user_profile.user.username._3gram",
                            "user_profile.user.full_name",
                            "user_profile.user.full_name._2gram",
                            "user_profile.user.full_name._3gram",
                            "user_profile.user.email"
                        ).type(MultiMatchQueryBuilder.Type.BOOL_PREFIX).fuzziness(Fuzziness.AUTO)
                    )
                )
            );
        } else {
            assertThat(query.must(), empty());
        }

        if (hint != null) {
            final List<QueryBuilder> shouldQueries = new ArrayList<>(query.should());
            if (hint.getUids() != null) {
                assertThat(shouldQueries.remove(0), equalTo(QueryBuilders.termsQuery("user_profile.uid", hint.getUids())));
            }
            final Tuple<String, List<String>> label = hint.getSingleLabel();
            if (label != null) {
                final List<String> labelValues = label.v2();
                assertThat(shouldQueries.remove(0), equalTo(QueryBuilders.termsQuery("user_profile.labels." + label.v1(), labelValues)));
            }
            assertThat(query.minimumShouldMatch(), equalTo("0"));
        } else {
            assertThat(query.should(), empty());
        }
    }

    // Note this method is to test the origin is switched security_profile for all profile related actions.
    // The actual result of the action is not relevant as long as the action is performed with the correct origin.
    // Therefore, exceptions (used in this test) work as good as full successful responses.
    public void testSecurityProfileOrigin() {
        // Activate profile
        doAnswer(invocation -> {
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[2];
            listener.onResponse(SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
            return null;
        }).when(client).execute(eq(SearchAction.INSTANCE), any(SearchRequest.class), anyActionListener());

        when(client.prepareIndex(SECURITY_PROFILE_ALIAS)).thenReturn(
            new IndexRequestBuilder(client, IndexAction.INSTANCE, SECURITY_PROFILE_ALIAS)
        );

        final RuntimeException expectedException = new RuntimeException("expected");
        doAnswer(invocation -> {
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
            final ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(expectedException);
            return null;
        }).when(client).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), anyActionListener());

        final PlainActionFuture<Profile> future1 = new PlainActionFuture<>();
        profileService.activateProfile(AuthenticationTestHelper.builder().realm().build(), future1);
        final RuntimeException e1 = expectThrows(RuntimeException.class, future1::actionGet);
        assertThat(e1, is(expectedException));

        // Update
        doAnswer(invocation -> {
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
            final ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(expectedException);
            return null;
        }).when(client).execute(eq(UpdateAction.INSTANCE), any(UpdateRequest.class), anyActionListener());
        final PlainActionFuture<UpdateResponse> future2 = new PlainActionFuture<>();
        profileService.doUpdate(mock(UpdateRequest.class), future2);
        final RuntimeException e2 = expectThrows(RuntimeException.class, future2::actionGet);
        assertThat(e2, is(expectedException));

        // Suggest
        doAnswer(invocation -> {
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
            final ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(expectedException);
            return null;
        }).when(client).execute(eq(SearchAction.INSTANCE), any(SearchRequest.class), anyActionListener());
        final PlainActionFuture<SuggestProfilesResponse> future3 = new PlainActionFuture<>();
        profileService.suggestProfile(new SuggestProfilesRequest(Set.of(), "", 1, null), future3);
        final RuntimeException e3 = expectThrows(RuntimeException.class, future3::actionGet);
        assertThat(e3, is(expectedException));
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
