/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.common.ResultsAndErrors;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequestTests;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.DomainConfig;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.profile.ProfileDocument.ProfileDocumentUser;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.concurrent.ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.core.security.support.Validation.VALID_NAME_CHARS;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProfileServiceTests extends ESTestCase {

    private static final String SAMPLE_PROFILE_DOCUMENT_TEMPLATE = """
        {
          "user_profile":  {
            "uid": "%s",
            "enabled": true,
            "user": {
              "username": "%s",
              "roles": %s,
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
    private Version minNodeVersion;

    @Before
    public void prepare() {
        threadPool = spy(
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
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        minNodeVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_17_0, Version.CURRENT);
        when(discoveryNodes.getMinNodeVersion()).thenReturn(minNodeVersion);
        this.profileService = new ProfileService(
            Settings.EMPTY,
            Clock.systemUTC(),
            client,
            profileIndex,
            clusterService,
            name -> new DomainConfig(name, Set.of(), false, null),
            threadPool
        );
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testGetProfilesByUids() {
        final List<SampleDocumentParameter> sampleDocumentParameters = randomList(
            1,
            5,
            () -> new SampleDocumentParameter(
                randomAlphaOfLength(20),
                randomAlphaOfLengthBetween(3, 18),
                randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8)),
                Instant.now().toEpochMilli() + randomLongBetween(-100_000, 100_000)
            )
        );
        mockMultiGetRequest(sampleDocumentParameters);

        final PlainActionFuture<ResultsAndErrors<Profile>> future = new PlainActionFuture<>();
        final Set<String> dataKeys = randomFrom(Set.of("app1"), Set.of("app2"), Set.of("app1", "app2"), Set.of());
        profileService.getProfiles(sampleDocumentParameters.stream().map(SampleDocumentParameter::uid).toList(), dataKeys, future);
        final ResultsAndErrors<Profile> resultsAndErrors = future.actionGet();
        assertThat(resultsAndErrors.results().size(), equalTo(sampleDocumentParameters.size()));
        assertThat(resultsAndErrors.errors(), anEmptyMap());

        int i = 0;
        for (Profile profile : resultsAndErrors.results()) {
            final Map<String, Object> applicationData = new HashMap<>();

            if (dataKeys != null && dataKeys.contains("app1")) {
                applicationData.put("app1", Map.of("name", "app1"));
            }

            if (dataKeys != null && dataKeys.contains("app2")) {
                applicationData.put("app2", Map.of("name", "app2"));
            }

            final SampleDocumentParameter sampleDocumentParameter = sampleDocumentParameters.get(i);
            assertThat(
                profile,
                equalTo(
                    new Profile(
                        sampleDocumentParameter.uid,
                        true,
                        sampleDocumentParameter.lastSynchronized,
                        new Profile.ProfileUser(
                            sampleDocumentParameter.username,
                            sampleDocumentParameter.roles,
                            "realm_name_1",
                            "domainA",
                            "foo@example.com",
                            "User Foo"
                        ),
                        Map.of(),
                        applicationData,
                        new Profile.VersionControl(1, 0)
                    )
                )
            );
            i++;
        }
    }

    public void testGetProfilesEmptyUids() {
        final PlainActionFuture<ResultsAndErrors<Profile>> future = new PlainActionFuture<>();
        profileService.getProfiles(List.of(), Set.of(), future);
        assertThat(future.actionGet().isEmpty(), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testGetProfileSubjectsNoIndex() throws Exception {
        when(profileIndex.indexExists()).thenReturn(false);
        PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future = new PlainActionFuture<>();
        profileService.getProfileSubjects(randomList(1, 5, () -> randomAlphaOfLength(20)), future);
        ResultsAndErrors<Map.Entry<String, Subject>> resultsAndErrors = future.get();
        assertThat(resultsAndErrors.results().size(), is(0));
        assertThat(resultsAndErrors.errors().size(), is(0));
        when(profileIndex.indexExists()).thenReturn(true);
        ElasticsearchException unavailableException = new ElasticsearchException("mock profile index unavailable");
        when(profileIndex.isAvailable()).thenReturn(false);
        when(profileIndex.getUnavailableReason()).thenReturn(unavailableException);
        PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future2 = new PlainActionFuture<>();
        profileService.getProfileSubjects(randomList(1, 5, () -> randomAlphaOfLength(20)), future2);
        ExecutionException e = expectThrows(ExecutionException.class, () -> future2.get());
        assertThat(e.getCause(), is(unavailableException));
        PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future3 = new PlainActionFuture<>();
        profileService.getProfileSubjects(List.of(), future3);
        resultsAndErrors = future3.get();
        assertThat(resultsAndErrors.results().size(), is(0));
        assertThat(resultsAndErrors.errors().size(), is(0));
        verify(profileIndex, never()).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
    }

    @SuppressWarnings("unchecked")
    public void testGetProfileSubjectsWithMissingUids() throws Exception {
        final Collection<String> allProfileUids = randomList(1, 5, () -> randomAlphaOfLength(20));
        final Collection<String> missingProfileUids = randomSubsetOf(allProfileUids);
        doAnswer(invocation -> {
            assertThat(
                threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME),
                equalTo(minNodeVersion.onOrAfter(Version.V_8_3_0) ? SECURITY_PROFILE_ORIGIN : SECURITY_ORIGIN)
            );
            final MultiGetRequest multiGetRequest = (MultiGetRequest) invocation.getArguments()[1];
            List<MultiGetItemResponse> responses = new ArrayList<>();
            for (MultiGetRequest.Item item : multiGetRequest.getItems()) {
                assertThat(item.index(), is(SECURITY_PROFILE_ALIAS));
                assertThat(item.id(), Matchers.startsWith("profile_"));
                assertThat(allProfileUids, hasItem(item.id().substring("profile_".length())));
                if (missingProfileUids.contains(item.id().substring("profile_".length()))) {
                    GetResponse missingResponse = mock(GetResponse.class);
                    when(missingResponse.isExists()).thenReturn(false);
                    when(missingResponse.getId()).thenReturn(item.id());
                    responses.add(new MultiGetItemResponse(missingResponse, null));
                } else {
                    String source = getSampleProfileDocumentSource(
                        item.id().substring("profile_".length()),
                        "foo_username_" + item.id().substring("profile_".length()),
                        List.of("foo_role_" + item.id().substring("profile_".length())),
                        Instant.now().toEpochMilli()
                    );
                    GetResult existingResult = new GetResult(
                        SECURITY_PROFILE_ALIAS,
                        item.id(),
                        0,
                        1,
                        1,
                        true,
                        new BytesArray(source),
                        emptyMap(),
                        emptyMap()
                    );
                    responses.add(new MultiGetItemResponse(new GetResponse(existingResult), null));
                }
            }
            final ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocation.getArguments()[2];
            listener.onResponse(new MultiGetResponse(responses.toArray(MultiGetItemResponse[]::new)));
            return null;
        }).when(client).execute(eq(MultiGetAction.INSTANCE), any(MultiGetRequest.class), anyActionListener());

        final PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future = new PlainActionFuture<>();
        profileService.getProfileSubjects(allProfileUids, future);

        ResultsAndErrors<Map.Entry<String, Subject>> resultsAndErrors = future.get();
        verify(profileIndex).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
        assertThat(resultsAndErrors.errors().size(), equalTo(missingProfileUids.size()));
        resultsAndErrors.errors().forEach((uid, e) -> {
            assertThat(missingProfileUids, hasItem(uid));
            assertThat(e, instanceOf(ResourceNotFoundException.class));
        });

        assertThat(resultsAndErrors.results().size(), is(allProfileUids.size() - missingProfileUids.size()));
        for (Map.Entry<String, Subject> profileIdAndSubject : resultsAndErrors.results()) {
            assertThat(allProfileUids, hasItem(profileIdAndSubject.getKey()));
            assertThat(missingProfileUids, not(hasItem(profileIdAndSubject.getKey())));
            assertThat(profileIdAndSubject.getValue().getUser().principal(), is("foo_username_" + profileIdAndSubject.getKey()));
            assertThat(profileIdAndSubject.getValue().getUser().roles(), arrayWithSize(1));
            assertThat(profileIdAndSubject.getValue().getUser().roles()[0], is("foo_role_" + profileIdAndSubject.getKey()));
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetProfileSubjectWithFailures() throws Exception {
        final ElasticsearchException mGetException = new ElasticsearchException("mget Exception");
        doAnswer(invocation -> {
            assertThat(
                threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME),
                equalTo(minNodeVersion.onOrAfter(Version.V_8_3_0) ? SECURITY_PROFILE_ORIGIN : SECURITY_ORIGIN)
            );
            final ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocation.getArguments()[2];
            listener.onFailure(mGetException);
            return null;
        }).when(client).execute(eq(MultiGetAction.INSTANCE), any(MultiGetRequest.class), anyActionListener());
        final PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future = new PlainActionFuture<>();
        profileService.getProfileSubjects(randomList(1, 5, () -> randomAlphaOfLength(20)), future);
        ExecutionException e = expectThrows(ExecutionException.class, () -> future.get());
        assertThat(e.getCause(), is(mGetException));
        final Collection<String> allProfileUids = randomList(1, 5, () -> randomAlphaOfLength(20));
        final Collection<String> errorProfileUids = randomSubsetOf(allProfileUids);
        final Collection<String> missingProfileUids = Sets.difference(Set.copyOf(allProfileUids), Set.copyOf(errorProfileUids));
        doAnswer(invocation -> {
            assertThat(
                threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME),
                equalTo(minNodeVersion.onOrAfter(Version.V_8_3_0) ? SECURITY_PROFILE_ORIGIN : SECURITY_ORIGIN)
            );
            final MultiGetRequest multiGetRequest = (MultiGetRequest) invocation.getArguments()[1];
            List<MultiGetItemResponse> responses = new ArrayList<>();
            for (MultiGetRequest.Item item : multiGetRequest.getItems()) {
                assertThat(item.index(), is(SECURITY_PROFILE_ALIAS));
                assertThat(item.id(), Matchers.startsWith("profile_"));
                if (false == errorProfileUids.contains(item.id().substring("profile_".length()))) {
                    GetResponse missingResponse = mock(GetResponse.class);
                    when(missingResponse.isExists()).thenReturn(false);
                    when(missingResponse.getId()).thenReturn(item.id());
                    responses.add(new MultiGetItemResponse(missingResponse, null));
                } else {
                    MultiGetResponse.Failure failure = mock(MultiGetResponse.Failure.class);
                    when(failure.getId()).thenReturn(item.id());
                    when(failure.getFailure()).thenReturn(new ElasticsearchException("failed mget item"));
                    responses.add(new MultiGetItemResponse(null, failure));
                }
            }
            final ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocation.getArguments()[2];
            listener.onResponse(new MultiGetResponse(responses.toArray(MultiGetItemResponse[]::new)));
            return null;
        }).when(client).execute(eq(MultiGetAction.INSTANCE), any(MultiGetRequest.class), anyActionListener());

        final PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future2 = new PlainActionFuture<>();
        profileService.getProfileSubjects(allProfileUids, future2);

        ResultsAndErrors<Map.Entry<String, Subject>> resultsAndErrors = future2.get();
        assertThat(resultsAndErrors.results().isEmpty(), is(true));
        assertThat(resultsAndErrors.errors().size(), equalTo(allProfileUids.size()));
        assertThat(resultsAndErrors.errors().keySet(), equalTo(Set.copyOf(allProfileUids)));
        missingProfileUids.forEach(uid -> assertThat(resultsAndErrors.errors().get(uid), instanceOf(ResourceNotFoundException.class)));
        errorProfileUids.forEach(uid -> assertThat(resultsAndErrors.errors().get(uid), instanceOf(ElasticsearchException.class)));
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
        final Subject subject = AuthenticationTestHelper.builder().realm().build(false).getEffectiveSubject();
        final PlainActionFuture<Profile> future1 = new PlainActionFuture<>();
        profileService.maybeIncrementDifferentiatorAndCreateNewProfile(subject, randomProfileDocument(randomAlphaOfLength(20)), future1);
        final ElasticsearchException e1 = expectThrows(ElasticsearchException.class, future1::actionGet);
        assertThat(e1.getMessage(), containsString("does not contain any underscore character"));

        final PlainActionFuture<Profile> future2 = new PlainActionFuture<>();
        profileService.maybeIncrementDifferentiatorAndCreateNewProfile(
            subject,
            randomProfileDocument(randomAlphaOfLength(20) + "_"),
            future2
        );
        final ElasticsearchException e2 = expectThrows(ElasticsearchException.class, future2::actionGet);
        assertThat(e2.getMessage(), containsString("does not contain a differentiator"));

        final PlainActionFuture<Profile> future3 = new PlainActionFuture<>();
        profileService.maybeIncrementDifferentiatorAndCreateNewProfile(
            subject,
            randomProfileDocument(randomAlphaOfLength(20) + "_" + randomAlphaOfLengthBetween(1, 3)),
            future3
        );
        final ElasticsearchException e3 = expectThrows(ElasticsearchException.class, future3::actionGet);
        assertThat(e3.getMessage(), containsString("differentiator is not a number"));
    }

    public void testLiteralUsernameWillThrowOnDuplicate() throws IOException {
        final Subject subject = new Subject(AuthenticationTestHelper.randomUser(), AuthenticationTestHelper.randomRealmRef(true));
        final ProfileService service = new ProfileService(
            Settings.EMPTY,
            Clock.systemUTC(),
            client,
            profileIndex,
            mock(ClusterService.class),
            domainName -> new DomainConfig(domainName, Set.of(), true, "suffix"),
            threadPool
        );
        final PlainActionFuture<Profile> future = new PlainActionFuture<>();
        service.maybeIncrementDifferentiatorAndCreateNewProfile(
            subject,
            ProfileDocument.fromSubjectWithUid(subject, "u_" + subject.getUser().principal() + "_suffix"),
            future
        );

        final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("cannot create new profile for [" + subject.getUser().principal() + "]"));
        assertThat(
            e.getMessage(),
            containsString("suffix setting of domain [" + subject.getRealm().getDomain().name() + "] does not support auto-increment")
        );
    }

    public void testBuildSearchRequest() {
        final String name = randomAlphaOfLengthBetween(0, 8);
        final int size = randomIntBetween(0, Integer.MAX_VALUE);
        final SuggestProfilesRequest.Hint hint = SuggestProfilesRequestTests.randomHint();
        final SuggestProfilesRequest suggestProfilesRequest = new SuggestProfilesRequest(Set.of(), name, size, hint);
        final TaskId parentTaskId = new TaskId(randomAlphaOfLength(20), randomNonNegativeLong());

        final SearchRequest searchRequest = profileService.buildSearchRequestForSuggest(suggestProfilesRequest, parentTaskId);
        assertThat(searchRequest.getParentTask(), is(parentTaskId));

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
            assertThat(
                threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME),
                equalTo(minNodeVersion.onOrAfter(Version.V_8_3_0) ? SECURITY_PROFILE_ORIGIN : SECURITY_ORIGIN)
            );
            @SuppressWarnings("unchecked")
            final ActionListener<MultiSearchResponse> listener = (ActionListener<MultiSearchResponse>) invocation.getArguments()[2];
            listener.onResponse(
                new MultiSearchResponse(
                    new MultiSearchResponse.Item[] {
                        new MultiSearchResponse.Item(SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY), null) },
                    1L
                )
            );
            return null;
        }).when(client).execute(eq(MultiSearchAction.INSTANCE), any(MultiSearchRequest.class), anyActionListener());

        when(client.prepareIndex(SECURITY_PROFILE_ALIAS)).thenReturn(
            new IndexRequestBuilder(client, IndexAction.INSTANCE, SECURITY_PROFILE_ALIAS)
        );

        final RuntimeException expectedException = new RuntimeException("expected");
        doAnswer(invocation -> {
            assertThat(
                threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME),
                equalTo(minNodeVersion.onOrAfter(Version.V_8_3_0) ? SECURITY_PROFILE_ORIGIN : SECURITY_ORIGIN)
            );
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
            assertThat(
                threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME),
                equalTo(minNodeVersion.onOrAfter(Version.V_8_3_0) ? SECURITY_PROFILE_ORIGIN : SECURITY_ORIGIN)
            );
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
            assertThat(
                threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME),
                equalTo(minNodeVersion.onOrAfter(Version.V_8_3_0) ? SECURITY_PROFILE_ORIGIN : SECURITY_ORIGIN)
            );
            final ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(expectedException);
            return null;
        }).when(client).execute(eq(SearchAction.INSTANCE), any(SearchRequest.class), anyActionListener());
        final PlainActionFuture<SuggestProfilesResponse> future3 = new PlainActionFuture<>();
        profileService.suggestProfile(
            new SuggestProfilesRequest(Set.of(), "", 1, null),
            new TaskId(randomAlphaOfLength(20), randomNonNegativeLong()),
            future3
        );
        final RuntimeException e3 = expectThrows(RuntimeException.class, future3::actionGet);
        assertThat(e3, is(expectedException));
    }

    public void testActivateProfileWithDifferentUidFormats() throws IOException {
        final ProfileService service = spy(
            new ProfileService(Settings.EMPTY, Clock.systemUTC(), client, profileIndex, mock(ClusterService.class), domainName -> {
                if (domainName.startsWith("hash")) {
                    return new DomainConfig(domainName, Set.of(), false, null);
                } else {
                    return new DomainConfig(domainName, Set.of(), true, "suffix");
                }
            }, threadPool)
        );

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<ProfileService.VersionedDocument>) invocation.getArguments()[1];
            listener.onResponse(null);
            return null;
        }).when(service).searchVersionedDocumentForSubject(any(), anyActionListener());

        doAnswer(invocation -> {
            final Object[] arguments = invocation.getArguments();
            final Subject subject = (Subject) arguments[0];
            final User user = subject.getUser();
            final Authentication.RealmRef realmRef = subject.getRealm();
            final String uid = (String) arguments[1];
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<Profile>) arguments[2];
            listener.onResponse(
                new Profile(
                    uid,
                    true,
                    0,
                    new Profile.ProfileUser(
                        user.principal(),
                        Arrays.asList(user.roles()),
                        realmRef.getName(),
                        realmRef.getDomain() == null ? null : realmRef.getDomain().name(),
                        user.email(),
                        user.fullName()
                    ),
                    Map.of(),
                    Map.of(),
                    new Profile.VersionControl(0, 0)
                )
            );
            return null;
        }).when(service).createNewProfile(any(), any(), anyActionListener());

        // Domainless realm or domain with hashed username
        Authentication.RealmRef realmRef1 = AuthenticationTestHelper.randomRealmRef(false);
        if (randomBoolean()) {
            realmRef1 = new Authentication.RealmRef(
                realmRef1.getName(),
                realmRef1.getType(),
                realmRef1.getNodeName(),
                new RealmDomain("hash", Set.of(new RealmConfig.RealmIdentifier(realmRef1.getType(), realmRef1.getName())))
            );
        }

        final Authentication authentication1 = AuthenticationTestHelper.builder().realm().realmRef(realmRef1).build();
        final Subject subject1 = authentication1.getEffectiveSubject();
        final PlainActionFuture<Profile> future1 = new PlainActionFuture<>();
        service.activateProfile(authentication1, future1);
        final Profile profile1 = future1.actionGet();
        assertThat(
            profile1.uid(),
            equalTo(
                "u_"
                    + Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(MessageDigests.digest(new BytesArray(subject1.getUser().principal()), MessageDigests.sha256()))
                    + "_0"
            )
        );
        assertThat(profile1.user().username(), equalTo(subject1.getUser().principal()));

        // Domain with literal username
        Authentication.RealmRef realmRef2 = AuthenticationTestHelper.randomRealmRef(false);
        realmRef2 = new Authentication.RealmRef(
            realmRef2.getName(),
            realmRef2.getType(),
            realmRef2.getNodeName(),
            new RealmDomain("literal", Set.of(new RealmConfig.RealmIdentifier(realmRef2.getType(), realmRef2.getName())))
        );

        // Username is allowed to have dash as long it is not the 1st character
        final User user2 = AuthenticationTestHelper.userWithRandomMetadataAndDetails(
            randomFrom(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(1, 8) + "-" + randomAlphaOfLengthBetween(0, 8))
        );
        final Authentication authentication2 = AuthenticationTestHelper.builder().user(user2).realm().realmRef(realmRef2).build();
        final Subject subject2 = authentication2.getEffectiveSubject();
        final PlainActionFuture<Profile> future2 = new PlainActionFuture<>();
        service.activateProfile(authentication2, future2);
        final Profile profile2 = future2.actionGet();
        assertThat(profile2.uid(), equalTo("u_" + subject2.getUser().principal() + "_suffix"));
        assertThat(profile2.user().username(), equalTo(subject2.getUser().principal()));

        // Domain with literal username, but the username is invalid
        final List<Character> invalidFirstCharsOfLiteralUsername = VALID_NAME_CHARS.stream()
            .filter(c -> false == Character.isLetterOrDigit(c))
            .toList();
        final List<Character> invalidCharsOfLiteralUsername = invalidFirstCharsOfLiteralUsername.stream().filter(c -> c != '-').toList();
        final String invalidLiteralUsername = randomFrom(
            "",
            "fóóbár",
            randomAlphaOfLength(257),
            randomFrom(invalidFirstCharsOfLiteralUsername) + randomAlphaOfLengthBetween(0, 8),
            randomAlphaOfLengthBetween(1, 8) + randomFrom(invalidCharsOfLiteralUsername) + randomAlphaOfLengthBetween(0, 8)
        );
        final Authentication.RealmRef realmRef3 = realmRef2;
        final Authentication authentication3 = AuthenticationTestHelper.builder()
            .realm()
            .user(new User(invalidLiteralUsername))
            .realmRef(realmRef3)
            .build();
        final PlainActionFuture<Profile> future3 = new PlainActionFuture<>();
        service.activateProfile(authentication3, future3);

        final ElasticsearchException e3 = expectThrows(ElasticsearchException.class, future3::actionGet);
        assertThat(
            e3.getMessage(),
            containsString("Security domain [" + realmRef3.getDomain().name() + "] is configured to use literal username.")
        );
        assertThat(e3.getMessage(), containsString("The username must begin with an alphanumeric character"));
    }

    public void testShouldSkipUpdateForActivate() {
        // Scenario 1: Should skip update since content is the same and last_synchronized is within 30 seconds
        final ProfileDocument profileDocument = randomProfileDocument(randomAlphaOfLength(40));
        final ProfileDocumentUser user = profileDocument.user();
        assertThat(
            profileService.shouldSkipUpdateForActivate(
                profileDocument,
                new ProfileDocument(
                    profileDocument.uid(),
                    profileDocument.enabled(),
                    profileDocument.lastSynchronized() + randomLongBetween(0, 29_999),
                    profileDocument.user(),
                    profileDocument.labels(),
                    profileDocument.applicationData()
                )
            ),
            is(true)
        );

        // Scenario 2: Should not skip update if 30 seconds has passed even when content is the same
        assertThat(
            profileService.shouldSkipUpdateForActivate(
                profileDocument,
                new ProfileDocument(
                    profileDocument.uid(),
                    profileDocument.enabled(),
                    profileDocument.lastSynchronized() + randomLongBetween(30_000, 60_000),
                    profileDocument.user(),
                    profileDocument.labels(),
                    profileDocument.applicationData()
                )
            ),
            is(false)
        );

        // Scenario 4: Should not skip update if enabled status changes
        assertThat(
            profileService.shouldSkipUpdateForActivate(
                new ProfileDocument(
                    profileDocument.uid(),
                    false,
                    profileDocument.lastSynchronized(),
                    profileDocument.user(),
                    profileDocument.labels(),
                    profileDocument.applicationData()
                ),
                profileDocument
            ),
            is(false)
        );

        // Scenario 4: Should not skip update if user info changes
        final ProfileDocumentUser user4 = switch (randomIntBetween(0, 4)) {
            case 0 -> new ProfileDocumentUser(
                randomValueOtherThan(user.username(), () -> randomAlphaOfLengthBetween(5, 8)),
                user.roles(),
                user.realm(),
                user.email(),
                user.fullName()
            );
            case 1 -> new ProfileDocumentUser(
                user.username(),
                randomValueOtherThan(user.roles(), () -> randomList(3, () -> randomAlphaOfLengthBetween(5, 8))),
                user.realm(),
                user.email(),
                user.fullName()
            );
            case 2 -> new ProfileDocumentUser(
                user.username(),
                user.roles(),
                randomValueOtherThan(user.realm(), AuthenticationTestHelper::randomRealmRef),
                user.email(),
                user.fullName()
            );
            case 3 -> new ProfileDocumentUser(
                user.username(),
                user.roles(),
                user.realm(),
                randomValueOtherThan(user.email(), () -> randomAlphaOfLengthBetween(10, 20)),
                user.fullName()
            );
            default -> new ProfileDocumentUser(
                user.username(),
                user.roles(),
                user.realm(),
                user.email(),
                randomValueOtherThan(user.fullName(), () -> randomAlphaOfLengthBetween(10, 20))
            );
        };

        assertThat(
            profileService.shouldSkipUpdateForActivate(
                profileDocument,
                new ProfileDocument(
                    profileDocument.uid(),
                    true,
                    profileDocument.lastSynchronized(),
                    user4,
                    profileDocument.labels(),
                    profileDocument.applicationData()
                )
            ),
            is(false)
        );
    }

    public void testActivateWhenShouldSkipUpdateForActivateReturnsTrue() throws IOException {
        final ProfileService service = spy(profileService);

        doAnswer(
            invocation -> new UpdateRequestBuilder(
                client,
                UpdateAction.INSTANCE,
                SECURITY_PROFILE_ALIAS,
                (String) invocation.getArguments()[1]
            )
        ).when(client).prepareUpdate(eq(SECURITY_PROFILE_ALIAS), anyString());

        final UpdateResponse updateResponse = mock(UpdateResponse.class);
        when(updateResponse.getPrimaryTerm()).thenReturn(randomNonNegativeLong());
        when(updateResponse.getSeqNo()).thenReturn(randomNonNegativeLong());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<UpdateResponse> listener = (ActionListener<UpdateResponse>) invocation.getArguments()[1];
            listener.onResponse(updateResponse);
            return null;
        }).when(service).doUpdate(any(), anyActionListener());

        final Subject subject = AuthenticationTestHelper.builder().realm().build().getEffectiveSubject();
        final ProfileService.VersionedDocument versionedDocument = new ProfileService.VersionedDocument(
            ProfileDocument.fromSubject(subject),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        doAnswer(invocation -> true).when(service).shouldSkipUpdateForActivate(any(), any());

        final PlainActionFuture<Profile> future = new PlainActionFuture<>();
        service.updateProfileForActivate(subject, versionedDocument, future);
        assertThat(future.actionGet(), equalTo(versionedDocument.toProfile(Set.of())));
        verify(service, times(1)).shouldSkipUpdateForActivate(any(), any());
        verify(service, never()).doUpdate(any(), anyActionListener());
    }

    public void testActivateWhenShouldSkipUpdateForActivateReturnsFalseFirst() throws IOException {
        final ProfileService service = spy(profileService);
        doAnswer(
            invocation -> new UpdateRequestBuilder(
                client,
                UpdateAction.INSTANCE,
                SECURITY_PROFILE_ALIAS,
                (String) invocation.getArguments()[1]
            )
        ).when(client).prepareUpdate(eq(SECURITY_PROFILE_ALIAS), anyString());

        // Throw version conflict on update to force GET document
        final Exception updateException;
        if (randomBoolean()) {
            updateException = new RemoteTransportException("", new VersionConflictEngineException(mock(ShardId.class), "", ""));
        } else {
            updateException = new VersionConflictEngineException(mock(ShardId.class), "", "");
        }
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<UpdateResponse> listener = (ActionListener<UpdateResponse>) invocation.getArguments()[1];
            listener.onFailure(updateException);
            return null;
        }).when(service).doUpdate(any(), anyActionListener());

        final Subject subject = AuthenticationTestHelper.builder().realm().build().getEffectiveSubject();
        final var versionedDocument = new ProfileService.VersionedDocument(ProfileDocument.fromSubject(subject), 1, 0);

        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().field("user_profile", versionedDocument.doc()).endObject();

        SecurityMocks.mockGetRequest(
            client,
            SECURITY_PROFILE_ALIAS,
            "profile_" + versionedDocument.doc().uid(),
            BytesReference.bytes(builder)
        );
        doAnswer(invocation -> {
            final GetRequest getRequest = (GetRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<GetResponse>) invocation.getArguments()[2];
            client.get(getRequest, listener);
            return null;
        }).when(client).execute(eq(GetAction.INSTANCE), any(GetRequest.class), anyActionListener());

        // First check returns false, second check return true or false randomly
        final boolean secondCheckResult = randomBoolean();
        doAnswer(invocation -> false).doAnswer(invocation -> secondCheckResult).when(service).shouldSkipUpdateForActivate(any(), any());

        final PlainActionFuture<Profile> future = new PlainActionFuture<>();
        service.updateProfileForActivate(subject, versionedDocument, future);
        if (secondCheckResult) {
            assertThat(future.actionGet(), equalTo(versionedDocument.toProfile(Set.of())));
        } else {
            assertThat(expectThrows(VersionConflictEngineException.class, future::actionGet), sameInstance(updateException));
        }
        verify(service, times(2)).shouldSkipUpdateForActivate(any(), any());
        verify(service).doUpdate(any(), anyActionListener());
    }

    public void testActivateWhenGetRequestErrors() throws IOException {
        final ProfileService service = spy(profileService);
        doAnswer(
            invocation -> new UpdateRequestBuilder(
                client,
                UpdateAction.INSTANCE,
                SECURITY_PROFILE_ALIAS,
                (String) invocation.getArguments()[1]
            )
        ).when(client).prepareUpdate(eq(SECURITY_PROFILE_ALIAS), anyString());

        // Throw version conflict on update to force GET document
        final var versionConflictEngineException = new VersionConflictEngineException(mock(ShardId.class), "", "");
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<UpdateResponse> listener = (ActionListener<UpdateResponse>) invocation.getArguments()[1];
            listener.onFailure(versionConflictEngineException);
            return null;
        }).when(service).doUpdate(any(), anyActionListener());

        final Subject subject = AuthenticationTestHelper.builder().realm().build().getEffectiveSubject();
        final var versionedDocument = new ProfileService.VersionedDocument(ProfileDocument.fromSubject(subject), 1, 0);

        final ResourceNotFoundException getException = new ResourceNotFoundException("not found");
        SecurityMocks.mockGetRequestException(client, getException);
        doAnswer(invocation -> {
            final GetRequest getRequest = (GetRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<GetResponse>) invocation.getArguments()[2];
            client.get(getRequest, listener);
            return null;
        }).when(client).execute(eq(GetAction.INSTANCE), any(GetRequest.class), anyActionListener());

        // First check returns false
        doAnswer(invocation -> false).when(service).shouldSkipUpdateForActivate(any(), any());

        final PlainActionFuture<Profile> future = new PlainActionFuture<>();
        service.updateProfileForActivate(subject, versionedDocument, future);
        assertThat(expectThrows(ResourceNotFoundException.class, future::actionGet), sameInstance(getException));
        assertThat(getException.getSuppressed(), arrayContaining(versionConflictEngineException));
        verify(service, times(1)).shouldSkipUpdateForActivate(any(), any());
        verify(service).doUpdate(any(), anyActionListener());
    }

    record SampleDocumentParameter(String uid, String username, List<String> roles, long lastSynchronized) {}

    private void mockMultiGetRequest(List<SampleDocumentParameter> sampleDocumentParameters) {
        mockMultiGetRequest(sampleDocumentParameters, Map.of());
    }

    private void mockMultiGetRequest(List<SampleDocumentParameter> sampleDocumentParameters, Map<String, Exception> errors) {
        doAnswer(invocation -> {
            assertThat(
                threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME),
                equalTo(minNodeVersion.onOrAfter(Version.V_8_3_0) ? SECURITY_PROFILE_ORIGIN : SECURITY_ORIGIN)
            );
            final MultiGetRequest multiGetRequest = (MultiGetRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            final ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocation.getArguments()[2];
            client.multiGet(multiGetRequest, listener);
            return null;
        }).when(client).execute(eq(MultiGetAction.INSTANCE), any(MultiGetRequest.class), anyActionListener());

        final Map<String, String> results = sampleDocumentParameters.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    param -> "profile_" + param.uid,
                    param -> getSampleProfileDocumentSource(param.uid, param.username, param.roles, param.lastSynchronized)
                )
            );

        SecurityMocks.mockMultiGetRequest(
            client,
            SECURITY_PROFILE_ALIAS,
            results,
            errors.entrySet().stream().collect(Collectors.toUnmodifiableMap(entry -> "profile_" + entry.getKey(), Map.Entry::getValue))
        );
    }

    public static String getSampleProfileDocumentSource(String uid, String username, List<String> roles, long lastSynchronized) {
        return SAMPLE_PROFILE_DOCUMENT_TEMPLATE.formatted(
            uid,
            username,
            roles.stream().map(v -> "\"" + v + "\"").collect(Collectors.toList()),
            lastSynchronized
        );
    }

    private ProfileDocument randomProfileDocument(String uid) {
        return new ProfileDocument(
            uid,
            true,
            randomNonNegativeLong(),
            new ProfileDocumentUser(
                randomAlphaOfLengthBetween(5, 8),
                randomList(3, () -> randomAlphaOfLengthBetween(5, 8)),
                AuthenticationTestHelper.randomRealmRef(),
                "foo@example.com",
                randomAlphaOfLengthBetween(10, 20)
            ),
            Map.of(),
            null
        );
    }
}
