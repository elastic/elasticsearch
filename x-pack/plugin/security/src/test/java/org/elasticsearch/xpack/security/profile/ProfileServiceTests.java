/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.common.ResultsAndErrors;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
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
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.profile.ProfileDocument.ProfileDocumentUser;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.concurrent.ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.core.security.support.Validation.VALID_NAME_CHARS;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
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
    Function<RealmConfig.RealmIdentifier, Authentication.RealmRef> realmRefLookup;

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
                    EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
                )
            )
        );
        this.client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.prepareSearch(SECURITY_PROFILE_ALIAS)).thenReturn(new SearchRequestBuilder(client).setIndices(SECURITY_PROFILE_ALIAS));
        this.profileIndex = SecurityMocks.mockSecurityIndexManager(SECURITY_PROFILE_ALIAS);
        realmRefLookup = realmIdentifier -> null;
        Realms realms = mock(Realms.class);
        when(realms.getDomainConfig(anyString())).then(args -> new DomainConfig(args.getArgument(0), Set.of(), false, null));
        when(realms.getRealmRef(any(RealmConfig.RealmIdentifier.class))).then(args -> realmRefLookup.apply(args.getArgument(0)));
        this.profileService = new ProfileService(Settings.EMPTY, Clock.systemUTC(), client, profileIndex, realms);
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
        SecurityIndexManager.IndexState projectIndex = profileIndex.forCurrentProject();
        when(projectIndex.indexExists()).thenReturn(false);
        PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future = new PlainActionFuture<>();
        profileService.getProfileSubjects(randomList(1, 5, () -> randomAlphaOfLength(20)), future);
        ResultsAndErrors<Map.Entry<String, Subject>> resultsAndErrors = future.get();
        assertThat(resultsAndErrors.results().size(), is(0));
        assertThat(resultsAndErrors.errors().size(), is(0));
        when(projectIndex.indexExists()).thenReturn(true);
        ElasticsearchException unavailableException = new ElasticsearchException("mock profile index unavailable");
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(false);
        when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(unavailableException);
        PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future2 = new PlainActionFuture<>();
        profileService.getProfileSubjects(randomList(1, 5, () -> randomAlphaOfLength(20)), future2);
        ExecutionException e = expectThrows(ExecutionException.class, () -> future2.get());
        assertThat(e.getCause(), is(unavailableException));
        PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future3 = new PlainActionFuture<>();
        profileService.getProfileSubjects(List.of(), future3);
        resultsAndErrors = future3.get();
        assertThat(resultsAndErrors.results().size(), is(0));
        assertThat(resultsAndErrors.errors().size(), is(0));
        verify(projectIndex, never()).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
    }

    @SuppressWarnings("unchecked")
    public void testGetProfileSubjectsWithMissingUids() throws Exception {
        final Collection<String> allProfileUids = randomList(1, 5, () -> randomAlphaOfLength(20));
        final Collection<String> missingProfileUids = randomSubsetOf(allProfileUids);
        doAnswer(invocation -> {
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
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
        }).when(client).execute(eq(TransportMultiGetAction.TYPE), any(MultiGetRequest.class), anyActionListener());

        final PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future = new PlainActionFuture<>();
        profileService.getProfileSubjects(allProfileUids, future);

        ResultsAndErrors<Map.Entry<String, Subject>> resultsAndErrors = future.get();
        SecurityIndexManager.IndexState projectIndex = profileIndex.forCurrentProject();
        verify(projectIndex).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
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
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
            final ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocation.getArguments()[2];
            listener.onFailure(mGetException);
            return null;
        }).when(client).execute(eq(TransportMultiGetAction.TYPE), any(MultiGetRequest.class), anyActionListener());
        final PlainActionFuture<ResultsAndErrors<Map.Entry<String, Subject>>> future = new PlainActionFuture<>();
        profileService.getProfileSubjects(randomList(1, 5, () -> randomAlphaOfLength(20)), future);
        ExecutionException e = expectThrows(ExecutionException.class, () -> future.get());
        assertThat(e.getCause(), is(mGetException));
        final Collection<String> allProfileUids = randomList(1, 5, () -> randomAlphaOfLength(20));
        final Collection<String> errorProfileUids = randomSubsetOf(allProfileUids);
        final Collection<String> missingProfileUids = Sets.difference(Set.copyOf(allProfileUids), Set.copyOf(errorProfileUids));
        doAnswer(invocation -> {
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
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
        }).when(client).execute(eq(TransportMultiGetAction.TYPE), any(MultiGetRequest.class), anyActionListener());

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
        Realms realms = mock(Realms.class);
        when(realms.getDomainConfig(anyString())).then(args -> new DomainConfig(args.getArgument(0), Set.of(), true, "suffix"));
        final ProfileService service = new ProfileService(Settings.EMPTY, Clock.systemUTC(), client, profileIndex, realms);
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
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
            @SuppressWarnings("unchecked")
            final ActionListener<MultiSearchResponse> listener = (ActionListener<MultiSearchResponse>) invocation.getArguments()[2];
            var resp = new MultiSearchResponse(
                new MultiSearchResponse.Item[] {
                    new MultiSearchResponse.Item(SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY), null) },
                1L
            );
            try {
                listener.onResponse(resp);
            } finally {
                resp.decRef();
            }
            return null;
        }).when(client).execute(eq(TransportMultiSearchAction.TYPE), any(MultiSearchRequest.class), anyActionListener());

        when(client.prepareIndex(SECURITY_PROFILE_ALIAS)).thenReturn(new IndexRequestBuilder(client, SECURITY_PROFILE_ALIAS));

        final RuntimeException expectedException = new RuntimeException("expected");
        doAnswer(invocation -> {
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
            final ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(expectedException);
            return null;
        }).when(client).execute(eq(TransportBulkAction.TYPE), any(BulkRequest.class), anyActionListener());

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
        }).when(client).execute(eq(TransportUpdateAction.TYPE), any(UpdateRequest.class), anyActionListener());
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
        }).when(client).execute(eq(TransportSearchAction.TYPE), any(SearchRequest.class), anyActionListener());
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
        Realms realms = mock(Realms.class);
        when(realms.getDomainConfig(anyString())).then(args -> {
            String domainName = args.getArgument(0);
            if (domainName.startsWith("hash")) {
                return new DomainConfig(domainName, Set.of(), false, null);
            } else {
                return new DomainConfig(domainName, Set.of(), true, "suffix");
            }
        });
        final ProfileService service = spy(new ProfileService(Settings.EMPTY, Clock.systemUTC(), client, profileIndex, realms));

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

        doAnswer(invocation -> new UpdateRequestBuilder(client, SECURITY_PROFILE_ALIAS, (String) invocation.getArguments()[1])).when(client)
            .prepareUpdate(eq(SECURITY_PROFILE_ALIAS), anyString());

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
        doAnswer(invocation -> new UpdateRequestBuilder(client, SECURITY_PROFILE_ALIAS, (String) invocation.getArguments()[1])).when(client)
            .prepareUpdate(eq(SECURITY_PROFILE_ALIAS), anyString());

        // Throw version conflict on update to force GET document
        final Exception updateException;
        final VersionConflictEngineException versionConflictException = new VersionConflictEngineException(mock(ShardId.class), "", "");
        if (randomBoolean()) {
            updateException = new RemoteTransportException("", versionConflictException);
        } else {
            updateException = versionConflictException;
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
        }).when(client).execute(eq(TransportGetAction.TYPE), any(GetRequest.class), anyActionListener());

        // First check returns false, second check return true or false randomly
        final boolean secondCheckResult = randomBoolean();
        doAnswer(invocation -> false).doAnswer(invocation -> secondCheckResult).when(service).shouldSkipUpdateForActivate(any(), any());

        final PlainActionFuture<Profile> future = new PlainActionFuture<>();
        service.updateProfileForActivate(subject, versionedDocument, future);
        if (secondCheckResult) {
            assertThat(future.actionGet(), equalTo(versionedDocument.toProfile(Set.of())));
        } else {
            // The actionGet method unwraps ES exception to rethrow the cause of it
            assertThat(expectThrows(VersionConflictEngineException.class, future::actionGet), sameInstance(versionConflictException));
        }
        verify(service, times(2)).shouldSkipUpdateForActivate(any(), any());
        verify(service).doUpdate(any(), anyActionListener());
    }

    public void testActivateWhenGetRequestErrors() throws IOException {
        final ProfileService service = spy(profileService);
        doAnswer(invocation -> new UpdateRequestBuilder(client, SECURITY_PROFILE_ALIAS, (String) invocation.getArguments()[1])).when(client)
            .prepareUpdate(eq(SECURITY_PROFILE_ALIAS), anyString());

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
        }).when(client).execute(eq(TransportGetAction.TYPE), any(GetRequest.class), anyActionListener());

        // First check returns false
        doAnswer(invocation -> false).when(service).shouldSkipUpdateForActivate(any(), any());

        final PlainActionFuture<Profile> future = new PlainActionFuture<>();
        service.updateProfileForActivate(subject, versionedDocument, future);
        assertThat(expectThrows(ResourceNotFoundException.class, future::actionGet), sameInstance(getException));
        assertThat(getException.getSuppressed(), arrayContaining(versionConflictEngineException));
        verify(service, times(1)).shouldSkipUpdateForActivate(any(), any());
        verify(service).doUpdate(any(), anyActionListener());
    }

    public void testUsageStats() {
        final List<String> metricNames = List.of("total", "enabled", "recent");

        final String erroredMetric;
        if (randomBoolean()) {
            erroredMetric = randomFrom(metricNames);
        } else {
            erroredMetric = null;
        }

        final Map<String, Long> metrics = metricNames.stream()
            .collect(Collectors.toUnmodifiableMap(Function.identity(), n -> n.equals(erroredMetric) ? 0L : randomNonNegativeLong()));

        final MultiSearchResponse.Item[] items = metricNames.stream().map(name -> {
            if (name.equals(erroredMetric)) {
                return new MultiSearchResponse.Item(null, new RuntimeException());
            } else {
                final var searchResponse = mock(SearchResponse.class);
                when(searchResponse.getHits()).thenReturn(
                    SearchHits.empty(new TotalHits(metrics.get(name), TotalHits.Relation.EQUAL_TO), 1)
                );
                return new MultiSearchResponse.Item(searchResponse, null);
            }
        }).toArray(MultiSearchResponse.Item[]::new);

        final MultiSearchResponse multiSearchResponse = new MultiSearchResponse(items, randomNonNegativeLong());
        try {
            doAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                final var listener = (ActionListener<MultiSearchResponse>) invocation.getArgument(2);
                listener.onResponse(multiSearchResponse);
                return null;
            }).when(client).execute(eq(TransportMultiSearchAction.TYPE), any(MultiSearchRequest.class), anyActionListener());

            when(client.prepareMultiSearch()).thenReturn(new MultiSearchRequestBuilder(client));
            final PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
            profileService.usageStats(future);
            assertThat(future.actionGet(), equalTo(metrics));
        } finally {
            multiSearchResponse.decRef();
        }
    }

    public void testUsageStatsWhenNoIndex() {
        SecurityIndexManager.IndexState projectIndex = profileIndex.forCurrentProject();
        when(projectIndex.indexExists()).thenReturn(false);
        final PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        profileService.usageStats(future);
        assertThat(future.actionGet(), equalTo(Map.of("total", 0L, "enabled", 0L, "recent", 0L)));
    }

    @SuppressWarnings("unchecked")
    public void testProfileSearchForApiKeyOwnerWithoutDomain() throws Exception {
        String realmName = "realmName_" + randomAlphaOfLength(8);
        String realmType = "realmType_" + randomAlphaOfLength(8);
        String username = "username_" + randomAlphaOfLength(8);
        List<ApiKey> apiKeys = List.of(createApiKeyForOwner("keyId_" + randomAlphaOfLength(8), username, realmName, realmType));
        realmRefLookup = realmIdentifier -> {
            assertThat(realmIdentifier.getName(), is(realmName));
            assertThat(realmIdentifier.getType(), is(realmType));
            return new Authentication.RealmRef(realmName, realmType, "nodeName_" + randomAlphaOfLength(8));
        };
        MultiSearchResponse.Item[] responseItems = new MultiSearchResponse.Item[1];
        responseItems[0] = new MultiSearchResponse.Item(new TestEmptySearchResponse(), null);
        MultiSearchResponse emptyMultiSearchResponse = new MultiSearchResponse(responseItems, randomNonNegativeLong());
        try {
            doAnswer(invocation -> {
                assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
                MultiSearchRequest multiSearchRequest = (MultiSearchRequest) invocation.getArguments()[1];
                assertThat(multiSearchRequest.requests(), iterableWithSize(1));
                assertThat(multiSearchRequest.requests().get(0).source().query(), instanceOf(BoolQueryBuilder.class));
                assertThat(((BoolQueryBuilder) multiSearchRequest.requests().get(0).source().query()).filter(), iterableWithSize(3));
                assertThat(
                    ((BoolQueryBuilder) multiSearchRequest.requests().get(0).source().query()).filter(),
                    containsInAnyOrder(
                        new TermQueryBuilder("user_profile.user.username.keyword", username),
                        new TermQueryBuilder("user_profile.user.realm.type", realmType),
                        new TermQueryBuilder("user_profile.user.realm.name", realmName)
                    )
                );
                var listener = (ActionListener<MultiSearchResponse>) invocation.getArgument(2);
                listener.onResponse(emptyMultiSearchResponse);
                return null;
            }).when(client).execute(eq(TransportMultiSearchAction.TYPE), any(MultiSearchRequest.class), anyActionListener());
            when(client.prepareMultiSearch()).thenReturn(new MultiSearchRequestBuilder(client));

            PlainActionFuture<Collection<String>> listener = new PlainActionFuture<>();
            profileService.resolveProfileUidsForApiKeys(apiKeys, listener);
            Collection<String> profileUids = listener.get();
            assertThat(profileUids, iterableWithSize(1));
            assertThat(profileUids.iterator().next(), nullValue());
        } finally {
            emptyMultiSearchResponse.decRef();
        }
    }

    @SuppressWarnings("unchecked")
    public void testProfileSearchForApiKeyOwnerWithDomain() throws Exception {
        String realmName = "realmName_" + randomAlphaOfLength(8);
        String realmType = "realmType_" + randomAlphaOfLength(8);
        String username = "username_" + randomAlphaOfLength(8);
        int domainSize = randomIntBetween(1, 3);
        Set<RealmConfig.RealmIdentifier> domain = new HashSet<>(domainSize + 1);
        domain.add(new RealmConfig.RealmIdentifier(realmType, realmName));
        for (int i = 0; i < domainSize; i++) {
            domain.add(new RealmConfig.RealmIdentifier("realmTypeFromDomain_" + i, "realmNameFromDomain_" + i));
        }
        RealmDomain realmDomain = new RealmDomain("domainName_ " + randomAlphaOfLength(8), domain);
        List<ApiKey> apiKeys = List.of(createApiKeyForOwner("keyId_" + randomAlphaOfLength(8), username, realmName, realmType));
        realmRefLookup = realmIdentifier -> {
            assertThat(realmIdentifier.getName(), is(realmName));
            assertThat(realmIdentifier.getType(), is(realmType));
            return new Authentication.RealmRef(realmName, realmType, "nodeName_" + randomAlphaOfLength(8), realmDomain);
        };
        MultiSearchResponse.Item[] responseItems = new MultiSearchResponse.Item[1];
        responseItems[0] = new MultiSearchResponse.Item(new TestEmptySearchResponse(), null);
        MultiSearchResponse emptyMultiSearchResponse = new MultiSearchResponse(responseItems, randomNonNegativeLong());
        try {
            doAnswer(invocation -> {
                assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
                MultiSearchRequest multiSearchRequest = (MultiSearchRequest) invocation.getArguments()[1];
                assertThat(multiSearchRequest.requests(), iterableWithSize(1));
                assertThat(multiSearchRequest.requests().get(0).source().query(), instanceOf(BoolQueryBuilder.class));
                assertThat(((BoolQueryBuilder) multiSearchRequest.requests().get(0).source().query()).filter(), iterableWithSize(1));
                assertThat(
                    ((BoolQueryBuilder) multiSearchRequest.requests().get(0).source().query()).filter(),
                    contains(new TermQueryBuilder("user_profile.user.username.keyword", username))
                );
                assertThat(
                    ((BoolQueryBuilder) multiSearchRequest.requests().get(0).source().query()).should(),
                    iterableWithSize(domain.size())
                );
                assertThat(((BoolQueryBuilder) multiSearchRequest.requests().get(0).source().query()).minimumShouldMatch(), is("1"));
                for (RealmConfig.RealmIdentifier domainRealmIdentifier : domain) {
                    BoolQueryBuilder realmDomainBoolQueryBuilder = new BoolQueryBuilder();
                    realmDomainBoolQueryBuilder.filter()
                        .add(new TermQueryBuilder("user_profile.user.realm.type", domainRealmIdentifier.getType()));
                    realmDomainBoolQueryBuilder.filter()
                        .add(new TermQueryBuilder("user_profile.user.realm.name", domainRealmIdentifier.getName()));
                    assertThat(
                        ((BoolQueryBuilder) multiSearchRequest.requests().get(0).source().query()).should(),
                        hasItem(realmDomainBoolQueryBuilder)
                    );
                }
                var listener = (ActionListener<MultiSearchResponse>) invocation.getArgument(2);
                listener.onResponse(emptyMultiSearchResponse);
                return null;
            }).when(client).execute(eq(TransportMultiSearchAction.TYPE), any(MultiSearchRequest.class), anyActionListener());
            when(client.prepareMultiSearch()).thenReturn(new MultiSearchRequestBuilder(client));

            PlainActionFuture<Collection<String>> listener = new PlainActionFuture<>();
            profileService.resolveProfileUidsForApiKeys(apiKeys, listener);
            Collection<String> profileUids = listener.get();
            assertThat(profileUids, iterableWithSize(1));
            assertThat(profileUids.iterator().next(), nullValue());
        } finally {
            emptyMultiSearchResponse.decRef();
        }
    }

    @SuppressWarnings("unchecked")
    public void testProfileSearchForOwnerOfMultipleApiKeys() throws Exception {
        String realmName = "realmName_" + randomAlphaOfLength(8);
        String realmType = "realmType_" + randomAlphaOfLength(8);
        String username = "username_" + randomAlphaOfLength(8);
        int apiKeyCount = randomIntBetween(2, 6);
        List<ApiKey> apiKeys = new ArrayList<>(apiKeyCount);
        for (int i = 0; i < apiKeyCount; i++) {
            // all keys have the same owner
            apiKeys.add(createApiKeyForOwner("keyId_" + i, username, realmName, realmType));
        }
        realmRefLookup = realmIdentifier -> {
            assertThat(realmIdentifier.getName(), is(realmName));
            assertThat(realmIdentifier.getType(), is(realmType));
            return new Authentication.RealmRef(realmName, realmType, "nodeName");
        };
        MultiSearchResponse.Item[] responseItems = new MultiSearchResponse.Item[1];
        responseItems[0] = new MultiSearchResponse.Item(new TestEmptySearchResponse(), null);
        MultiSearchResponse emptyMultiSearchResponse = new MultiSearchResponse(responseItems, randomNonNegativeLong());
        try {
            doAnswer(invocation -> {
                assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
                MultiSearchRequest multiSearchRequest = (MultiSearchRequest) invocation.getArguments()[1];
                // a single search request for a single owner of multiple keys
                assertThat(multiSearchRequest.requests(), iterableWithSize(1));
                assertThat(multiSearchRequest.requests().get(0).source().query(), instanceOf(BoolQueryBuilder.class));
                assertThat(((BoolQueryBuilder) multiSearchRequest.requests().get(0).source().query()).filter(), iterableWithSize(3));
                assertThat(
                    ((BoolQueryBuilder) multiSearchRequest.requests().get(0).source().query()).filter(),
                    containsInAnyOrder(
                        new TermQueryBuilder("user_profile.user.username.keyword", username),
                        new TermQueryBuilder("user_profile.user.realm.type", realmType),
                        new TermQueryBuilder("user_profile.user.realm.name", realmName)
                    )
                );
                var listener = (ActionListener<MultiSearchResponse>) invocation.getArgument(2);
                listener.onResponse(emptyMultiSearchResponse);
                return null;
            }).when(client).execute(eq(TransportMultiSearchAction.TYPE), any(MultiSearchRequest.class), anyActionListener());
            when(client.prepareMultiSearch()).thenReturn(new MultiSearchRequestBuilder(client));

            PlainActionFuture<Collection<String>> listener = new PlainActionFuture<>();
            profileService.resolveProfileUidsForApiKeys(apiKeys, listener);
            Collection<String> profileUids = listener.get();
            assertThat(profileUids, iterableWithSize(apiKeyCount));
            var profileUidsIterator = profileUids.iterator();
            while (profileUidsIterator.hasNext()) {
                assertThat(profileUidsIterator.next(), nullValue());
            }
        } finally {
            emptyMultiSearchResponse.decRef();
        }
    }

    public void testProfileSearchErrorForApiKeyOwner() {
        // 2 keys with different owners
        List<ApiKey> apiKeys = List.of(
            createApiKeyForOwner("keyId_0", "username_0", "realmName_0", "realmType_0"),
            createApiKeyForOwner("keyId_1", "username_1", "realmName_1", "realmType_1")
        );
        realmRefLookup = realmIdentifier -> {
            assertThat(realmIdentifier.getName(), either(is("realmName_0")).or(is("realmName_1")));
            assertThat(realmIdentifier.getType(), either(is("realmType_0")).or(is("realmType_1")));
            return new Authentication.RealmRef(realmIdentifier.getName(), realmIdentifier.getType(), "nodeName");
        };
        MultiSearchResponse.Item[] responseItems = new MultiSearchResponse.Item[2];
        // one search request (for one of the key owner) fails
        if (randomBoolean()) {
            responseItems[0] = new MultiSearchResponse.Item(new TestEmptySearchResponse(), null);
            responseItems[1] = new MultiSearchResponse.Item(null, new Exception("test search failure"));
        } else {
            responseItems[0] = new MultiSearchResponse.Item(null, new Exception("test search failure"));
            responseItems[1] = new MultiSearchResponse.Item(new TestEmptySearchResponse(), null);
        }
        MultiSearchResponse multiSearchResponseWithError = new MultiSearchResponse(responseItems, randomNonNegativeLong());
        try {
            doAnswer(invocation -> {
                assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
                // a single search request for a single owner of multiple keys
                MultiSearchRequest multiSearchRequest = (MultiSearchRequest) invocation.getArguments()[1];
                // 2 search requests for the 2 Api key owners
                assertThat(multiSearchRequest.requests(), iterableWithSize(2));
                for (int i = 0; i < 2; i++) {
                    assertThat(multiSearchRequest.requests().get(i).source().query(), instanceOf(BoolQueryBuilder.class));
                    assertThat(((BoolQueryBuilder) multiSearchRequest.requests().get(i).source().query()).filter(), iterableWithSize(3));
                    List<QueryBuilder> filters = ((BoolQueryBuilder) multiSearchRequest.requests().get(i).source().query()).filter();
                    assertThat(
                        filters,
                        either(
                            Matchers.<QueryBuilder>containsInAnyOrder(
                                new TermQueryBuilder("user_profile.user.username.keyword", "username_1"),
                                new TermQueryBuilder("user_profile.user.realm.type", "realmType_1"),
                                new TermQueryBuilder("user_profile.user.realm.name", "realmName_1")
                            )
                        ).or(
                            Matchers.<QueryBuilder>containsInAnyOrder(
                                new TermQueryBuilder("user_profile.user.username.keyword", "username_0"),
                                new TermQueryBuilder("user_profile.user.realm.type", "realmType_0"),
                                new TermQueryBuilder("user_profile.user.realm.name", "realmName_0")
                            )
                        )
                    );
                }
                @SuppressWarnings("unchecked")
                var listener = (ActionListener<MultiSearchResponse>) invocation.getArgument(2);
                listener.onResponse(multiSearchResponseWithError);
                return null;
            }).when(client).execute(eq(TransportMultiSearchAction.TYPE), any(MultiSearchRequest.class), anyActionListener());
            when(client.prepareMultiSearch()).thenReturn(new MultiSearchRequestBuilder(client));

            PlainActionFuture<Collection<String>> listener = new PlainActionFuture<>();
            profileService.resolveProfileUidsForApiKeys(apiKeys, listener);
            ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
            assertThat(
                e.getMessage(),
                containsString("failed to retrieve profile for users. please retry without fetching profile uid (with_profile_uid=false)")
            );
        } finally {
            multiSearchResponseWithError.decRef();
        }
    }

    public void testUnclearApiKeyOwnersAreIgnoredWhenRetrievingProfiles() throws Exception {
        String realmName = "realmName_" + randomAlphaOfLength(8);
        String realmType = "realmType_" + randomAlphaOfLength(8);
        String username = "username_" + randomAlphaOfLength(8);
        List<ApiKey> apiKeys = List.of(
            // null username
            createApiKeyForOwner("keyId_" + randomAlphaOfLength(8), null, randomAlphaOfLength(4), randomAlphaOfLength(4)),
            // null realm name
            createApiKeyForOwner("keyId_" + randomAlphaOfLength(8), randomAlphaOfLength(4), null, randomAlphaOfLength(4)),
            // null realm type
            createApiKeyForOwner("keyId_" + randomAlphaOfLength(8), randomAlphaOfLength(4), randomAlphaOfLength(4), null),
            // the realm does not exist
            createApiKeyForOwner("keyId_" + randomAlphaOfLength(8), username, realmName, realmType)
        );
        realmRefLookup = realmIdentifier -> {
            assertThat(realmIdentifier.getName(), is(realmName));
            assertThat(realmIdentifier.getType(), is(realmType));
            // realm not configured
            return null;
        };
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<MultiSearchResponse>) invocation.getArgument(2);
            listener.onFailure(new Exception("test failed, code should not be reachable"));
            return null;
        }).when(client).execute(eq(TransportMultiSearchAction.TYPE), any(MultiSearchRequest.class), anyActionListener());
        when(client.prepareMultiSearch()).thenReturn(new MultiSearchRequestBuilder(client));

        PlainActionFuture<Collection<String>> listener = new PlainActionFuture<>();
        profileService.resolveProfileUidsForApiKeys(apiKeys, listener);
        Collection<String> profileUids = listener.get();
        assertThat(profileUids, iterableWithSize(4));
        assertThat(profileUids, contains(nullValue(), nullValue(), nullValue(), nullValue()));
    }

    public void testProfilesIndexMissingOrUnavailableWhenRetrievingProfilesOfApiKeyOwners() throws Exception {
        // profiles index missing
        SecurityIndexManager.IndexState projectIndex = profileIndex.forCurrentProject();
        when(projectIndex.indexExists()).thenReturn(false);
        String realmName = "realmName_" + randomAlphaOfLength(8);
        String realmType = "realmType_" + randomAlphaOfLength(8);
        String username = "username_" + randomAlphaOfLength(8);
        List<ApiKey> apiKeys = List.of(createApiKeyForOwner("keyId_" + randomAlphaOfLength(8), username, realmName, realmType));
        realmRefLookup = realmIdentifier -> {
            assertThat(realmIdentifier.getName(), is(realmName));
            assertThat(realmIdentifier.getType(), is(realmType));
            return new Authentication.RealmRef(realmName, realmType, "nodeName_" + randomAlphaOfLength(8));
        };
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<MultiSearchResponse>) invocation.getArgument(2);
            listener.onFailure(new Exception("test failed, code should not be reachable"));
            return null;
        }).when(client).execute(eq(TransportMultiSearchAction.TYPE), any(MultiSearchRequest.class), anyActionListener());
        when(client.prepareMultiSearch()).thenReturn(new MultiSearchRequestBuilder(client));
        PlainActionFuture<Collection<String>> listener = new PlainActionFuture<>();
        profileService.resolveProfileUidsForApiKeys(apiKeys, listener);
        Collection<String> profileUids = listener.get();
        assertThat(profileUids, nullValue());
        // profiles index unavailable
        Mockito.reset(projectIndex);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.isAvailable(any())).thenReturn(false);
        when(projectIndex.getUnavailableReason(any())).thenReturn(new ElasticsearchException("test unavailable"));
        listener = new PlainActionFuture<>();
        profileService.resolveProfileUidsForApiKeys(apiKeys, listener);
        PlainActionFuture<Collection<String>> finalListener = listener;
        ExecutionException e = expectThrows(ExecutionException.class, () -> finalListener.get());
        assertThat(e.getMessage(), containsString("test unavailable"));
    }

    record SampleDocumentParameter(String uid, String username, List<String> roles, long lastSynchronized) {}

    private void mockMultiGetRequest(List<SampleDocumentParameter> sampleDocumentParameters) {
        mockMultiGetRequest(sampleDocumentParameters, Map.of());
    }

    private void mockMultiGetRequest(List<SampleDocumentParameter> sampleDocumentParameters, Map<String, Exception> errors) {
        doAnswer(invocation -> {
            assertThat(threadPool.getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME), equalTo(SECURITY_PROFILE_ORIGIN));
            final MultiGetRequest multiGetRequest = (MultiGetRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            final ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocation.getArguments()[2];
            client.multiGet(multiGetRequest, listener);
            return null;
        }).when(client).execute(eq(TransportMultiGetAction.TYPE), any(MultiGetRequest.class), anyActionListener());

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
        return String.format(
            Locale.ROOT,
            SAMPLE_PROFILE_DOCUMENT_TEMPLATE,
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

    private static ApiKey createApiKeyForOwner(String apiKeyId, String username, String realmName, String realmType) {
        return new ApiKey(
            randomAlphaOfLength(4),
            apiKeyId,
            randomFrom(ApiKey.Type.values()),
            Instant.now(),
            Instant.now().plusSeconds(3600),
            false,
            null,
            username,
            realmName,
            realmType,
            null,
            List.of(
                new RoleDescriptor(
                    randomAlphaOfLength(5),
                    new String[] { "manage_index_template" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("rad").build() },
                    null,
                    null,
                    null,
                    Map.of("_key", "value"),
                    null,
                    null,
                    null,
                    null,
                    null
                )
            ),
            null
        );
    }

    private static class TestEmptySearchResponse extends SearchResponse {

        TestEmptySearchResponse() {
            super(
                SearchHits.EMPTY_WITH_TOTAL_HITS,
                null,
                null,
                false,
                null,
                null,
                1,
                null,
                0,
                0,
                0,
                0L,
                ShardSearchFailure.EMPTY_ARRAY,
                Clusters.EMPTY,
                null
            );
        }
    }
}
