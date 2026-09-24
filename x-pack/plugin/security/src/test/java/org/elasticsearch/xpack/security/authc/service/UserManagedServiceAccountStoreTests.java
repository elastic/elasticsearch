/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountAuthor;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.SecurityFeatures;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.xpack.security.authc.service.UserManagedServiceAccountStore.SERVICE_ACCOUNT_DOC_TYPE;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserManagedServiceAccountStoreTests extends ESTestCase {

    private static final String PRINCIPAL = "engineering/deploy_bot";
    private static final ServiceAccountId ACCOUNT_ID = ServiceAccountId.fromPrincipal(PRINCIPAL);
    private static final String DOC_ID = SERVICE_ACCOUNT_DOC_TYPE + "-" + PRINCIPAL;
    private static final String ROLE_A = "deploy_bot_role_a";
    private static final String ROLE_B = "deploy_bot_role_b";

    private static final Instant NOW = Instant.ofEpochMilli(1_700_000_000_000L);

    private Client client;
    private Authentication authentication;
    private ClusterService clusterService;
    private ClusterState clusterState;
    private FeatureService featureService;
    private SecurityIndexManager securityIndex;
    private SecurityIndexManager.IndexState projectIndex;
    private UserManagedServiceAccountStore store;

    private final List<ActionRequest> requests = new ArrayList<>();
    private final AtomicInteger getRequestCount = new AtomicInteger();
    private final List<String> clearedCacheKeys = new ArrayList<>();
    private final AtomicReference<BiConsumer<ActionRequest, ActionListener<ActionResponse>>> responseProvider = new AtomicReference<>();

    /**
     * The store is driven through a real {@link FilterClient} that records every request and answers it from
     * {@link #responseProvider}, so the tests assert on the requests the store actually builds rather than on mock
     * interactions. The collaborators below are mocked because neither can be constructed without a running node: a
     * {@link Client} needs a transport, and a {@link SecurityIndexManager} needs cluster state, index mappings and a
     * project resolver. {@link CacheInvalidatorRegistry} is cheap to construct, so the real one is used.
     */
    @Before
    public void init() {
        responseProvider.set((request, listener) -> fail("unexpected request " + request));

        final Client mockClient = mock(Client.class);
        when(mockClient.settings()).thenReturn(Settings.EMPTY);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(mockClient.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = new FilterClient(mockClient) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                requests.add(request);
                if (request instanceof GetRequest) {
                    getRequestCount.incrementAndGet();
                }
                responseProvider.get().accept(request, (ActionListener<ActionResponse>) listener);
            }
        };

        // Cluster state is only passed to FeatureService to decide whether every node supports
        // creating accounts. The document version is a format number, not a release version.
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(SecurityFeatures.USER_MANAGED_SERVICE_ACCOUNTS))).thenReturn(true);
        when(featureService.clusterHasFeature(any(), eq(SecurityFeatures.USER_MANAGED_SERVICE_ACCOUNT_ATTRIBUTION))).thenReturn(true);
        authentication = AuthenticationTestHelper.builder().realm().build(false);

        securityIndex = mock(SecurityIndexManager.class);
        projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        // Running the action inline bypasses the index version check and the index creation a real
        // SecurityIndexManager would perform first; the tests that need those to fail stub indexExists and
        // isAvailable instead.
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[1]).run();
            return null;
        }).when(projectIndex).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[1]).run();
            return null;
        }).when(projectIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));

        store = newStore(Settings.EMPTY);
    }

    public void testLoadedAccountIsAuthorizedByItsNamedRoles() {
        final boolean enabled = randomBoolean();
        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_A, ROLE_B), enabled));

        final UserManagedServiceAccount account = getByPrincipal(PRINCIPAL);
        assertThat(account.id(), equalTo(ACCOUNT_ID));
        assertThat(account.roles(), contains(ROLE_A, ROLE_B));
        assertThat(account.enabled(), is(enabled));
        assertThat(account.asUser().principal(), equalTo(PRINCIPAL));
        assertThat(account.asUser().roles(), arrayContaining(ROLE_A, ROLE_B));
        assertThat(account.asUser().enabled(), is(enabled));
        // The marker is what routes authorization to the named roles above rather than to a built-in account of
        // the same name, so an account without it would silently authorize as something else.
        assertThat(account.asUser().metadata(), equalTo(Map.of(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD, true)));
    }

    public void testGetByPrincipalCachesTheAccount() {
        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_A), true));

        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_A));
        assertThat(getRequestCount.get(), equalTo(1));

        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_A));
        assertThat(getRequestCount.get(), equalTo(1));
    }

    public void testGetByPrincipalDoesNotCacheTheAbsenceOfAnAccount() {
        respondToGetWith(null);

        assertThat(getByPrincipal(PRINCIPAL), nullValue());
        assertThat(getRequestCount.get(), equalTo(1));

        assertThat(getByPrincipal(PRINCIPAL), nullValue());
        assertThat(getRequestCount.get(), equalTo(2));
    }

    public void testGetByPrincipalFindsNothingForPrincipalsNoAccountCouldHold() {
        final String principal = randomFrom(
            reservedNamespace() + "/fleet-server", // the reserved namespace, in any capitalization
            "engineering",                         // not a {namespace}/{service-name} pair
            "engineering/deploy bot",              // outside the permitted character set
            "_engineering/deploy_bot"              // does not start with a letter or digit
        );
        assertThat(getByPrincipal(principal), nullValue());
        assertThat(getRequestCount.get(), equalTo(0));
    }

    public void testMalformedDocumentsAreTreatedAsAbsentAccounts() {
        final Map<String, Consumer<Map<String, Object>>> corruptions = new LinkedHashMap<>();
        corruptions.put("doc_type of another document type", source -> source.put("doc_type", "user"));
        corruptions.put("missing doc_type", source -> source.remove("doc_type"));
        corruptions.put("username of another account", source -> source.put("username", "engineering/other_bot"));
        corruptions.put("missing username", source -> source.remove("username"));
        corruptions.put("missing roles", source -> source.remove("roles"));
        corruptions.put("roles that is not a list", source -> source.put("roles", ROLE_A));
        corruptions.put("a role that is not a string", source -> source.put("roles", List.of(ROLE_A, 42)));
        corruptions.put("missing enabled", source -> source.remove("enabled"));
        corruptions.put("enabled that is not a boolean", source -> source.put("enabled", "true"));
        corruptions.put("description that is not a string", source -> source.put("description", List.of("a", "b")));

        corruptions.forEach((description, corruption) -> {
            final Map<String, Object> source = accountDocument(PRINCIPAL, List.of(ROLE_A), true);
            corruption.accept(source);
            respondToGetWith(source);
            store.invalidateAll();
            assertThat("document with " + description, getByPrincipal(PRINCIPAL), nullValue());
        });
    }

    /**
     * Documents written before the field existed have no description, as do accounts written without one since, and
     * both read back as an account with none.
     */
    public void testAnAbsentOrNullDescriptionReadsBackAsNone() {
        final Map<String, Object> source = accountDocument(PRINCIPAL, List.of(ROLE_A), true);
        if (randomBoolean()) {
            source.put("description", null);
        }
        respondToGetWith(source);
        assertThat(getByPrincipal(PRINCIPAL).description(), nullValue());
    }

    public void testAStoredDescriptionIsLoaded() {
        final String description = randomAlphaOfLengthBetween(1, 30);
        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_A), true, description));
        assertThat(getByPrincipal(PRINCIPAL).description(), equalTo(description));
    }

    public void testStoredAttributionIsLoaded() {
        final ServiceAccountAuthor creator = randomAuthor();
        final ServiceAccountAuthor editor = randomAuthor();
        final Instant createdAt = Instant.ofEpochMilli(randomLongBetween(0, NOW.toEpochMilli()));
        final Instant editedAt = Instant.ofEpochMilli(randomLongBetween(createdAt.toEpochMilli(), NOW.toEpochMilli()));
        final Map<String, Object> source = accountDocument(PRINCIPAL, List.of(ROLE_A), true);
        source.put("creator", storedAuthor(creator));
        source.put("created_at", createdAt.toEpochMilli());
        // The editor is absent until the account is first replaced.
        final boolean edited = randomBoolean();
        if (edited) {
            source.put("editor", storedAuthor(editor));
            source.put("edited_at", editedAt.toEpochMilli());
        }
        respondToGetWith(source);

        final UserManagedServiceAccount account = getByPrincipal(PRINCIPAL);
        assertThat(account.creator(), equalTo(creator));
        assertThat(account.createdAt(), equalTo(createdAt));
        assertThat(account.editor(), edited ? equalTo(editor) : nullValue());
        assertThat(account.editedAt(), edited ? equalTo(editedAt) : nullValue());
        // Attribution is for administrators, not for authorization or audit, so the user is unchanged by it.
        assertThat(account.asUser().metadata().keySet(), contains(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD));
    }

    /**
     * Documents written before attribution was recorded have none of the fields, and read back as an account whose
     * creator and editor are unknown.
     */
    public void testAnAccountWrittenBeforeAttributionWasRecordedReadsBackWithoutIt() {
        final Map<String, Object> source = accountDocument(PRINCIPAL, List.of(ROLE_A), true);
        source.put("version", 1);
        respondToGetWith(source);

        final UserManagedServiceAccount account = getByPrincipal(PRINCIPAL);
        assertThat(account.creator(), nullValue());
        assertThat(account.createdAt(), nullValue());
        assertThat(account.editor(), nullValue());
        assertThat(account.editedAt(), nullValue());
    }

    public void testMalformedAttributionIsTreatedAsAnAbsentAccount() {
        final Map<String, Consumer<Map<String, Object>>> corruptions = new LinkedHashMap<>();
        corruptions.put("creator that is not an object", source -> source.put("creator", "alice"));
        corruptions.put("creator without a principal", source -> {
            final Map<String, Object> creator = new HashMap<>(storedAuthor(randomAuthor()));
            creator.remove("principal");
            source.put("creator", creator);
        });
        corruptions.put("creator with a null realm", source -> {
            final Map<String, Object> creator = new HashMap<>(storedAuthor(randomAuthor()));
            creator.put("realm", null);
            source.put("creator", creator);
        });
        corruptions.put("creator with a full name that is not a string", source -> {
            final Map<String, Object> creator = new HashMap<>(storedAuthor(randomAuthor()));
            creator.put("full_name", 42);
            source.put("creator", creator);
        });
        corruptions.put("creator with a realm domain that is not an object", source -> {
            final Map<String, Object> creator = new HashMap<>(storedAuthor(randomAuthor()));
            creator.put("realm_domain", "domain1");
            source.put("creator", creator);
        });
        corruptions.put("creator with a realm domain without a name", source -> {
            final Map<String, Object> creator = new HashMap<>(storedAuthor(randomAuthor()));
            creator.put("realm_domain", Map.of("realms", List.of()));
            source.put("creator", creator);
        });
        corruptions.put("created_at that is not a number", source -> source.put("created_at", "2024-01-01"));
        corruptions.put("editor that is not an object", source -> source.put("editor", List.of("bob")));
        corruptions.put("edited_at that is not a number", source -> source.put("edited_at", true));
        corruptions.put("creator with an api key that is not an object", source -> {
            final Map<String, Object> creator = new HashMap<>(storedAuthor(randomAuthor()));
            creator.put("api_key", "VuaCfGcBCdbkQm-e5aOx");
            source.put("creator", creator);
        });
        corruptions.put("creator with an api key without an id", source -> {
            final Map<String, Object> creator = new HashMap<>(storedAuthor(randomAuthor()));
            creator.put("api_key", Map.of("name", "deploy-bot-key"));
            source.put("creator", creator);
        });

        corruptions.forEach((description, corruption) -> {
            final Map<String, Object> source = accountDocument(PRINCIPAL, List.of(ROLE_A), true);
            corruption.accept(source);
            respondToGetWith(source);
            store.invalidateAll();
            assertThat("document with " + description, getByPrincipal(PRINCIPAL), nullValue());
        });
    }

    /**
     * Like role names, the description's write-time rule is not applied on read: tightening the cap later must not
     * make an already-stored account unreadable.
     */
    public void testAStoredDescriptionThatWouldFailWriteValidationIsStillLoaded() {
        final String storedDescription = randomAlphaOfLength(Validation.UserManagedServiceAccounts.MAX_DESCRIPTION_LENGTH + 1);
        assertNotNull(Validation.UserManagedServiceAccounts.validateDescription(storedDescription));
        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_A), true, storedDescription));
        assertThat(getByPrincipal(PRINCIPAL).description(), equalTo(storedDescription));
    }

    public void testAStoredRoleNameThatWouldFailWriteValidationIsStillLoaded() {
        final String storedRole = " leading space";
        assertNotNull(NativeRealmValidationUtil.validateRoleName(storedRole, true));
        respondToGetWith(accountDocument(PRINCIPAL, List.of(storedRole), true));
        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(storedRole));
    }

    public void testAReadThatRacedAnInvalidationDoesNotPopulateTheCache() throws Exception {
        final CountDownLatch releaseGet = new CountDownLatch(1);
        responseProvider.set((request, listener) -> {
            try {
                releaseGet.await();
                respondToGet(request, accountDocument(PRINCIPAL, List.of(ROLE_A), true), listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });

        final PlainActionFuture<UserManagedServiceAccount> racingRead = new PlainActionFuture<>();
        final Thread readingThread = new Thread(() -> store.getByPrincipal(PRINCIPAL, racingRead));
        readingThread.start();
        assertBusy(() -> assertThat(getRequestCount.get(), equalTo(1)));

        store.invalidate(List.of(PRINCIPAL));
        releaseGet.countDown();
        readingThread.join();

        // The in-flight read still answers its own caller, but its result is too old to be shared.
        assertThat(racingRead.actionGet().roles(), contains(ROLE_A));
        assertThat(store.getAccountCache().get(PRINCIPAL), nullValue());

        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_B), true));
        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_B));
        assertThat(getRequestCount.get(), equalTo(2));
    }

    public void testPutAccountWritesTheDocumentAndClearsTheCacheClusterWide() {
        store = newStore(randomCacheTtlSettings());
        respondWithUpdateResult(DocWriteResponse.Result.CREATED);

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(
            ACCOUNT_ID,
            List.of(ROLE_B, ROLE_A, ROLE_B),
            false,
            "Deploys things",
            authentication,
            RefreshPolicy.WAIT_UNTIL,
            future
        );
        assertThat(future.actionGet(), is(UserManagedServiceAccountStore.PutResult.CREATED));

        final UpdateRequest updateRequest = updateRequest();
        assertThat(updateRequest.getRefreshPolicy(), is(RefreshPolicy.WAIT_UNTIL));
        assertThat(updateRequest.id(), equalTo(DOC_ID));
        final Map<String, Object> upsert = upsertDocument();
        assertThat(upsert.get("doc_type"), equalTo(SERVICE_ACCOUNT_DOC_TYPE));
        assertThat(upsert.get("username"), equalTo(PRINCIPAL));
        assertThat(upsert.get("version"), equalTo(UserManagedServiceAccount.Version.CURRENT.id()));
        assertThat(upsert.get("enabled"), is(false));
        // Sorted and de-duplicated, so that the document does not depend on how the caller ordered the roles.
        assertThat(upsert.get("roles"), equalTo(List.of(ROLE_A, ROLE_B)));
        assertThat(upsert.get("description"), equalTo("Deploys things"));

        // The changes carry the same account fields, so that an existing document ends up the same as a new one would.
        final Map<String, Object> changes = changesDocument();
        assertThat(changes.get("version"), equalTo(UserManagedServiceAccount.Version.CURRENT.id()));
        assertThat(changes.get("enabled"), is(false));
        assertThat(changes.get("roles"), equalTo(List.of(ROLE_A, ROLE_B)));
        assertThat(changes.get("description"), equalTo("Deploys things"));
        assertThat(changes, not(hasKey("doc_type")));
        assertThat(changes, not(hasKey("username")));

        assertThat(clearedCacheKeys, contains(PRINCIPAL));
    }

    /**
     * A new document records the caller as its creator and has no editor yet. The changes for an existing document
     * record the caller as its editor and leave the creator alone.
     */
    public void testPutAccountAttributesACreationToTheCallerAndAReplacementToTheEditor() {
        respondWithUpdateResult(randomFrom(DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED));
        final ServiceAccountAuthor author = ServiceAccountAuthor.fromAuthentication(authentication);

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, randomDescription(), authentication, RefreshPolicy.NONE, future);
        future.actionGet();

        final Map<String, Object> upsert = upsertDocument();
        assertThat(upsert.get("creator"), equalTo(storedAuthor(author)));
        assertThat(upsert.get("created_at"), equalTo(NOW.toEpochMilli()));
        assertThat(upsert, not(hasKey("editor")));
        assertThat(upsert, not(hasKey("edited_at")));

        final Map<String, Object> changes = changesDocument();
        assertThat(changes.get("editor"), equalTo(storedAuthor(author)));
        assertThat(changes.get("edited_at"), equalTo(NOW.toEpochMilli()));
        assertThat(changes, not(hasKey("creator")));
        assertThat(changes, not(hasKey("created_at")));
    }

    /**
     * The author is the effective subject: a request run as another user is attributed to that user, and one made
     * with an API key to the key's owner.
     */
    public void testPutAccountAttributesTheWriteToTheEffectiveSubject() {
        authentication = randomBoolean()
            ? AuthenticationTestHelper.builder().realm().runAs().build(false)
            : AuthenticationTestHelper.builder().apiKey().build(false);
        respondWithUpdateResult(DocWriteResponse.Result.CREATED);

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, null, authentication, RefreshPolicy.NONE, future);
        future.actionGet();

        @SuppressWarnings("unchecked")
        final Map<String, Object> creator = (Map<String, Object>) upsertDocument().get("creator");
        assertThat(creator.get("principal"), equalTo(authentication.getEffectiveSubject().getUser().principal()));
        assertThat(creator.get("realm"), equalTo(ApiKeyService.getCreatorRealmName(authentication)));
        assertThat(creator.get("realm_type"), equalTo(ApiKeyService.getCreatorRealmType(authentication)));
        assertThat(creator, not(hasKey("metadata")));
        // A write through a key records the key; one run as another user records none, as an explicit null.
        assertThat(creator, hasKey("api_key"));
        if (authentication.isApiKey()) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> apiKey = (Map<String, Object>) creator.get("api_key");
            assertThat(
                apiKey.get("id"),
                equalTo(authentication.getEffectiveSubject().getMetadata().get(AuthenticationField.API_KEY_ID_KEY))
            );
        } else {
            assertThat(creator.get("api_key"), nullValue());
        }
    }

    /**
     * The attribution fields are only written once every node declares them, because until then the strict mapping
     * may not hold them. The account itself is still written.
     */
    public void testPutAccountLeavesTheAttributionOutUntilEveryNodeSupportsIt() {
        when(featureService.clusterHasFeature(any(), eq(SecurityFeatures.USER_MANAGED_SERVICE_ACCOUNT_ATTRIBUTION))).thenReturn(false);
        respondWithUpdateResult(DocWriteResponse.Result.CREATED);

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, randomDescription(), authentication, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(UserManagedServiceAccountStore.PutResult.CREATED));

        for (Map<String, Object> document : List.of(upsertDocument(), changesDocument())) {
            assertThat(document.get("roles"), equalTo(List.of(ROLE_A)));
            for (String field : List.of("creator", "created_at", "editor", "edited_at")) {
                assertThat(field, document, not(hasKey(field)));
            }
        }
    }

    /**
     * Written as no field at all in a new document rather than as a null, so that an account without a description
     * has the same document as one written before the field existed. The changes for an existing document write it
     * as an explicit null instead: they are merged into the document, and left out, the old description would stay.
     */
    public void testPutAccountLeavesTheDescriptionOutOfANewDocumentAndClearsItInAnExistingOne() {
        respondWithUpdateResult(DocWriteResponse.Result.CREATED);

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, null, authentication, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(UserManagedServiceAccountStore.PutResult.CREATED));

        assertThat(upsertDocument(), not(hasKey("description")));
        final Map<String, Object> changes = changesDocument();
        assertThat(changes, hasKey("description"));
        assertThat(changes.get("description"), nullValue());
    }

    /**
     * An editor's absent fields are written as explicit nulls for the same reason: merged field by field, a left-out
     * field would keep whatever the previous editor had there.
     */
    public void testPutAccountWritesEveryFieldOfTheEditorSoThatAPreviousEditorCannotShowThrough() {
        authentication = AuthenticationTestHelper.builder()
            .realm(false)
            .user(new User(randomAlphaOfLengthBetween(3, 8), new String[] { "role" }, null, null, Map.of(), true))
            .build(false);
        respondWithUpdateResult(DocWriteResponse.Result.UPDATED);

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, null, authentication, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(UserManagedServiceAccountStore.PutResult.UPDATED));

        @SuppressWarnings("unchecked")
        final Map<String, Object> editor = (Map<String, Object>) changesDocument().get("editor");
        assertThat(editor.keySet(), equalTo(Set.of("principal", "full_name", "email", "realm", "realm_type", "realm_domain", "api_key")));
        assertThat(editor.get("full_name"), nullValue());
        assertThat(editor.get("email"), nullValue());
        assertThat(editor.get("realm_domain"), nullValue());
    }

    /**
     * An unnamed API key must clear the previous editor's key name when the update merges nested objects.
     */
    public void testPutAccountClearsThePreviousEditorsApiKeyName() {
        respondWithUpdateResult(DocWriteResponse.Result.UPDATED);
        final Map<String, Object> source = accountDocument(PRINCIPAL, List.of(ROLE_A), true);
        for (String keyId : List.of("named-key-id", "unnamed-key-id")) {
            final Map<String, Object> metadata = new HashMap<>();
            metadata.put(AuthenticationField.API_KEY_ID_KEY, keyId);
            metadata.put(AuthenticationField.API_KEY_NAME_KEY, keyId.equals("named-key-id") ? "deploy-bot-key" : null);
            authentication = AuthenticationTestHelper.builder().apiKey().metadata(metadata).build(false);
            requests.clear();

            final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
            store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, null, authentication, RefreshPolicy.NONE, future);
            assertThat(future.actionGet(), is(UserManagedServiceAccountStore.PutResult.UPDATED));

            // Apply the same recursive merge used by UpdateHelper for a partial document update.
            XContentHelper.update(source, changesDocument(), false);
            assertThat(source.get("editor"), equalTo(storedAuthor(ServiceAccountAuthor.fromAuthentication(authentication))));
        }
    }

    public void testPutAccountReportsAnUpdateOfAnExistingAccount() {
        // A no-op needs the same caller to write the same account within the same millisecond; it is reported as
        // an update rather than distinguished.
        respondWithUpdateResult(randomFrom(DocWriteResponse.Result.UPDATED, DocWriteResponse.Result.NOOP));

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, randomDescription(), authentication, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(UserManagedServiceAccountStore.PutResult.UPDATED));
        assertThat(clearedCacheKeys, contains(PRINCIPAL));
    }

    public void testPutAccountReportsEveryValidationErrorAtOnce() {
        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(
            new ServiceAccountId(reservedNamespace(), "deploy bot"),
            List.of("a role name that is far too long".repeat(32)),
            true,
            randomAlphaOfLength(Validation.UserManagedServiceAccounts.MAX_DESCRIPTION_LENGTH + 1),
            authentication,
            RefreshPolicy.NONE,
            future
        );

        final ValidationException e = expectThrows(ValidationException.class, future::actionGet);
        assertThat(e.validationErrors(), hasSize(4));
        assertThat(e.getMessage(), containsString("the [elastic] namespace is reserved for built-in service accounts"));
        assertThat(e.getMessage(), containsString("service account service name [deploy bot]"));
        assertThat(e.getMessage(), containsString("Role names must be at least"));
        assertThat(e.getMessage(), containsString("a service account description may not be more than"));
    }

    public void testPutAccountRejectsAnOverlongDescription() {
        final int max = Validation.UserManagedServiceAccounts.MAX_DESCRIPTION_LENGTH;
        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, randomAlphaOfLength(max + 1), authentication, RefreshPolicy.NONE, future);

        final ValidationException e = expectThrows(ValidationException.class, future::actionGet);
        assertThat(
            e.validationErrors(),
            contains("a service account description may not be more than " + max + " characters long, but [" + (max + 1) + "] were given")
        );
    }

    public void testPutAccountRejectsMoreRolesThanAnAccountMayHold() {
        final int max = Validation.UserManagedServiceAccounts.MAX_ROLES;
        final List<String> tooMany = randomBoolean()
            ? IntStream.range(0, max + 1).mapToObj(i -> "role-" + i).toList()
            : Collections.nCopies(max + 1, "role-a");
        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, tooMany, true, randomDescription(), authentication, RefreshPolicy.NONE, future);

        final ValidationException e = expectThrows(ValidationException.class, future::actionGet);
        assertThat(
            e.validationErrors(),
            contains("a service account may not have more than " + max + " roles, but [" + (max + 1) + "] were given")
        );
    }

    public void testPutAccountRequiresEveryNodeToSupportUserManagedServiceAccounts() {
        when(featureService.clusterHasFeature(any(), eq(SecurityFeatures.USER_MANAGED_SERVICE_ACCOUNTS))).thenReturn(false);

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, randomDescription(), authentication, RefreshPolicy.NONE, future);

        final IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            equalTo("cannot create a user-managed service account because not all nodes in the cluster support them yet")
        );
    }

    public void testDeleteAccountClearsTheCacheClusterWide() {
        store = newStore(randomCacheTtlSettings());
        respondWithDeleteResult(true);

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.IMMEDIATE, future);
        assertThat(future.actionGet(), is(true));

        assertThat(clearedCacheKeys, contains(PRINCIPAL));
    }

    public void testDeleteAccountClearsTheCacheEvenWhenThereWasNothingToDelete() {
        respondWithDeleteResult(false);

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.IMMEDIATE, future);
        assertThat(future.actionGet(), is(false));

        assertThat(clearedCacheKeys, contains(PRINCIPAL));
    }

    public void testDeleteAccountFailsWhenTheCacheCannotBeCleared() {
        final ElasticsearchException failure = new ElasticsearchException("node unreachable");
        final boolean found = randomBoolean();
        responseProvider.set((request, listener) -> {
            if (request instanceof DeleteRequest) {
                listener.onResponse(deleteResponse(found));
            } else if (request instanceof ClearSecurityCacheRequest) {
                listener.onFailure(failure);
            } else {
                fail("unexpected request " + request);
            }
        });

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.IMMEDIATE, future);

        final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("clearing the cache for service account [" + PRINCIPAL + "] failed"));
        assertThat(e.getCause(), is(failure));
    }

    public void testDeleteAccountRejectsAnIdNoAccountCouldHold() {
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(new ServiceAccountId(reservedNamespace(), "fleet-server"), RefreshPolicy.NONE, future);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), equalTo("the [elastic] namespace is reserved for built-in service accounts"));
    }

    public void testDeleteAccountIsNotGatedOnTheClusterFeature() {
        // Deleting is how an operator resolves a cluster that holds accounts an older node cannot authorize, so
        // unlike creating it stays available while a rolling upgrade is in progress.
        when(featureService.clusterHasFeature(any(), eq(SecurityFeatures.USER_MANAGED_SERVICE_ACCOUNTS))).thenReturn(false);
        respondWithDeleteResult(true);

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(true));
    }

    public void testListAccountsSelectsASingleAccountByPrincipal() {
        respondToSearchWith(List.of(accountDocument(PRINCIPAL, List.of(ROLE_A), true)));

        final List<UserManagedServiceAccount> accounts = listAccounts("engineering", "deploy_bot");
        assertThat(accounts, hasSize(1));
        assertThat(accounts.get(0).id(), equalTo(ACCOUNT_ID));

        assertThat(
            searchedQuery(),
            equalTo(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE))
                    .filter(QueryBuilders.termQuery("username", PRINCIPAL))
            )
        );
    }

    public void testListAccountsSelectsANamespaceByPrefix() {
        respondToSearchWith(
            List.of(accountDocument(PRINCIPAL, List.of(ROLE_A), true), accountDocument("engineering/other_bot", List.of(ROLE_B), false))
        );

        assertThat(listAccounts("engineering", null), hasSize(2));
        assertThat(
            searchedQuery(),
            equalTo(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE))
                    .filter(QueryBuilders.prefixQuery("username", "engineering/"))
            )
        );
    }

    public void testListAccountsSelectsANamespaceWhenExpensiveQueriesAreDisabled() {
        store = newStore(Settings.builder().put(ALLOW_EXPENSIVE_QUERIES.getKey(), false).build());
        // The prefix cannot run, so the search returns every service-account document and the store
        // keeps only the ones in the namespace.
        respondToSearchWith(
            List.of(
                accountDocument(PRINCIPAL, List.of(ROLE_A), true),
                accountDocument("engineering/other_bot", List.of(ROLE_B), false),
                accountDocument("operations/pager-bot", List.of(ROLE_B), true)
            )
        );

        assertThat(listAccounts("engineering", null), hasSize(2));
        assertThat(
            searchedQuery(),
            equalTo(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)))
        );
    }

    public void testListAccountsNarrowsToAServiceNameGivenWithoutANamespace() {
        respondToSearchWith(
            List.of(accountDocument(PRINCIPAL, List.of(ROLE_A), true), accountDocument("operations/other_bot", List.of(ROLE_B), false))
        );

        final List<UserManagedServiceAccount> accounts = listAccounts(null, "deploy_bot");
        assertThat(accounts, hasSize(1));
        assertThat(accounts.get(0).id(), equalTo(ACCOUNT_ID));

        // A leading wildcard would be the only way to express this in the query, so it is applied after parsing.
        assertThat(
            searchedQuery(),
            equalTo(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)))
        );
    }

    public void testListAccountsReturnsAccountsFromEveryNamespace() {
        respondToSearchWith(
            List.of(accountDocument(PRINCIPAL, List.of(ROLE_A), true), accountDocument("operations/pager-bot", List.of(ROLE_B), false))
        );

        final List<UserManagedServiceAccount> accounts = listAccounts(null, null);
        assertThat(accounts, hasSize(2));
        assertThat(accounts.get(0).id(), equalTo(ACCOUNT_ID));
        assertThat(accounts.get(1).id(), equalTo(ServiceAccountId.fromPrincipal("operations/pager-bot")));

        assertThat(
            searchedQuery(),
            equalTo(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)))
        );
    }

    public void testListAccountsFindsNothingWhenTheServiceNameBelongsToAnotherNamespace() {
        // Both set: the query is the exact principal, so operations/pager-bot is not a hit.
        respondToSearchWith(List.of());

        assertThat(listAccounts("engineering", "pager-bot"), empty());
        assertThat(
            searchedQuery(),
            equalTo(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE))
                    .filter(QueryBuilders.termQuery("username", "engineering/pager-bot"))
            )
        );
    }

    public void testListAccountsFindsNothingWhenNoAccountHasTheServiceName() {
        respondToSearchWith(
            List.of(accountDocument(PRINCIPAL, List.of(ROLE_A), true), accountDocument("operations/other_bot", List.of(ROLE_B), false))
        );

        assertThat(listAccounts(null, "pager-bot"), empty());
        assertThat(
            searchedQuery(),
            equalTo(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)))
        );
    }

    public void testAStoredDocumentCannotClaimTheReservedNamespace() {
        // Principals are re-validated on read, so a document written by hand cannot shadow a built-in account.
        respondToSearchWith(List.of(accountDocument(reservedNamespace() + "/fleet-server", List.of(ROLE_A), true)));
        assertThat(listAccounts(null, null), empty());
    }

    public void testQueryAccountsReportsOnePageWithTheTotalAndSortValuesOfTheWholeResult() {
        final Map<String, Object> first = accountDocument(PRINCIPAL, List.of(ROLE_A), true);
        final Map<String, Object> second = accountDocument("engineering/other_bot", List.of(ROLE_B), false);
        // The page holds two hits, but the query matched more than that.
        respondToSearchWith(List.of(first, second), 7, source -> new Object[] { source.get("username") });

        final SearchSourceBuilder searchSource = SearchSourceBuilder.searchSource()
            .query(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE))
            .size(2)
            .sort("username");
        final UserManagedServiceAccountStore.QueryResult result = queryAccounts(searchSource);

        assertThat(result.total(), equalTo(7L));
        assertThat(result.items(), hasSize(2));
        assertThat(result.items().get(0).account().id(), equalTo(ACCOUNT_ID));
        assertThat(result.items().get(0).account().roles(), contains(ROLE_A));
        assertThat(result.items().get(0).account().enabled(), is(true));
        assertThat(result.items().get(0).sortValues(), arrayContaining(PRINCIPAL));
        assertThat(result.items().get(1).account().id(), equalTo(ServiceAccountId.fromPrincipal("engineering/other_bot")));
        assertThat(result.items().get(1).sortValues(), arrayContaining("engineering/other_bot"));

        // The search runs as given: the caller has already shaped it for the security index.
        final SearchRequest searchRequest = onlyRequestOfType(SearchRequest.class);
        assertThat(searchRequest.indices(), arrayContaining(SECURITY_MAIN_ALIAS));
        assertThat(searchRequest.source(), is(searchSource));
    }

    public void testQueryAccountsFindsNothingWhenTheQueryMatchesNothing() {
        respondToSearchWith(List.of(), 0, source -> null);
        final UserManagedServiceAccountStore.QueryResult result = queryAccounts(SearchSourceBuilder.searchSource());
        assertThat(result.items(), empty());
        assertThat(result.total(), equalTo(0L));
    }

    public void testQueryAccountsDropsHitsThatDoNotParse() {
        final Map<String, Object> damaged = accountDocument("engineering/other_bot", List.of(ROLE_B), false);
        damaged.put("roles", ROLE_B);
        final Map<String, Object> reserved = accountDocument(reservedNamespace() + "/fleet-server", List.of(ROLE_A), true);
        respondToSearchWith(List.of(accountDocument(PRINCIPAL, List.of(ROLE_A), true), damaged, reserved), 3, source -> null);

        final UserManagedServiceAccountStore.QueryResult result = queryAccounts(SearchSourceBuilder.searchSource());

        // The total still counts the dropped hits: it is the search's count, not the parse's.
        assertThat(result.total(), equalTo(3L));
        assertThat(result.items(), hasSize(1));
        assertThat(result.items().get(0).account().id(), equalTo(ACCOUNT_ID));
        assertThat(result.items().get(0).sortValues(), emptyArray());
    }

    public void testAccountsAreReadFromTheIndexEveryTimeWhenCachingIsDisabled() {
        store = newStore(Settings.builder().put(UserManagedServiceAccountStore.CACHE_TTL_SETTING.getKey(), TimeValue.ZERO).build());
        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_A), true));

        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_A));
        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_A));
        assertThat(getRequestCount.get(), equalTo(2));
        assertThat(store.getAccountCache(), nullValue());
    }

    public void testListAccountsFindsNothingForIdsNoAccountCouldHold() {
        assertThat(listAccounts(reservedNamespace(), null), empty());
        assertThat(listAccounts("engineering*", null), empty());
        assertThat(listAccounts("engineering", "deploy*"), empty());
        assertThat(requests, empty());
    }

    public void testAnAbsentSecurityIndexHoldsNoAccounts() {
        when(projectIndex.indexExists()).thenReturn(false);

        assertThat(getByPrincipal(PRINCIPAL), nullValue());
        assertThat(listAccounts(null, null), empty());
        assertThat(queryAccounts(SearchSourceBuilder.searchSource()), is(UserManagedServiceAccountStore.QueryResult.EMPTY));

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(false));
        assertThat(requests, empty());
    }

    public void testAnUnavailableSecurityIndexFailsTheRequest() {
        final ElasticsearchException unavailable = new ElasticsearchException("index unavailable");
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(false);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(false);
        when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(unavailable);
        when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(unavailable);

        final PlainActionFuture<UserManagedServiceAccount> get = new PlainActionFuture<>();
        store.getByPrincipal(PRINCIPAL, get);
        assertThat(expectThrows(ElasticsearchException.class, get::actionGet), is(unavailable));

        final PlainActionFuture<List<UserManagedServiceAccount>> list = new PlainActionFuture<>();
        store.listAccounts(null, null, list);
        assertThat(expectThrows(ElasticsearchException.class, list::actionGet), is(unavailable));

        final PlainActionFuture<UserManagedServiceAccountStore.QueryResult> query = new PlainActionFuture<>();
        store.queryAccounts(SearchSourceBuilder.searchSource(), query);
        assertThat(expectThrows(ElasticsearchException.class, query::actionGet), is(unavailable));

        final PlainActionFuture<Boolean> delete = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.NONE, delete);
        assertThat(expectThrows(ElasticsearchException.class, delete::actionGet), is(unavailable));
    }

    private static String reservedNamespace() {
        return randomFrom("elastic", "ELASTIC", "Elastic");
    }

    /**
     * A write still broadcasts a cache clear when this node does not cache: another node may.
     */
    private static Settings randomCacheTtlSettings() {
        if (randomBoolean()) {
            return Settings.EMPTY;
        }
        return Settings.builder()
            .put(
                UserManagedServiceAccountStore.CACHE_TTL_SETTING.getKey(),
                randomBoolean() ? TimeValue.ZERO : TimeValue.timeValueMinutes(randomIntBetween(1, 20))
            )
            .build();
    }

    private UserManagedServiceAccountStore newStore(Settings settings) {
        return new UserManagedServiceAccountStore(
            settings,
            Clock.fixed(NOW, ZoneOffset.UTC),
            client,
            securityIndex,
            clusterService,
            featureService,
            new CacheInvalidatorRegistry()
        );
    }

    private UserManagedServiceAccount getByPrincipal(String principal) {
        final PlainActionFuture<UserManagedServiceAccount> future = new PlainActionFuture<>();
        store.getByPrincipal(principal, future);
        return future.actionGet();
    }

    private List<UserManagedServiceAccount> listAccounts(String namespace, String serviceName) {
        final PlainActionFuture<List<UserManagedServiceAccount>> future = new PlainActionFuture<>();
        store.listAccounts(namespace, serviceName, future);
        return future.actionGet();
    }

    private UserManagedServiceAccountStore.QueryResult queryAccounts(SearchSourceBuilder searchSourceBuilder) {
        final PlainActionFuture<UserManagedServiceAccountStore.QueryResult> future = new PlainActionFuture<>();
        store.queryAccounts(searchSourceBuilder, future);
        return future.actionGet();
    }

    private void respondToGetWith(Map<String, Object> source) {
        responseProvider.set((request, listener) -> {
            try {
                respondToGet(request, source, listener);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
    }

    private static void respondToGet(ActionRequest request, Map<String, Object> source, ActionListener<?> listener) throws IOException {
        assertThat(request, instanceOf(GetRequest.class));
        final GetRequest getRequest = (GetRequest) request;
        assertThat(getRequest.id(), equalTo(DOC_ID));
        final GetResult getResult = new GetResult(
            getRequest.index(),
            getRequest.id(),
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            1L,
            source != null,
            source == null ? null : BytesReference.bytes(XContentFactory.jsonBuilder().map(source)),
            Map.of(),
            Map.of()
        );
        @SuppressWarnings("unchecked")
        final ActionListener<GetResponse> getListener = (ActionListener<GetResponse>) listener;
        getListener.onResponse(new GetResponse(getResult));
    }

    private void respondWithUpdateResult(DocWriteResponse.Result result) {
        responseProvider.set((request, listener) -> {
            if (request instanceof UpdateRequest) {
                listener.onResponse(new UpdateResponse(mock(ShardId.class), DOC_ID, randomLong(), randomLong(), randomLong(), result));
            } else if (recordClearedCache(request, listener) == false) {
                fail("unexpected request " + request);
            }
        });
    }

    private void respondWithDeleteResult(boolean found) {
        responseProvider.set((request, listener) -> {
            if (request instanceof DeleteRequest) {
                listener.onResponse(deleteResponse(found));
            } else if (recordClearedCache(request, listener) == false) {
                fail("unexpected request " + request);
            }
        });
    }

    private static DeleteResponse deleteResponse(boolean found) {
        return new DeleteResponse(mock(ShardId.class), DOC_ID, randomLong(), randomLong(), randomLong(), found);
    }

    private boolean recordClearedCache(ActionRequest request, ActionListener<ActionResponse> listener) {
        if (request instanceof ClearSecurityCacheRequest clearSecurityCacheRequest) {
            assertThat(clearSecurityCacheRequest.cacheName(), equalTo(UserManagedServiceAccountStore.CACHE_NAME));
            clearedCacheKeys.addAll(List.of(clearSecurityCacheRequest.keys()));
            listener.onResponse(new ClearSecurityCacheResponse(mock(ClusterName.class), List.of(), List.of()));
            return true;
        }
        return false;
    }

    private void respondToSearchWith(List<Map<String, Object>> sources) {
        respondToSearchWith(sources, sources.size(), source -> null);
    }

    /**
     * Answers the next search with the given page. {@code total} may exceed the page, as it does for a paginated
     * query, and {@code sortValues} supplies each hit's sort values, as a sorted query would.
     */
    private void respondToSearchWith(List<Map<String, Object>> sources, long total, Function<Map<String, Object>, Object[]> sortValues) {
        responseProvider.set((request, listener) -> {
            if (request instanceof SearchRequest) {
                ActionListener.respondAndRelease(listener, searchResponse(sources, total, sortValues));
            } else if (request instanceof SearchScrollRequest) {
                // Reached only when a hit did not parse, since the scroll runs until as many results as hits
                // have been collected. An empty page ends it.
                ActionListener.respondAndRelease(listener, searchResponse(List.of()));
            } else if (request instanceof ClearScrollRequest) {
                listener.onResponse(new ClearScrollResponse(true, 1));
            } else {
                fail("unexpected request " + request);
            }
        });
    }

    private static SearchResponse searchResponse(List<Map<String, Object>> sources) {
        return searchResponse(sources, sources.size(), source -> null);
    }

    private static SearchResponse searchResponse(
        List<Map<String, Object>> sources,
        long total,
        Function<Map<String, Object>, Object[]> sortValues
    ) {
        final SearchHit[] hits = new SearchHit[sources.size()];
        for (int i = 0; i < hits.length; i++) {
            final Map<String, Object> source = sources.get(i);
            hits[i] = SearchHit.unpooled(i, SERVICE_ACCOUNT_DOC_TYPE + "-" + source.get("username"));
            try {
                hits[i].sourceRef(BytesReference.bytes(XContentFactory.jsonBuilder().map(source)));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            final Object[] hitSortValues = sortValues.apply(source);
            if (hitSortValues != null) {
                hits[i].sortValues(hitSortValues, new DocValueFormat[] { DocValueFormat.RAW });
            }
        }
        final SearchHits searchHits = new SearchHits(hits, new TotalHits(total, TotalHits.Relation.EQUAL_TO), 0f);
        try {
            return SearchResponseUtils.successfulResponse(searchHits);
        } finally {
            searchHits.decRef();
        }
    }

    private UpdateRequest updateRequest() {
        return onlyRequestOfType(UpdateRequest.class);
    }

    /**
     * The document written when the account does not exist yet.
     */
    private Map<String, Object> upsertDocument() {
        return updateRequest().upsertRequest().sourceAsMap();
    }

    /**
     * The changes merged into the document when the account already exists.
     */
    private Map<String, Object> changesDocument() {
        return updateRequest().doc().sourceAsMap();
    }

    private static ServiceAccountAuthor randomAuthor() {
        return new ServiceAccountAuthor(
            randomAlphaOfLengthBetween(3, 8),
            randomBoolean() ? null : randomAlphaOfLengthBetween(3, 12),
            randomBoolean() ? null : randomAlphaOfLengthBetween(3, 12),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomBoolean() ? null : AuthenticationTestHelper.randomDomain(randomBoolean()),
            randomBoolean()
                ? null
                : new ServiceAccountAuthor.ApiKey(randomAlphaOfLength(20), randomBoolean() ? null : randomAlphaOfLength(8))
        );
    }

    /**
     * The author as the store writes it: every field present, absent ones as nulls, and the realm domain whole.
     */
    private static Map<String, Object> storedAuthor(ServiceAccountAuthor author) {
        final Map<String, Object> stored = new HashMap<>();
        stored.put("principal", author.principal());
        stored.put("full_name", author.fullName());
        stored.put("email", author.email());
        stored.put("realm", author.realm());
        stored.put("realm_type", author.realmType());
        stored.put("realm_domain", author.realmDomain() == null ? null : storedRealmDomain(author.realmDomain()));
        stored.put("api_key", author.apiKey() == null ? null : storedApiKey(author.apiKey()));
        return stored;
    }

    private static Map<String, Object> storedApiKey(ServiceAccountAuthor.ApiKey apiKey) {
        final Map<String, Object> stored = new HashMap<>();
        stored.put("id", apiKey.id());
        stored.put("name", apiKey.name());
        return stored;
    }

    private static Map<String, Object> storedRealmDomain(RealmDomain realmDomain) {
        try {
            return XContentHelper.convertToMap(
                BytesReference.bytes(realmDomain.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)),
                false
            ).v2();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private QueryBuilder searchedQuery() {
        return onlyRequestOfType(SearchRequest.class).source().query();
    }

    private <T extends ActionRequest> T onlyRequestOfType(Class<T> requestClass) {
        final List<T> matching = requests.stream().filter(requestClass::isInstance).map(requestClass::cast).toList();
        assertThat(matching, hasSize(1));
        return matching.get(0);
    }

    private static Map<String, Object> accountDocument(String principal, List<String> roles, boolean enabled) {
        return accountDocument(principal, roles, enabled, null);
    }

    private static Map<String, Object> accountDocument(
        String principal,
        List<String> roles,
        boolean enabled,
        @Nullable String description
    ) {
        final Map<String, Object> source = new HashMap<>();
        source.put("doc_type", SERVICE_ACCOUNT_DOC_TYPE);
        source.put("version", UserManagedServiceAccount.Version.CURRENT.id());
        source.put("username", principal);
        source.put("roles", roles);
        source.put("enabled", enabled);
        if (description != null) {
            source.put("description", description);
        }
        return source;
    }

    private static String randomDescription() {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
