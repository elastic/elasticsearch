/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ManagedServiceAccountStoreTests extends ESTestCase {

    private static final String PRINCIPAL = "yaml_poc/worker_1";
    private static final String ROLE_A = "managed_store_role_a";
    private static final String ROLE_B = "managed_store_role_b";

    private Client client;
    private ClusterService clusterService;
    private SecurityIndexManager securityIndex;
    private final AtomicReference<ProjectId> activeProject = new AtomicReference<>();
    private final AtomicInteger getRequestCount = new AtomicInteger();
    private final AtomicReference<BiConsumer<ActionRequest, ActionListener<ActionResponse>>> responseProvider = new AtomicReference<>();
    private ManagedServiceAccountStore store;

    @Before
    public void init() {
        activeProject.set(randomUniqueProjectId());
        Client mockClient = mock(Client.class);
        when(mockClient.settings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
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
                if (request instanceof GetRequest) {
                    getRequestCount.incrementAndGet();
                }
                responseProvider.get().accept(request, (ActionListener<ActionResponse>) listener);
            }
        };

        clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.getMinNodeVersion()).thenReturn(org.elasticsearch.Version.CURRENT);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(clusterState.getMinTransportVersion()).thenReturn(org.elasticsearch.TransportVersion.current());
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test"));

        securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.forCurrentProject()).thenAnswer(invocation -> indexState(activeProject.get()));

        final Settings settings = Settings.builder()
            .put(ManagedServiceAccountStore.CACHE_TTL_SETTING.getKey(), "20m")
            .put(ManagedServiceAccountStore.CACHE_MAX_ACCOUNTS_SETTING.getKey(), 1000)
            .build();
        store = new ManagedServiceAccountStore(settings, client, securityIndex, clusterService, new CacheInvalidatorRegistry());
    }

    public void testGetByPrincipalCachesAccount() throws Exception {
        responseProvider.set((request, listener) -> {
            try {
                respondToGet(request, Map.of(activeProject.get(), validAccountDocument(PRINCIPAL, List.of(ROLE_A), true)), listener);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });

        assertThat(getAccountRoles(), equalTo(List.of(ROLE_A)));
        assertThat(getRequestCount.get(), equalTo(1));

        assertThat(getAccountRoles(), equalTo(List.of(ROLE_A)));
        assertThat(getRequestCount.get(), equalTo(1));
    }

    public void testCacheDoesNotRestoreAccountAfterInvalidationDuringLoad() throws Exception {
        final CountDownLatch releaseGet = new CountDownLatch(1);
        responseProvider.set((request, listener) -> {
            try {
                releaseGet.await();
                respondToGet(request, Map.of(activeProject.get(), validAccountDocument(PRINCIPAL, List.of(ROLE_A), true)), listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });

        final PlainActionFuture<ManagedServiceAccount> firstLoad = new PlainActionFuture<>();
        final Thread loadingThread = new Thread(() -> store.getByPrincipal(PRINCIPAL, firstLoad));
        loadingThread.start();
        assertBusy(() -> assertThat(getRequestCount.get(), equalTo(1)));

        store.invalidate(List.of(PRINCIPAL));
        releaseGet.countDown();
        loadingThread.join();

        assertThat(firstLoad.get().roles(), equalTo(List.of(ROLE_A)));
        assertNull(store.getAccountCache().get(PRINCIPAL));

        responseProvider.set((request, listener) -> {
            try {
                respondToGet(request, Map.of(activeProject.get(), validAccountDocument(PRINCIPAL, List.of(ROLE_B), true)), listener);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
        assertThat(getAccountRoles(), equalTo(List.of(ROLE_B)));
        assertThat(getRequestCount.get(), equalTo(2));
    }

    public void testParseAccountDocumentRejectsMissingEnabled() throws Exception {
        final Map<String, Object> source = validAccountDocument(PRINCIPAL, List.of(ROLE_A), true);
        source.remove("enabled");
        assertThat(loadAccountFromSource(PRINCIPAL, source), nullValue());
    }

    public void testParseAccountDocumentRejectsWrongUsername() throws Exception {
        final Map<String, Object> source = validAccountDocument("other/service", List.of(ROLE_A), true);
        assertThat(loadAccountFromSource(PRINCIPAL, source), nullValue());
    }

    public void testParseAccountDocumentRejectsInvalidRoleType() throws Exception {
        final Map<String, Object> source = validAccountDocument(PRINCIPAL, List.of(ROLE_A), true);
        source.put("roles", List.of(ROLE_A, 42));
        assertThat(loadAccountFromSource(PRINCIPAL, source), nullValue());
    }

    public void testParseAccountDocumentRejectsMissingRolesField() throws Exception {
        final Map<String, Object> source = validAccountDocument(PRINCIPAL, List.of(ROLE_A), true);
        source.remove("roles");
        assertThat(loadAccountFromSource(PRINCIPAL, source), nullValue());
    }

    public void testGetByPrincipalRejectsElasticNamespace() {
        PlainActionFuture<ManagedServiceAccount> future = new PlainActionFuture<>();
        store.getByPrincipal("elastic/worker", future);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(exception.getMessage(), equalTo("the [elastic] namespace is reserved for built-in service accounts"));
    }

    public void testListAccountsReturnsEmptyForInvalidServiceName() throws Exception {
        PlainActionFuture<List<ManagedServiceAccount>> future = new PlainActionFuture<>();
        store.listAccounts("yaml_poc", "worker*", future);
        assertThat(future.get(), empty());
    }

    public void testListAccountsReturnsEmptyForInvalidNamespace() throws Exception {
        PlainActionFuture<List<ManagedServiceAccount>> future = new PlainActionFuture<>();
        store.listAccounts("yaml*", null, future);
        assertThat(future.get(), empty());
    }

    public void testListAccountsReturnsEmptyForElasticNamespace() throws Exception {
        PlainActionFuture<List<ManagedServiceAccount>> future = new PlainActionFuture<>();
        store.listAccounts("elastic", null, future);
        assertThat(future.get(), empty());
    }

    public void testParseAccountDocumentRejectsWrongDocType() throws Exception {
        final Map<String, Object> source = validAccountDocument(PRINCIPAL, List.of(ROLE_A), true);
        source.put("doc_type", "user");
        assertThat(loadAccountFromSource(PRINCIPAL, source), nullValue());
    }

    private List<String> getAccountRoles() throws Exception {
        PlainActionFuture<ManagedServiceAccount> future = new PlainActionFuture<>();
        store.getByPrincipal(PRINCIPAL, future);
        ManagedServiceAccount account = future.get();
        assertNotNull(account);
        return account.roles();
    }

    private ManagedServiceAccount loadAccountFromSource(String expectedPrincipal, Map<String, Object> source) throws Exception {
        responseProvider.set((request, listener) -> {
            try {
                respondToGet(request, Map.of(activeProject.get(), source), listener);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
        PlainActionFuture<ManagedServiceAccount> future = new PlainActionFuture<>();
        store.getByPrincipal(expectedPrincipal, future);
        return future.get();
    }

    private void respondToGet(ActionRequest request, Map<ProjectId, Map<String, Object>> docsByProject, ActionListener<?> listener)
        throws IOException {
        assertTrue(request instanceof GetRequest);
        GetRequest getRequest = (GetRequest) request;
        final Map<String, Object> source = docsByProject.get(activeProject.get());
        final GetResult getResult = new GetResult(
            getRequest.index(),
            getRequest.id(),
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            1L,
            source != null,
            source == null ? null : BytesReference.bytes(XContentFactory.jsonBuilder().map(source)),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> getListener = (ActionListener<GetResponse>) listener;
        getListener.onResponse(new GetResponse(getResult));
    }

    private SecurityIndexManager.IndexState indexState(ProjectId projectId) {
        return securityIndex.new IndexState(
            projectId, SecurityIndexManager.ProjectStatus.PROJECT_AVAILABLE, Instant.now(), true, true, true, true, true, null, false, null,
            null, null, ".security-7", ClusterHealthStatus.GREEN, IndexMetadata.State.OPEN, "uuid", Set.of()
        );
    }

    private static Map<String, Object> validAccountDocument(String principal, List<String> roles, boolean enabled) {
        final Map<String, Object> source = new HashMap<>();
        source.put("doc_type", ManagedServiceAccountStore.SERVICE_ACCOUNT_DOC_TYPE);
        source.put("username", principal);
        source.put("roles", roles);
        source.put("enabled", enabled);
        return source;
    }
}
