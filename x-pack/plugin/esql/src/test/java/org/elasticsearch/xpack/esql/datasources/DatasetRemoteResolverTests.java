/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteClusterSettings;
import org.elasticsearch.transport.RemoteClusterSettings.ProxyConnectionStrategySettings;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

/**
 * Unit-pins the BWC + technical-preview fail-closed branches of
 * {@link DatasetRemoteResolver#anyLinkedProjectHasDataset}. These branches carry the cross-project (CPS) safety
 * guarantees — the per-project transport-version gate that keeps a coordinator from dispatching the remote resolve to
 * an older linked project, and the fail-closed propagation of a per-project transport error — and are exercised nowhere
 * else.
 *
 * <p>Built on real {@link MockTransportService}s rather than mocks, the way {@code EnrichPolicyResolverTests} is. A
 * local node hosts the {@link DatasetRemoteResolver} under test; each "linked project" is a real remote node registered
 * via {@link RemoteClusterService#updateRemoteCluster}, so the resolver's
 * {@code getRemoteClusterService().maybeEnsureConnectedAndGetConnection(alias, true, ...)} resolves a real
 * {@link org.elasticsearch.transport.Transport.Connection}. {@link RemoteClusterService} is {@code final} (not
 * mockable), and the connection's {@code getTransportVersion()} is negotiated during the real handshake — which is
 * exactly what the version-gate branch reads, so the gate is exercised by setting the remote node's transport version.
 * Each remote registers a handler for {@link EsqlDatasetActionNames#ESQL_RESOLVE_DATASET_REMOTE_ACTION_NAME} that
 * returns a scripted {@link EsqlResolveDatasetAction.Response} (or throws), letting each scenario steer
 * detection / emptiness / failure. The resolver under test is built with {@code registerHandler=false}: it only
 * dispatches here, never receives.
 */
public class DatasetRemoteResolverTests extends ESTestCase {

    private static final String[] PATTERNS = { "logs-*" };

    /** A transport version below the {@code esql_resolve_dataset_remote} wire contract — the "does not support" leg. */
    private static final TransportVersion OLD_VERSION = TransportVersion.fromName("cohere_bit_embedding_type_support_added");

    private TestThreadPool threadPool;
    private MockTransportService localTransport;
    private final List<MockTransportService> remotes = new ArrayList<>();
    private DatasetRemoteResolver resolver;

    @Before
    public void setUpResolver() {
        // Guard the premise of the version-gate tests: OLD_VERSION must genuinely fail the contract check, else the skip
        // branch would never be reached and the test would silently pass for the wrong reason.
        assertThat(OLD_VERSION.supports(EsqlResolveDatasetAction.ESQL_RESOLVE_DATASET_REMOTE), equalTo(false));
        assertThat(TransportVersion.current().supports(EsqlResolveDatasetAction.ESQL_RESOLVE_DATASET_REMOTE), equalTo(true));

        threadPool = new TestThreadPool(getTestName());
        localTransport = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        localTransport.start();
        localTransport.acceptIncomingRequests();

        resolver = new DatasetRemoteResolver(
            localTransport,
            mock(ClusterService.class),
            mock(ProjectResolver.class),
            mock(IndexNameExpressionResolver.class),
            false // do not register the node-to-node handler: this test only dispatches
        );
    }

    @After
    public void stopTransports() {
        remotes.forEach(MockTransportService::stop);
        if (localTransport != null) {
            localTransport.stop();
        }
        terminate(threadPool);
    }

    /** Empty alias set short-circuits to {@code false} without resolving any connection. */
    public void testEmptyAliasesShortCircuitsToFalse() {
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        resolver.anyLinkedProjectHasDataset(PATTERNS, List.of(), future);
        assertThat(future.actionGet(10, TimeUnit.SECONDS), equalTo(false));
    }

    /** A linked project below the wire contract is skipped (no dispatch); as the sole project, the result is {@code false}. */
    public void testVersionGateSkipsOldProject() {
        // The handler would flag detection if (wrongly) dispatched to — proving the gate skips before sending.
        registerRemote("old", OLD_VERSION, request -> new EsqlResolveDatasetAction.Response(Set.of("logs-prod"), false, Set.of()));

        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        resolver.anyLinkedProjectHasDataset(PATTERNS, List.of("old"), future);
        assertThat(future.actionGet(10, TimeUnit.SECONDS), equalTo(false));
    }

    /** A supporting-version project returning a non-empty dataset set drives the result to {@code true}. */
    public void testPositiveDetection() {
        registerRemote(
            "p1",
            TransportVersion.current(),
            request -> new EsqlResolveDatasetAction.Response(Set.of("logs-prod"), false, Set.of())
        );

        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        resolver.anyLinkedProjectHasDataset(PATTERNS, List.of("p1"), future);
        assertThat(future.actionGet(10, TimeUnit.SECONDS), equalTo(true));
    }

    /** A supporting-version project returning an empty dataset set yields {@code false}. */
    public void testNegativeEmptyResponse() {
        registerRemote("p1", TransportVersion.current(), request -> new EsqlResolveDatasetAction.Response(Set.of(), false, Set.of()));

        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        resolver.anyLinkedProjectHasDataset(PATTERNS, List.of("p1"), future);
        assertThat(future.actionGet(10, TimeUnit.SECONDS), equalTo(false));
    }

    /** Fail-closed: a per-project dispatch failure propagates to {@code onFailure}, not a swallow to {@code false}. */
    public void testFailClosedOnRemoteException() {
        registerRemoteThatThrows("p1", TransportVersion.current());

        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        resolver.anyLinkedProjectHasDataset(PATTERNS, List.of("p1"), future);

        Exception thrown = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(rootCauseMessage(thrown), equalTo("simulated remote failure"));
    }

    /** Mixed: one project below the contract (skipped) + one supporting project that detects → {@code true}. */
    public void testMixedSkippedAndDetecting() {
        registerRemote("old", OLD_VERSION, request -> new EsqlResolveDatasetAction.Response(Set.of(), false, Set.of()));
        registerRemote(
            "new",
            TransportVersion.current(),
            request -> new EsqlResolveDatasetAction.Response(Set.of("logs-prod"), false, Set.of())
        );

        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        resolver.anyLinkedProjectHasDataset(PATTERNS, List.of("old", "new"), future);
        assertThat(future.actionGet(10, TimeUnit.SECONDS), equalTo(true));
    }

    // ---- real remote-cluster wiring ----

    /** Stand up a remote node on {@code transportVersion}, register a resolve handler answering with {@code responder}, and connect it. */
    private void registerRemote(
        String alias,
        TransportVersion transportVersion,
        Function<DatasetRemoteResolver.RemoteRequest, EsqlResolveDatasetAction.Response> responder
    ) {
        MockTransportService remote = startRemote(alias, transportVersion);
        remote.registerRequestHandler(
            EsqlDatasetActionNames.ESQL_RESOLVE_DATASET_REMOTE_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            DatasetRemoteResolver.RemoteRequest::new,
            (request, channel, task) -> channel.sendResponse(responder.apply(request))
        );
        connect(alias, remote);
    }

    /** As {@link #registerRemote} but the remote handler answers with an exception, exercising the fail-closed dispatch leg. */
    private void registerRemoteThatThrows(String alias, TransportVersion transportVersion) {
        MockTransportService remote = startRemote(alias, transportVersion);
        remote.registerRequestHandler(
            EsqlDatasetActionNames.ESQL_RESOLVE_DATASET_REMOTE_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            DatasetRemoteResolver.RemoteRequest::new,
            (request, channel, task) -> channel.sendResponse(new TransportException("simulated remote failure"))
        );
        connect(alias, remote);
    }

    private MockTransportService startRemote(String alias, TransportVersion transportVersion) {
        MockTransportService remote = MockTransportService.createNewService(
            Settings.builder().put("node.name", alias).build(),
            VersionInformation.CURRENT,
            transportVersion,
            threadPool
        );
        remote.start();
        remote.acceptIncomingRequests();
        remotes.add(remote); // stopped in @After
        return remote;
    }

    /**
     * Connect the local node to {@code remote} under {@code alias} using PROXY mode: the proxy strategy opens a direct
     * connection to the remote's address without a sniff round (which would require the remote to answer
     * {@code cluster:monitor/state} — handlers this test deliberately does not stand up).
     */
    private void connect(String alias, MockTransportService remote) {
        DiscoveryNode remoteNode = remote.getLocalNode();
        Settings proxySettings = Settings.builder()
            .put(RemoteClusterSettings.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(alias).getKey(), "proxy")
            .put(
                ProxyConnectionStrategySettings.PROXY_ADDRESS.getConcreteSettingForNamespace(alias).getKey(),
                remoteNode.getAddress().toString()
            )
            .build();
        PlainActionFuture<RemoteClusterService.RemoteClusterConnectionStatus> future = new PlainActionFuture<>();
        localTransport.getRemoteClusterService()
            .updateRemoteCluster(RemoteClusterSettings.toConfig(ProjectId.DEFAULT, ProjectId.DEFAULT, alias, proxySettings), false, future);
        future.actionGet(10, TimeUnit.SECONDS);
    }

    /** Walk to the deepest cause so the assertion pins the original simulated failure message, not a transport wrapper. */
    private static String rootCauseMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }
}
