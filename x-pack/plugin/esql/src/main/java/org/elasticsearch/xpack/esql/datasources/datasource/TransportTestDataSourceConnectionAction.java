/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasources.TestConnectionResult;
import org.elasticsearch.xpack.esql.datasources.UnknownDataSourceTypeException;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.NodeEligibilityStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinator for {@link TestDataSourceConnectionAction}.
 *
 * <ol>
 *   <li>Validates the data source type against the PUT registry — unknown types are rejected with
 *       HTTP 400.</li>
 *   <li>Validates the settings structure via {@link DataSourceValidator#validateDatasource} —
 *       malformed settings are rejected with HTTP 400 rather than returning a soft {@code failure}.</li>
 *   <li>Fans out a {@link TestDataSourceNodeAction} request to every eligible node (data-capable,
 *       non-index-role nodes) with a per-node timeout of {@link #PROBE_TIMEOUT}.</li>
 *   <li>Aggregates per-node {@link TestConnectionResult} values according to the aggregate rule:
 *       {@code failure} beats {@code untestable} beats {@code success}. A mix of {@code success}
 *       and {@code untestable} resolves to {@code untestable} — the result is inconclusive.</li>
 * </ol>
 *
 * <p><b>Two-registry note.</b> The coordinator gates on the PUT validator registry
 * ({@code DataSourceService.validatorFor()}). Each node resolves the probe factory through
 * {@code DataSourceModule.testConnection}, which uses its own lookup chain
 * ({@code sourceFactories}, then {@code storageProviderRegistry}, then {@code testConnectionSchemes}).
 * If a per-node system property (e.g. a GCS plugin flag) causes the two to diverge, the node
 * throws an {@link IllegalArgumentException} which surfaces as a {@code TransportException} at the
 * coordinator and is mapped to {@code untestable} — inconclusive rather than a false failure.
 * Aligning both registries behind a single source of truth is a follow-up.
 *
 * <p><b>Concurrency note.</b> Each node probe opens a storage client and blocks a GENERIC-pool
 * thread until the probe completes or times out. In a large cluster this means one GENERIC thread
 * per data node for up to {@link #PROBE_TIMEOUT} per call. Cancellation of the REST request does
 * not propagate to node requests. A per-call concurrency limit is a follow-up.
 */
public class TransportTestDataSourceConnectionAction extends HandledTransportAction<
    TestDataSourceConnectionAction.Request,
    TestDataSourceConnectionAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportTestDataSourceConnectionAction.class);

    /** Per-node probe deadline. Not configurable — a settings-driven timeout adds complexity with little benefit. */
    static final TimeValue PROBE_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final DataSourceService dataSourceService;

    @Inject
    public TransportTestDataSourceConnectionAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService,
        DataSourceService dataSourceService
    ) {
        super(TestDataSourceConnectionAction.NAME, transportService, actionFilters, in -> {
            throw new UnsupportedOperationException("action [" + TestDataSourceConnectionAction.NAME + "] is local-only");
        }, threadPool.executor(ThreadPool.Names.GENERIC));
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.dataSourceService = dataSourceService;
    }

    @Override
    protected void doExecute(
        Task task,
        TestDataSourceConnectionAction.Request request,
        ActionListener<TestDataSourceConnectionAction.Response> listener
    ) {
        // --- Step 1: validate type against PUT registry (unknown type → 400) ---
        DataSourceValidator validator = dataSourceService.validatorFor(request.type());
        if (validator == null) {
            listener.onFailure(
                new ElasticsearchStatusException("Unknown data source type [" + request.type() + "]", RestStatus.BAD_REQUEST)
            );
            return;
        }

        // --- Step 2: validate settings structure (bad settings → 400, not soft failure) ---
        try {
            validator.validateDatasource(request.rawSettings());
        } catch (UnknownDataSourceTypeException e) {
            listener.onFailure(new ElasticsearchStatusException(e.getMessage(), RestStatus.BAD_REQUEST, e));
            return;
        } catch (Exception e) {
            listener.onFailure(new ElasticsearchStatusException(e.getMessage(), RestStatus.BAD_REQUEST, e));
            return;
        }

        // --- Step 3: select eligible nodes and fan out ---
        DiscoveryNodes allNodes = clusterService.state().nodes();
        List<DiscoveryNode> dataNodes = NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(allNodes);
        // Always include the coordinating node: schema resolution and dataset listing run there, so
        // egress blocked on the coordinator causes query failures even when all data nodes report success.
        DiscoveryNode localNode = allNodes.getLocalNode();
        List<DiscoveryNode> eligibleNodes;
        if (dataNodes.contains(localNode)) {
            eligibleNodes = dataNodes;
        } else {
            eligibleNodes = new ArrayList<>(dataNodes.size() + 1);
            eligibleNodes.addAll(dataNodes);
            eligibleNodes.add(localNode);
        }

        final int total = eligibleNodes.size();
        final AtomicArray<TestConnectionResult> results = new AtomicArray<>(total);
        final AtomicInteger remaining = new AtomicInteger(total);

        // Strip datasource-level region before probing. Queries drop it via DatasetRewriter (region
        // is a dataset-level setting); the probe must behave consistently to avoid signing with a
        // region that queries never use.
        Map<String, Object> probeSettings = new HashMap<>(request.rawSettings());
        probeSettings.remove("region");

        TestDataSourceNodeAction.NodeRequest nodeRequest = new TestDataSourceNodeAction.NodeRequest(request.type(), probeSettings);
        TransportRequestOptions options = TransportRequestOptions.timeout(PROBE_TIMEOUT);

        for (int i = 0; i < eligibleNodes.size(); i++) {
            final int idx = i;
            final DiscoveryNode node = eligibleNodes.get(i);
            transportService.sendRequest(
                node,
                TestDataSourceNodeAction.NAME,
                nodeRequest,
                options,
                new TransportResponseHandler<TestDataSourceNodeAction.NodeResponse>() {
                    @Override
                    public TestDataSourceNodeAction.NodeResponse read(org.elasticsearch.common.io.stream.StreamInput in)
                        throws IOException {
                        return new TestDataSourceNodeAction.NodeResponse(in);
                    }

                    @Override
                    public Executor executor() {
                        return TransportResponseHandler.TRANSPORT_WORKER;
                    }

                    @Override
                    public void handleResponse(TestDataSourceNodeAction.NodeResponse response) {
                        results.set(idx, response.result);
                        if (remaining.decrementAndGet() == 0) {
                            listener.onResponse(aggregate(results));
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        // Transport exceptions (no-handler on old nodes during rolling upgrade, timeout,
                        // or registry inconsistency) are inconclusive — the probe did not run.
                        // Map to untestable with no user-visible message: cause messages contain
                        // internal strings (action names, node addresses, registry IAE text) that
                        // must not be surfaced in a public response. The untestable message field
                        // is for user-visible guidance (e.g. "create a dataset"), not debug info.
                        logger.debug("test-connection probe on node [{}] did not complete: {}", node, exp.getMessage());
                        results.set(
                            idx,
                            new TestConnectionResult.Untestable(
                                "One or more nodes returned no probe result; the backend may still be reachable"
                            )
                        );
                        if (remaining.decrementAndGet() == 0) {
                            listener.onResponse(aggregate(results));
                        }
                    }
                }
            );
        }
    }

    /**
     * Aggregate rule:
     * <ul>
     *   <li>any {@code failure} → {@code failure} (takes first failure message)</li>
     *   <li>any {@code untestable} (and no failure) → {@code untestable} (takes first reason)</li>
     *   <li>all {@code success} → {@code success}</li>
     * </ul>
     * This means {@code success + untestable → untestable}: the result is inconclusive when any
     * node could not run the probe. Only unanimous success guarantees reachability from every node.
     */
    static TestDataSourceConnectionAction.Response aggregate(AtomicArray<TestConnectionResult> results) {
        String firstFailure = null;
        String firstUntestableReason = null;
        boolean anyFailure = false;
        boolean anyUntestable = false;

        for (TestConnectionResult r : results.asList()) {
            switch (r) {
                case TestConnectionResult.Success ignored -> {
                }
                case TestConnectionResult.Failure f -> {
                    anyFailure = true;
                    if (firstFailure == null) firstFailure = f.error();
                }
                case TestConnectionResult.Untestable u -> {
                    anyUntestable = true;
                    if (firstUntestableReason == null) firstUntestableReason = u.reason();
                }
            }
        }

        if (anyFailure) {
            return TestDataSourceConnectionAction.Response.failure(firstFailure);
        }
        if (anyUntestable) {
            return TestDataSourceConnectionAction.Response.untestable(firstUntestableReason);
        }
        return TestDataSourceConnectionAction.Response.success();
    }
}
