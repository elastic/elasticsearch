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
import java.util.List;
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
 */
public class TransportTestDataSourceConnectionAction extends HandledTransportAction<
    TestDataSourceConnectionAction.Request,
    TestDataSourceConnectionAction.Response> {

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
        List<DiscoveryNode> eligibleNodes = NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(allNodes);
        if (eligibleNodes.isEmpty()) {
            // No eligible remote nodes — run the probe on the local (coordinator) node.
            eligibleNodes = List.of(allNodes.getLocalNode());
        }

        final int total = eligibleNodes.size();
        final AtomicArray<TestConnectionResult> results = new AtomicArray<>(total);
        final AtomicInteger remaining = new AtomicInteger(total);

        TestDataSourceNodeAction.NodeRequest nodeRequest = new TestDataSourceNodeAction.NodeRequest(request.type(), request.rawSettings());
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
                        String msg = exp.getMessage() != null ? exp.getMessage() : exp.getClass().getName();
                        results.set(idx, TestConnectionResult.failure(msg));
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
    private static TestDataSourceConnectionAction.Response aggregate(AtomicArray<TestConnectionResult> results) {
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
