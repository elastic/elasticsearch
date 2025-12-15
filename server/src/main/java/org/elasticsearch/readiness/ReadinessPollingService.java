/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.readiness;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;

class ReadinessPollingService {
    public static final Setting<Integer> TIMEOUT = Setting.intSetting("readiness.polling.timeout", 60_000, NodeScope);
    public static final Setting<Integer> INITIAL_DELAY = Setting.intSetting("readiness.polling.initial_delay", 1_000, NodeScope);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final long timeoutMillis;
    private final long initialDelayMillis;

    ReadinessPollingService(
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        long timeoutMillis,
        long initialDelayMillis
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.timeoutMillis = timeoutMillis;
        this.initialDelayMillis = initialDelayMillis;
    }

    @Inject
    ReadinessPollingService(
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        Environment environment
    ) {
        this(
            clusterService,
            transportService,
            threadPool,
            TIMEOUT.get(environment.settings()),
            INITIAL_DELAY.get(environment.settings())
        );
    }

    /**
     * Asynchronously polls the indicated nodes until at least one reports readiness or the timeout expires.
     * Uses exponential backoff.
     * Failures are considered equivalent to "not ready", since this is used when a cluster is in flux.
     * <p>
     * Returns immediately once the polling loop has been initiated.
     *
     * @param nodeFilter predicate to select which nodes to poll
     * @param listener invoked with true if any node reports ready, false if timeout expires
     */
    public void execute(
        Predicate<DiscoveryNode> nodeFilter,
        ActionListener<Boolean> listener
    ) {
        final long deadline = System.currentTimeMillis() + timeoutMillis;

        Runnable attempt = new Runnable() {
            int attemptCount = 0;

            @Override
            public void run() {
                Set<DiscoveryNode> nodes = clusterService.state().nodes().stream().filter(nodeFilter).collect(Collectors.toSet());

                if (nodes.isEmpty()) {
                    listener.onResponse(false);
                    return;
                }

                // We want to respond at most once
                AtomicBoolean responded = new AtomicBoolean(false);

                for (DiscoveryNode node : nodes) {
                    transportService.sendRequest(
                        node,
                        TransportReadinessAction.TYPE.name(),
                        new ReadinessRequest(),
                        new TransportResponseHandler<ReadinessResponse>() {
                            @Override
                            public ReadinessResponse read(StreamInput in) throws IOException {
                                return ReadinessResponse.readFrom(in);
                            }

                            @Override
                            public void handleResponse(ReadinessResponse response) {
                                // If !response.isReady() then we ignore this one hoping for another node to be ready.
                                logger.debug("node [{}] reports readiness [{}]", node, response.isReady());
                                if (response.isReady() && responded.compareAndSet(false, true)) {
                                    logger.debug("readiness poll responding with true");
                                    listener.onResponse(true);
                                }
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                // Failures are equivalent to "not ready". The retry loop will handle this case.
                                logger.debug("failed to determine readiness of node [{}]", node, exp);
                            }

                            @Override
                            public Executor executor() {
                                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
                            }
                        }
                    );
                }

                attemptCount++;
                long delay = initialDelayMillis * (1L << (attemptCount - 1));
                delay = Math.min(delay, deadline - System.currentTimeMillis());
                if (delay > 0) {
                    threadPool.schedule(() -> {
                        if (responded.get() == false) {
                            // Retry
                            this.run();
                        }
                    }, TimeValue.timeValueMillis(delay), EsExecutors.DIRECT_EXECUTOR_SERVICE);
                } else if (responded.compareAndSet(false, true)) {
                    logger.debug("readiness poll responding with false");
                    listener.onResponse(false);
                }
            }
        };

        threadPool.generic().execute(attempt);
    }

    private static final Logger logger = LogManager.getLogger(ReadinessPollingService.class);
}
