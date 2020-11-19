/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckAction;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;

public class TransportDeprecationInfoAction extends TransportMasterNodeReadAction<DeprecationInfoAction.Request,
        DeprecationInfoAction.Response> {
    private static final List<DeprecationChecker> PLUGIN_CHECKERS = List.of(new MlDeprecationChecker());
    private static final Logger logger = LogManager.getLogger(TransportDeprecationInfoAction.class);

    private final XPackLicenseState licenseState;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings settings;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportDeprecationInfoAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                          XPackLicenseState licenseState, NodeClient client, NamedXContentRegistry xContentRegistry) {
        super(DeprecationInfoAction.NAME, transportService, clusterService, threadPool, actionFilters, DeprecationInfoAction.Request::new,
                indexNameExpressionResolver, DeprecationInfoAction.Response::new, ThreadPool.Names.GENERIC);
        this.licenseState = licenseState;
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settings = settings;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected ClusterBlockException checkBlock(DeprecationInfoAction.Request request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected final void masterOperation(Task task, final DeprecationInfoAction.Request request, ClusterState state,
                                         final ActionListener<DeprecationInfoAction.Response> listener) {
        if (licenseState.checkFeature(XPackLicenseState.Feature.DEPRECATION)) {

            NodesDeprecationCheckRequest nodeDepReq = new NodesDeprecationCheckRequest("_all");
            ClientHelper.executeAsyncWithOrigin(client, ClientHelper.DEPRECATION_ORIGIN,
                NodesDeprecationCheckAction.INSTANCE, nodeDepReq,
                ActionListener.wrap(response -> {
                if (response.hasFailures()) {
                    List<String> failedNodeIds = response.failures().stream()
                        .map(failure -> failure.nodeId() + ": " + failure.getMessage())
                        .collect(Collectors.toList());
                    logger.warn("nodes failed to run deprecation checks: {}", failedNodeIds);
                    for (FailedNodeException failure : response.failures()) {
                        logger.debug("node {} failed to run deprecation checks: {}", failure.nodeId(), failure);
                    }
                }
                StandardComponents components = new StandardComponents(xContentRegistry, settings, client);
                pluginSettingIssues(PLUGIN_CHECKERS, threadPool.generic(), components, ActionListener.wrap(
                    deprecationIssues -> listener.onResponse(
                        DeprecationInfoAction.Response.from(state, indexNameExpressionResolver,
                            request, response, INDEX_SETTINGS_CHECKS, CLUSTER_SETTINGS_CHECKS,
                            deprecationIssues)),
                    listener::onFailure
                ));

            }, listener::onFailure));
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DEPRECATION));
        }
    }

    static void pluginSettingIssues(List<DeprecationChecker> checkers,
                                    ExecutorService executorService,
                                    DeprecationChecker.Components components,
                                    ActionListener<Map<String, List<DeprecationIssue>>> listener) {
        NamedChainedExecutor<List<DeprecationIssue>> namedChainedExecutor = new NamedChainedExecutor<>(executorService);
        for (DeprecationChecker checker : checkers) {
            if (checker.enabled(components.settings())) {
                namedChainedExecutor.add(new NamedChainedExecutor.ChainTask<>() {
                    @Override
                    public void run(ActionListener<List<DeprecationIssue>> listener) {
                        checker.check(components, listener);
                    }

                    @Override
                    public String name() {
                        return checker.getName();
                    }
                });
            }
        }
        namedChainedExecutor.execute(listener);
    }

    static class StandardComponents implements DeprecationChecker.Components {

        private final NamedXContentRegistry xContentRegistry;
        private final Settings settings;
        private final Client client;

        StandardComponents(NamedXContentRegistry xContentRegistry, Settings settings, Client client) {
            this.xContentRegistry = xContentRegistry;
            this.settings = settings;
            this.client = client;
        }

        @Override
        public NamedXContentRegistry xContentRegistry() {
            return xContentRegistry;
        }

        @Override
        public Settings settings() {
            return settings;
        }

        @Override
        public Client client() {
            return client;
        }
    }

    static class NamedChainedExecutor<T> {
        public interface ChainTask<T> {
            void run(ActionListener<T> listener);

            String name();
        }

        private final ExecutorService executorService;
        private final LinkedList<ChainTask<T>> tasks = new LinkedList<>();
        private final Map<String, T> collectedResponses;

        /**
         * Creates a new NamedChainedExecutor.
         * Each chainedTask is executed in order serially
         * @param executorService The service where to execute the tasks
         */
        NamedChainedExecutor(ExecutorService executorService) {
            this.executorService = Objects.requireNonNull(executorService);
            this.collectedResponses = new HashMap<>();
        }

        public synchronized void add(ChainTask<T> task) {
            tasks.add(task);
        }

        private synchronized void execute(String previousName, T previousValue, ActionListener<Map<String, T>> listener) {
            if (previousName != null) {
                collectedResponses.put(previousName, previousValue);
            }
            if (tasks.isEmpty()) {
                listener.onResponse(Collections.unmodifiableMap(new HashMap<>(collectedResponses)));
                return;
            }
            ChainTask<T> task = tasks.pop();
            executorService.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() {
                    task.run(ActionListener.wrap(value -> execute(task.name(), value, listener), this::onFailure));
                }
            });
        }

        /**
         * Execute all the chained tasks serially, notify listener when completed
         *
         * @param listener The ActionListener to notify when all executions have been completed
         */
        public synchronized void execute(ActionListener<Map<String, T>> listener) {
            if (tasks.isEmpty()) {
                listener.onResponse(Collections.emptyMap());
                return;
            }
            collectedResponses.clear();
            execute(null, null, listener);
        }
    }
}
