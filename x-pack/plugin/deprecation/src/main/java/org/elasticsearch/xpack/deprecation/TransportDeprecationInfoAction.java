/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;

public class TransportDeprecationInfoAction extends TransportMasterNodeReadAction<
    DeprecationInfoAction.Request,
    DeprecationInfoAction.Response> {
    private static final DeprecationChecker ML_CHECKER = new MlDeprecationChecker();
    private static final Logger logger = LogManager.getLogger(TransportDeprecationInfoAction.class);

    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings settings;
    private final NamedXContentRegistry xContentRegistry;
    private volatile List<String> skipTheseDeprecations;

    @Inject
    public TransportDeprecationInfoAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NodeClient client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(
            DeprecationInfoAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeprecationInfoAction.Request::new,
            DeprecationInfoAction.Response::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settings = settings;
        this.xContentRegistry = xContentRegistry;
        skipTheseDeprecations = DeprecationChecks.SKIP_DEPRECATIONS_SETTING.get(settings);
        // Safe to register this here because it happens synchronously before the cluster service is started:
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DeprecationChecks.SKIP_DEPRECATIONS_SETTING, this::setSkipDeprecations);
    }

    private <T> void setSkipDeprecations(List<String> skipDeprecations) {
        this.skipTheseDeprecations = Collections.unmodifiableList(skipDeprecations);
    }

    @Override
    protected ClusterBlockException checkBlock(DeprecationInfoAction.Request request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected final void masterOperation(
        Task task,
        final DeprecationInfoAction.Request request,
        ClusterState state,
        final ActionListener<DeprecationInfoAction.Response> listener
    ) {
        NodesDeprecationCheckRequest nodeDepReq = new NodesDeprecationCheckRequest("_all");
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.DEPRECATION_ORIGIN,
            NodesDeprecationCheckAction.INSTANCE,
            nodeDepReq,
            listener.delegateFailureAndWrap((l, response) -> {
                if (response.hasFailures()) {
                    List<String> failedNodeIds = response.failures()
                        .stream()
                        .map(failure -> failure.nodeId() + ": " + failure.getMessage())
                        .collect(Collectors.toList());
                    logger.warn("nodes failed to run deprecation checks: {}", failedNodeIds);
                    for (FailedNodeException failure : response.failures()) {
                        logger.debug("node {} failed to run deprecation checks: {}", failure.nodeId(), failure);
                    }
                }
                transformConfigs(l.delegateFailureAndWrap((ll, transformConfigs) -> {
                    DeprecationChecker.Components components = new DeprecationChecker.Components(
                        xContentRegistry,
                        settings,
                        new OriginSettingClient(client, ClientHelper.DEPRECATION_ORIGIN)
                    );
                    pluginSettingIssues(
                        List.of(ML_CHECKER, new TransformDeprecationChecker(transformConfigs)),
                        components,
                        new ThreadedActionListener<>(
                            client.threadPool().generic(),
                            ll.map(
                                deprecationIssues -> DeprecationInfoAction.Response.from(
                                    state,
                                    indexNameExpressionResolver,
                                    request,
                                    response,
                                    CLUSTER_SETTINGS_CHECKS,
                                    deprecationIssues,
                                    skipTheseDeprecations,
                                    List.of(
                                        new IndexDeprecationChecker(indexNameExpressionResolver, indexToTransformIds(transformConfigs)),
                                        new DataStreamDeprecationChecker(indexNameExpressionResolver),
                                        new TemplateDeprecationChecker(),
                                        new IlmPolicyDeprecationChecker()
                                    )
                                )
                            )
                        )
                    );
                }));
            })
        );
    }

    static void pluginSettingIssues(
        List<DeprecationChecker> checkers,
        DeprecationChecker.Components components,
        ActionListener<Map<String, List<DeprecationIssue>>> listener
    ) {
        List<DeprecationChecker> enabledCheckers = checkers.stream().filter(c -> c.enabled(components.settings())).toList();
        if (enabledCheckers.isEmpty()) {
            listener.onResponse(Collections.emptyMap());
            return;
        }
        GroupedActionListener<DeprecationChecker.CheckResult> groupedActionListener = new GroupedActionListener<>(
            enabledCheckers.size(),
            listener.delegateFailureAndWrap(
                (l, checkResults) -> l.onResponse(
                    checkResults.stream()
                        .collect(
                            Collectors.toMap(DeprecationChecker.CheckResult::getCheckerName, DeprecationChecker.CheckResult::getIssues)
                        )
                )
            )
        );
        for (DeprecationChecker checker : checkers) {
            checker.check(components, groupedActionListener);
        }
    }

    private void transformConfigs(ActionListener<List<TransformConfig>> transformConfigsListener) {
        transformConfigs(new PageParams(0, PageParams.DEFAULT_SIZE), transformConfigsListener.map(Stream::toList));
    }

    private void transformConfigs(PageParams currentPage, ActionListener<Stream<TransformConfig>> currentPageListener) {
        var request = new GetTransformAction.Request(Metadata.ALL);
        request.setPageParams(currentPage);
        request.setAllowNoResources(true);

        client.execute(
            GetTransformAction.INSTANCE,
            request,
            new ThreadedActionListener<>(
                threadPool.generic(),
                currentPageListener.delegateFailureAndWrap((delegate, getTransformConfigResponse) -> {
                    var currentPageOfConfigs = getTransformConfigResponse.getTransformConfigurations().stream();
                    var currentPageSize = currentPage.getFrom() + currentPage.getSize();
                    var totalTransformConfigCount = getTransformConfigResponse.getTransformConfigurationCount();
                    if (totalTransformConfigCount >= currentPageSize) {
                        var nextPage = new PageParams(currentPageSize, PageParams.DEFAULT_SIZE);
                        transformConfigs(
                            nextPage,
                            delegate.map(nextPageOfConfigs -> Stream.concat(currentPageOfConfigs, nextPageOfConfigs))
                        );
                    } else {
                        delegate.onResponse(currentPageOfConfigs);
                    }
                })
            )
        );
    }

    private Map<String, List<String>> indexToTransformIds(List<TransformConfig> transformConfigs) {
        return transformConfigs.stream()
            .collect(
                Collectors.groupingBy(
                    config -> config.getDestination().getIndex(),
                    Collectors.mapping(TransformConfig::getId, Collectors.toList())
                )
            );
    }

}
