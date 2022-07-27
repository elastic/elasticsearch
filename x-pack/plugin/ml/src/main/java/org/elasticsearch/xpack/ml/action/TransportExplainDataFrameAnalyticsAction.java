/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.xpack.core.ml.dataframe.explain.MemoryEstimation;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.extractor.ExtractedFieldsDetector;
import org.elasticsearch.xpack.ml.dataframe.extractor.ExtractedFieldsDetectorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.MemoryUsageEstimationProcessManager;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

/**
 * Provides explanations on aspects of the given data frame analytics spec like memory estimation, field selection, etc.
 * Redirects to a different node if the current node is *not* an ML node.
 */
public class TransportExplainDataFrameAnalyticsAction extends HandledTransportAction<
    ExplainDataFrameAnalyticsAction.Request,
    ExplainDataFrameAnalyticsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportExplainDataFrameAnalyticsAction.class);
    private final XPackLicenseState licenseState;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final NodeClient client;
    private final MemoryUsageEstimationProcessManager processManager;
    private final SecurityContext securityContext;
    private final ThreadPool threadPool;
    private final Settings settings;

    @Inject
    public TransportExplainDataFrameAnalyticsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NodeClient client,
        XPackLicenseState licenseState,
        MemoryUsageEstimationProcessManager processManager,
        Settings settings,
        ThreadPool threadPool
    ) {
        super(ExplainDataFrameAnalyticsAction.NAME, transportService, actionFilters, ExplainDataFrameAnalyticsAction.Request::new);
        this.transportService = transportService;
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
        this.licenseState = licenseState;
        this.processManager = Objects.requireNonNull(processManager);
        this.threadPool = threadPool;
        this.settings = settings;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
    }

    @Override
    protected void doExecute(
        Task task,
        ExplainDataFrameAnalyticsAction.Request request,
        ActionListener<ExplainDataFrameAnalyticsAction.Response> listener
    ) {
        if (MachineLearningField.ML_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        // Since the data_frame_analyzer program will be so short-lived and use so little memory when run
        // purely for memory estimation we are happy to run it on nodes that might not be ML nodes. This
        // also helps with the case where there are no ML nodes in the cluster, but lazy ML nodes can be
        // added. We know the ML plugin is enabled on the current node, because this code is in it!
        DiscoveryNode localNode = clusterService.localNode();
        boolean isMlNode = MachineLearning.isMlNode(localNode);
        if (isMlNode || localNode.isMasterNode() || localNode.canContainData() || localNode.isIngestNode()) {
            if (isMlNode == false) {
                logger.debug("estimating data frame analytics memory on non-ML node");
            }
            explain(task, request, listener);
        } else {
            redirectToSuitableNode(request, listener);
        }
    }

    private void explain(
        Task task,
        ExplainDataFrameAnalyticsAction.Request request,
        ActionListener<ExplainDataFrameAnalyticsAction.Response> listener
    ) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        final ExtractedFieldsDetectorFactory extractedFieldsDetectorFactory = new ExtractedFieldsDetectorFactory(
            new ParentTaskAssigningClient(client, parentTaskId)
        );
        if (XPackSettings.SECURITY_ENABLED.get(settings)) {
            useSecondaryAuthIfAvailable(this.securityContext, () -> {
                // Set the auth headers (preferring the secondary headers) to the caller's.
                // Regardless if the config was previously stored or not.
                DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder(request.getConfig()).setHeaders(
                    ClientHelper.getPersistableSafeSecurityHeaders(threadPool.getThreadContext(), clusterService.state())
                ).build();
                extractedFieldsDetectorFactory.createFromSource(
                    config,
                    ActionListener.wrap(
                        extractedFieldsDetector -> explain(parentTaskId, config, extractedFieldsDetector, listener),
                        listener::onFailure
                    )
                );
            });
        } else {
            extractedFieldsDetectorFactory.createFromSource(
                request.getConfig(),
                ActionListener.wrap(
                    extractedFieldsDetector -> explain(parentTaskId, request.getConfig(), extractedFieldsDetector, listener),
                    listener::onFailure
                )
            );
        }
    }

    private void explain(
        TaskId parentTaskId,
        DataFrameAnalyticsConfig config,
        ExtractedFieldsDetector extractedFieldsDetector,
        ActionListener<ExplainDataFrameAnalyticsAction.Response> listener
    ) {
        Tuple<ExtractedFields, List<FieldSelection>> fieldExtraction = extractedFieldsDetector.detect();
        if (fieldExtraction.v1().getAllFields().isEmpty()) {
            listener.onResponse(
                new ExplainDataFrameAnalyticsAction.Response(
                    fieldExtraction.v2(),
                    new MemoryEstimation(ByteSizeValue.ZERO, ByteSizeValue.ZERO)
                )
            );
            return;
        }

        ActionListener<MemoryEstimation> memoryEstimationListener = ActionListener.wrap(
            memoryEstimation -> listener.onResponse(new ExplainDataFrameAnalyticsAction.Response(fieldExtraction.v2(), memoryEstimation)),
            listener::onFailure
        );

        estimateMemoryUsage(parentTaskId, config, fieldExtraction.v1(), memoryEstimationListener);
    }

    /**
     * Performs memory usage estimation.
     * Memory usage estimation spawns an ML C++ process which is
     * only available on nodes where the ML plugin is enabled.
     */
    private void estimateMemoryUsage(
        TaskId parentTaskId,
        DataFrameAnalyticsConfig config,
        ExtractedFields extractedFields,
        ActionListener<MemoryEstimation> listener
    ) {
        final String estimateMemoryTaskId = "memory_usage_estimation_" + parentTaskId.getId();
        DataFrameDataExtractorFactory extractorFactory = DataFrameDataExtractorFactory.createForSourceIndices(
            new ParentTaskAssigningClient(client, parentTaskId),
            estimateMemoryTaskId,
            config,
            extractedFields
        );
        processManager.runJobAsync(
            estimateMemoryTaskId,
            config,
            extractorFactory,
            ActionListener.wrap(
                result -> listener.onResponse(
                    new MemoryEstimation(result.getExpectedMemoryWithoutDisk(), result.getExpectedMemoryWithDisk())
                ),
                listener::onFailure
            )
        );
    }

    /**
     * Find a suitable node in the cluster that we can run the memory
     * estimation process on, and redirect the request to this node.
     */
    private void redirectToSuitableNode(
        ExplainDataFrameAnalyticsAction.Request request,
        ActionListener<ExplainDataFrameAnalyticsAction.Response> listener
    ) {
        Optional<DiscoveryNode> node = findSuitableNode(clusterService.state());
        if (node.isPresent()) {
            transportService.sendRequest(
                node.get(),
                actionName,
                request,
                new ActionListenerResponseHandler<>(listener, ExplainDataFrameAnalyticsAction.Response::new)
            );
        } else {
            listener.onFailure(ExceptionsHelper.badRequestException("No ML, data or ingest node to run on"));
        }
    }

    /**
     * Find a node that can run the memory estimation process.
     * Prefer the first available ML node in the cluster state.  If
     * there isn't one, redirect to a master-eligible node, with a
     * preference for one that isn't the active master.  Master-eligible
     * nodes are used as the fallback instead of other types, as we
     * demand that the ML plugin is enabled on all master-eligible nodes
     * when ML is being used, but not other non-ML nodes.
     */
    private static Optional<DiscoveryNode> findSuitableNode(ClusterState clusterState) {
        DiscoveryNodes nodes = clusterState.getNodes();
        for (DiscoveryNode node : nodes) {
            if (MachineLearning.isMlNode(node)) {
                return Optional.of(node);
            }
        }
        DiscoveryNode currentMaster = null;
        for (DiscoveryNode node : nodes) {
            if (node.isMasterNode()) {
                if (node.getId().equals(nodes.getMasterNodeId())) {
                    currentMaster = node;
                } else {
                    return Optional.of(node);
                }
            }
        }
        return Optional.ofNullable(currentMaster);
    }
}
