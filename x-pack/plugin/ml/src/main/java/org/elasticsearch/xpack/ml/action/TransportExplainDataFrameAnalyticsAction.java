/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.xpack.core.ml.dataframe.explain.MemoryEstimation;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.extractor.ExtractedFieldsDetector;
import org.elasticsearch.xpack.ml.dataframe.extractor.ExtractedFieldsDetectorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.MemoryUsageEstimationProcessManager;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Provides explanations on aspects of the given data frame analytics spec like memory estimation, field selection, etc.
 * Redirects to a different node if the current node is *not* an ML node.
 */
public class TransportExplainDataFrameAnalyticsAction
    extends HandledTransportAction<PutDataFrameAnalyticsAction.Request, ExplainDataFrameAnalyticsAction.Response> {

    private final XPackLicenseState licenseState;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final NodeClient client;
    private final MemoryUsageEstimationProcessManager processManager;

    @Inject
    public TransportExplainDataFrameAnalyticsAction(TransportService transportService,
                                                    ActionFilters actionFilters,
                                                    ClusterService clusterService,
                                                    NodeClient client,
                                                    XPackLicenseState licenseState,
                                                    MemoryUsageEstimationProcessManager processManager) {
        super(ExplainDataFrameAnalyticsAction.NAME, transportService, actionFilters, PutDataFrameAnalyticsAction.Request::new);
        this.transportService = transportService;
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
        this.licenseState = licenseState;
        this.processManager = Objects.requireNonNull(processManager);
    }

    @Override
    protected void doExecute(Task task,
                             PutDataFrameAnalyticsAction.Request request,
                             ActionListener<ExplainDataFrameAnalyticsAction.Response> listener) {
        if (licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        DiscoveryNode localNode = clusterService.localNode();
        if (MachineLearning.isMlNode(localNode)) {
            explain(task, request, listener);
        } else {
            redirectToMlNode(request, listener);
        }
    }

    private void explain(Task task, PutDataFrameAnalyticsAction.Request request,
                         ActionListener<ExplainDataFrameAnalyticsAction.Response> listener) {
        ExtractedFieldsDetectorFactory extractedFieldsDetectorFactory =
                new ExtractedFieldsDetectorFactory(new ParentTaskAssigningClient(client, task.getParentTaskId()));
        extractedFieldsDetectorFactory.createFromSource(
            request.getConfig(),
            ActionListener.wrap(
                extractedFieldsDetector -> explain(task, request, extractedFieldsDetector, listener),
                listener::onFailure)
        );
    }

    private void explain(Task task, PutDataFrameAnalyticsAction.Request request, ExtractedFieldsDetector extractedFieldsDetector,
                         ActionListener<ExplainDataFrameAnalyticsAction.Response> listener) {
        Tuple<ExtractedFields, List<FieldSelection>> fieldExtraction = extractedFieldsDetector.detect();

        ActionListener<MemoryEstimation> memoryEstimationListener = ActionListener.wrap(
            memoryEstimation -> listener.onResponse(new ExplainDataFrameAnalyticsAction.Response(fieldExtraction.v2(), memoryEstimation)),
            listener::onFailure
        );

        estimateMemoryUsage(task, request, fieldExtraction.v1(), memoryEstimationListener);
    }

    /**
     * Performs memory usage estimation.
     * Memory usage estimation spawns an ML C++ process which is only available on ML nodes. That's why this method can only be called on
     * the ML node.
     */
    private void estimateMemoryUsage(Task task,
                                     PutDataFrameAnalyticsAction.Request request,
                                     ExtractedFields extractedFields,
                                     ActionListener<MemoryEstimation> listener) {
        final String estimateMemoryTaskId = "memory_usage_estimation_" + task.getId();
        DataFrameDataExtractorFactory extractorFactory = DataFrameDataExtractorFactory.createForSourceIndices(
            new ParentTaskAssigningClient(client, task.getParentTaskId()), estimateMemoryTaskId, request.getConfig(), extractedFields);
        processManager.runJobAsync(
            estimateMemoryTaskId,
            request.getConfig(),
            extractorFactory,
            ActionListener.wrap(
                result -> listener.onResponse(
                    new MemoryEstimation(result.getExpectedMemoryWithoutDisk(), result.getExpectedMemoryWithDisk())),
                listener::onFailure
            )
        );
    }

    /**
     * Finds the first available ML node in the cluster and redirects the request to this node.
     */
    private void redirectToMlNode(PutDataFrameAnalyticsAction.Request request,
                                  ActionListener<ExplainDataFrameAnalyticsAction.Response> listener) {
        Optional<DiscoveryNode> node = findMlNode(clusterService.state());
        if (node.isPresent()) {
            transportService.sendRequest(node.get(), actionName, request,
                new ActionListenerResponseHandler<>(listener, ExplainDataFrameAnalyticsAction.Response::new));
        } else {
            listener.onFailure(ExceptionsHelper.badRequestException("No ML node to run on"));
        }
    }

    /**
     * Finds the first available ML node in the cluster state.
     */
    private static Optional<DiscoveryNode> findMlNode(ClusterState clusterState) {
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (MachineLearning.isMlNode(node)) {
                return Optional.of(node);
            }
        }
        return Optional.empty();
    }
}
