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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlControllerHolder;
import org.elasticsearch.xpack.ml.utils.MlProcessors;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeoutException;

public class TransportMlInfoAction extends HandledTransportAction<MlInfoAction.Request, MlInfoAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportMlInfoAction.class);

    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final Map<String, Object> nativeCodeInfo;

    @Inject
    public TransportMlInfoAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        MlControllerHolder mlControllerHolder
    ) {
        super(MlInfoAction.NAME, transportService, actionFilters, MlInfoAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;

        try {
            nativeCodeInfo = mlControllerHolder.getMlController().getNativeCodeInfo();
        } catch (TimeoutException e) {
            throw new RuntimeException("Could not get native code info from native controller", e);
        }
    }

    @Override
    protected void doExecute(Task task, MlInfoAction.Request request, ActionListener<MlInfoAction.Response> listener) {
        Map<String, Object> info = new HashMap<>();
        info.put("defaults", defaults());
        info.put("limits", limits());
        info.put("native_code", nativeCodeInfo);
        info.put(MlMetadata.UPGRADE_MODE.getPreferredName(), upgradeMode());
        listener.onResponse(new MlInfoAction.Response(info));
    }

    private Map<String, Object> defaults() {
        Map<String, Object> defaults = new HashMap<>();
        defaults.put("anomaly_detectors", anomalyDetectorsDefaults());
        defaults.put("datafeeds", datafeedsDefaults());
        return defaults;
    }

    private boolean upgradeMode() {
        return MlMetadata.getMlMetadata(clusterService.state()).isUpgradeMode();
    }

    private Map<String, Object> anomalyDetectorsDefaults() {
        Map<String, Object> defaults = new LinkedHashMap<>();
        defaults.put(AnalysisLimits.MODEL_MEMORY_LIMIT.getPreferredName(), defaultModelMemoryLimit());
        defaults.put(AnalysisLimits.CATEGORIZATION_EXAMPLES_LIMIT.getPreferredName(), AnalysisLimits.DEFAULT_CATEGORIZATION_EXAMPLES_LIMIT);
        defaults.put(Job.MODEL_SNAPSHOT_RETENTION_DAYS.getPreferredName(), Job.DEFAULT_MODEL_SNAPSHOT_RETENTION_DAYS);
        defaults.put(
            Job.DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS.getPreferredName(),
            Job.DEFAULT_DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS
        );
        try {
            defaults.put(
                CategorizationAnalyzerConfig.CATEGORIZATION_ANALYZER.getPreferredName(),
                CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(Collections.emptyList())
                    .asMap(xContentRegistry)
                    .get(CategorizationAnalyzerConfig.CATEGORIZATION_ANALYZER.getPreferredName())
            );
        } catch (IOException e) {
            logger.error("failed to convert default categorization analyzer to map", e);
        }
        return defaults;
    }

    private ByteSizeValue defaultModelMemoryLimit() {
        ByteSizeValue defaultLimit = ByteSizeValue.ofMb(AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB);
        ByteSizeValue maxModelMemoryLimit = NativeMemoryCalculator.getMaxModelMemoryLimit(clusterService);
        if (maxModelMemoryLimit != null && maxModelMemoryLimit.getBytes() > 0 && maxModelMemoryLimit.getBytes() < defaultLimit.getBytes()) {
            return maxModelMemoryLimit;
        }
        return defaultLimit;
    }

    private static Map<String, Object> datafeedsDefaults() {
        Map<String, Object> anomalyDetectorsDefaults = new HashMap<>();
        anomalyDetectorsDefaults.put(DatafeedConfig.SCROLL_SIZE.getPreferredName(), DatafeedConfig.DEFAULT_SCROLL_SIZE);
        return anomalyDetectorsDefaults;
    }

    private Map<String, Object> limits() {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        DiscoveryNodes nodes = clusterService.state().getNodes();
        Map<String, Object> limits = new HashMap<>();
        ByteSizeValue effectiveMaxModelMemoryLimit = NativeMemoryCalculator.calculateMaxModelMemoryLimitToFit(clusterSettings, nodes);
        ByteSizeValue maxModelMemoryLimit = NativeMemoryCalculator.getMaxModelMemoryLimit(clusterService);
        if (maxModelMemoryLimit != null && maxModelMemoryLimit.getBytes() > 0) {
            limits.put("max_model_memory_limit", maxModelMemoryLimit.getStringRep());
            if (effectiveMaxModelMemoryLimit == null || effectiveMaxModelMemoryLimit.compareTo(maxModelMemoryLimit) > 0) {
                effectiveMaxModelMemoryLimit = maxModelMemoryLimit;
            }
        }
        if (effectiveMaxModelMemoryLimit != null) {
            limits.put("effective_max_model_memory_limit", effectiveMaxModelMemoryLimit.getStringRep());
        }
        limits.put("total_ml_memory", NativeMemoryCalculator.calculateTotalMlMemory(clusterSettings, nodes).getStringRep());

        // Add processor information _if_ known with certainty. It won't be known with certainty if autoscaling is enabled.
        // If we can scale up in terms of memory, assume we can also scale up in terms of processors.
        List<DiscoveryNode> mlNodes = nodes.stream().filter(MachineLearning::isMlNode).toList();
        if (areMlNodesBiggestSize(clusterSettings.get(MachineLearning.MAX_ML_NODE_SIZE), mlNodes)) {
            Processors singleNodeProcessors = MlProcessors.getMaxMlNodeProcessors(
                nodes,
                clusterSettings.get(MachineLearning.ALLOCATED_PROCESSORS_SCALE)
            );
            if (singleNodeProcessors.count() > 0) {
                limits.put("max_single_ml_node_processors", singleNodeProcessors.roundUp());
            }
            Processors totalMlProcessors = MlProcessors.getTotalMlNodeProcessors(
                nodes,
                clusterSettings.get(MachineLearning.ALLOCATED_PROCESSORS_SCALE)
            );
            if (totalMlProcessors.count() > 0) {
                int potentialExtraProcessors = Math.max(0, clusterSettings.get(MachineLearning.MAX_LAZY_ML_NODES) - mlNodes.size())
                    * singleNodeProcessors.roundUp();
                limits.put("total_ml_processors", totalMlProcessors.roundUp() + potentialExtraProcessors);
            }
        }
        return limits;
    }

    static boolean areMlNodesBiggestSize(ByteSizeValue maxMLNodeSize, Collection<DiscoveryNode> mlNodes) {
        if (maxMLNodeSize.getBytes() == 0) {
            return true;
        }

        OptionalLong smallestMLNode = mlNodes.stream().map(NodeLoadDetector::getNodeSize).flatMapToLong(OptionalLong::stream).min();

        return smallestMLNode.isPresent() && smallestMLNode.getAsLong() >= maxMLNodeSize.getBytes();
    }
}
