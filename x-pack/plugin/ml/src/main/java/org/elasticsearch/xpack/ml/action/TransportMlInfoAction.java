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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlControllerHolder;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.xpack.ml.MachineLearning.MAX_LAZY_ML_NODES;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_ML_NODE_SIZE;
import static org.elasticsearch.xpack.ml.MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT;

public class TransportMlInfoAction extends HandledTransportAction<MlInfoAction.Request, MlInfoAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportMlInfoAction.class);

    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final Map<String, Object> nativeCodeInfo;

    @Inject
    public TransportMlInfoAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService,
                                 NamedXContentRegistry xContentRegistry, MlControllerHolder mlControllerHolder) {
        super(MlInfoAction.NAME, transportService, actionFilters, MlInfoAction.Request::new);
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
        defaults.put(Job.DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS.getPreferredName(),
            Job.DEFAULT_DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS);
        try {
            defaults.put(CategorizationAnalyzerConfig.CATEGORIZATION_ANALYZER.getPreferredName(),
                CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(Collections.emptyList())
                    .asMap(xContentRegistry).get(CategorizationAnalyzerConfig.CATEGORIZATION_ANALYZER.getPreferredName()));
        } catch (IOException e) {
            logger.error("failed to convert default categorization analyzer to map", e);
        }
        return defaults;
    }

    private ByteSizeValue defaultModelMemoryLimit() {
        ByteSizeValue defaultLimit = ByteSizeValue.ofMb(AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB);
        ByteSizeValue maxModelMemoryLimit = clusterService.getClusterSettings().get(MachineLearningField.MAX_MODEL_MEMORY_LIMIT);
        if (maxModelMemoryLimit != null && maxModelMemoryLimit.getBytes() > 0
            && maxModelMemoryLimit.getBytes() < defaultLimit.getBytes()) {
            return maxModelMemoryLimit;
        }
        return defaultLimit;
    }

    private Map<String, Object> datafeedsDefaults() {
        Map<String, Object> anomalyDetectorsDefaults = new HashMap<>();
        anomalyDetectorsDefaults.put(DatafeedConfig.SCROLL_SIZE.getPreferredName(), DatafeedConfig.DEFAULT_SCROLL_SIZE);
        return anomalyDetectorsDefaults;
    }

    static ByteSizeValue calculateTotalMlMemory(ClusterSettings clusterSettings, DiscoveryNodes nodes) {

        long totalMlMemory = 0;

        for (DiscoveryNode node : nodes) {
            OptionalLong limit = NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings);
            if (limit.isEmpty()) {
                continue;
            }
            totalMlMemory += limit.getAsLong();
        }

        // Round down to a whole number of megabytes, since we generally deal with model
        // memory limits in whole megabytes
        return ByteSizeValue.ofMb(ByteSizeUnit.BYTES.toMB(totalMlMemory));
    }

    static ByteSizeValue calculateEffectiveMaxModelMemoryLimit(ClusterSettings clusterSettings, DiscoveryNodes nodes) {

        long maxMlMemory = -1;
        int numMlNodes = 0;

        for (DiscoveryNode node : nodes) {
            OptionalLong limit = NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings);
            if (limit.isEmpty()) {
                continue;
            }
            maxMlMemory = Math.max(maxMlMemory, limit.getAsLong());
            ++numMlNodes;
        }

        // It is possible that there is scope for more ML nodes to be added
        // to the cluster, in which case take those into account too
        long maxMlNodeSize = clusterSettings.get(MAX_ML_NODE_SIZE).getBytes();
        int maxLazyNodes = clusterSettings.get(MAX_LAZY_ML_NODES);
        if (maxMlNodeSize > 0 && numMlNodes < maxLazyNodes) {
            maxMlMemory = Math.max(maxMlMemory, NativeMemoryCalculator.allowedBytesForMl(
                maxMlNodeSize,
                clusterSettings.get(MAX_MACHINE_MEMORY_PERCENT),
                clusterSettings.get(USE_AUTO_MACHINE_MEMORY_PERCENT)));
        }

        if (maxMlMemory <= 0) {
            // This implies there are currently no ML nodes in the cluster, and
            // no automatic mechanism for adding one, so we have no idea what
            // the effective limit would be if one were added
            return null;
        }

        maxMlMemory -= Math.max(Job.PROCESS_MEMORY_OVERHEAD.getBytes(), DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes());
        maxMlMemory -= MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
        return ByteSizeValue.ofMb(ByteSizeUnit.BYTES.toMB(Math.max(0L, maxMlMemory)));
    }

    private Map<String, Object> limits() {
        Map<String, Object> limits = new HashMap<>();
        ByteSizeValue effectiveMaxModelMemoryLimit = calculateEffectiveMaxModelMemoryLimit(
            clusterService.getClusterSettings(),
            clusterService.state().getNodes());
        ByteSizeValue maxModelMemoryLimit = clusterService.getClusterSettings().get(MachineLearningField.MAX_MODEL_MEMORY_LIMIT);
        if (maxModelMemoryLimit != null && maxModelMemoryLimit.getBytes() > 0) {
            limits.put("max_model_memory_limit", maxModelMemoryLimit.getStringRep());
            if (effectiveMaxModelMemoryLimit == null || effectiveMaxModelMemoryLimit.compareTo(maxModelMemoryLimit) > 0) {
                effectiveMaxModelMemoryLimit = maxModelMemoryLimit;
            }
        }
        if (effectiveMaxModelMemoryLimit != null) {
            limits.put("effective_max_model_memory_limit", effectiveMaxModelMemoryLimit.getStringRep());
        }
        limits.put("total_ml_memory",
            calculateTotalMlMemory(clusterService.getClusterSettings(), clusterService.state().getNodes()).getStringRep());
        return limits;
    }
}
