/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.search.crossproject.ProjectRoutingResolver;
import org.elasticsearch.telemetry.metric.LongAsyncMeasurement;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.ml.datafeed.DatafeedSearchTelemetry;
import org.elasticsearch.xpack.ml.datafeed.DatafeedSearchTelemetry.ExtractorType;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Gauges derived from ML configuration documents in {@code .ml-config}, collected by periodically
 * scanning the config index on the elected master. CPS datafeed adoption metrics are the first
 * metric family exposed here.
 */
public final class MlConfigMetrics extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(MlConfigMetrics.class);

    /**
     * Alias routing expression that matches all authorized target projects.
     */
    private static final String ROUTING_ALIAS_MATCH_ALL = "_alias:*";

    private static final Map<String, Object> MASTER_TRUE_MAP = Map.of("es.ml.is_master", Boolean.TRUE);
    private static final Map<String, Object> MASTER_FALSE_MAP = Map.of("es.ml.is_master", Boolean.FALSE);

    /** Marks a total that has not been counted since this node became master; its gauge records nothing. */
    private static final long TOTAL_UNAVAILABLE = -1L;

    public static final Setting<TimeValue> POLL_INTERVAL = Setting.timeSetting(
        "xpack.ml.config.metrics.poll_interval",
        TimeValue.timeValueSeconds(300),
        TimeValue.timeValueSeconds(10),
        Property.NodeScope
    );

    enum ProjectRoutingBucket {
        UNQUALIFIED("unqualified"),
        LOCAL_ONLY("local_only"),
        ALL_PROJECTS("all_projects"),
        ALIAS_PATTERN("alias_pattern"),
        TAG_EXPRESSION("tag_expression");

        private final String attributeValue;

        ProjectRoutingBucket(String attributeValue) {
            this.attributeValue = attributeValue;
        }

        String attributeValue() {
            return attributeValue;
        }
    }

    enum AuthType {
        UIAM("uiam"),
        LEGACY("legacy");

        private final String attributeValue;

        AuthType(String attributeValue) {
            this.attributeValue = attributeValue;
        }

        String attributeValue() {
            return attributeValue;
        }
    }

    static final class CpsDatafeedCounts {
        static final CpsDatafeedCounts EMPTY = new CpsDatafeedCounts(0, 0, 0, emptyRoutingCounts());

        private final long internalCredentialCount;
        private final long uiamAuthCount;
        private final long legacyAuthCount;
        private final Map<ProjectRoutingBucket, Long> routingCounts;

        CpsDatafeedCounts(
            long internalCredentialCount,
            long uiamAuthCount,
            long legacyAuthCount,
            Map<ProjectRoutingBucket, Long> routingCounts
        ) {
            this.internalCredentialCount = internalCredentialCount;
            this.uiamAuthCount = uiamAuthCount;
            this.legacyAuthCount = legacyAuthCount;
            this.routingCounts = Map.copyOf(routingCounts);
        }

        long internalCredentialCount() {
            return internalCredentialCount;
        }

        long countForAuthType(AuthType authType) {
            return authType == AuthType.UIAM ? uiamAuthCount : legacyAuthCount;
        }

        long countForRoutingBucket(ProjectRoutingBucket bucket) {
            return routingCounts.getOrDefault(bucket, 0L);
        }

        private static Map<ProjectRoutingBucket, Long> emptyRoutingCounts() {
            Map<ProjectRoutingBucket, Long> counts = new EnumMap<>(ProjectRoutingBucket.class);
            for (ProjectRoutingBucket bucket : ProjectRoutingBucket.values()) {
                counts.put(bucket, 0L);
            }
            return counts;
        }
    }

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final NamedXContentRegistry xContentRegistry;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final boolean hasMasterRole;
    private final List<AutoCloseable> metrics = new ArrayList<>();

    private volatile Map<String, Object> isMasterMap = MASTER_FALSE_MAP;
    private volatile CpsDatafeedCounts cpsCounts = CpsDatafeedCounts.EMPTY;
    private volatile Map<ExtractorType, Long> extractorTypeCounts = emptyExtractorTypeCounts();
    private volatile long anomalyDetectorTotal = TOTAL_UNAVAILABLE;
    private volatile long datafeedTotal = TOTAL_UNAVAILABLE;
    private final AtomicBoolean pollInProgress = new AtomicBoolean(false);

    private Scheduler.Cancellable scheduledPoll;
    private volatile TimeValue pollInterval;

    public MlConfigMetrics(
        MeterRegistry meterRegistry,
        ClusterService clusterService,
        ThreadPool threadPool,
        JobConfigProvider jobConfigProvider,
        DatafeedConfigProvider datafeedConfigProvider,
        Settings settings,
        NamedXContentRegistry xContentRegistry
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.jobConfigProvider = jobConfigProvider;
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry);
        this.crossProjectModeDecider = new CrossProjectModeDecider(settings);
        this.hasMasterRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.MASTER_ROLE);
        this.pollInterval = POLL_INTERVAL.get(settings);
        if (hasMasterRole) {
            clusterService.addListener(this);
            registerGauges(meterRegistry);
        }
    }

    private void registerGauges(MeterRegistry meterRegistry) {
        metrics.add(
            meterRegistry.registerLongAsyncGauge(
                "es.ml.datafeeds.cps.internal_credentials.current",
                "Count of datafeed configs with a persisted cloud_internal_credential envelope.",
                "datafeeds",
                measurement -> measurement.record(cpsCounts.internalCredentialCount(), isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongAsyncGauge(
                "es.ml.datafeeds.cps.auth_type.current",
                "Count of datafeed configs by CPS authentication type.",
                "datafeeds",
                this::recordAuthTypeCounts
            )
        );
        metrics.add(
            meterRegistry.registerLongAsyncGauge(
                "es.ml.datafeeds.cps.project_routing.current",
                "Count of datafeed configs by project_routing bucket.",
                "datafeeds",
                this::recordProjectRoutingCounts
            )
        );
        metrics.add(
            meterRegistry.registerLongAsyncGauge(
                "es.ml.datafeeds.extractor_type.current",
                "Count of datafeed configs by extractor type (scroll, aggregation, composite).",
                "datafeeds",
                this::recordExtractorTypeCounts
            )
        );
        metrics.add(
            meterRegistry.registerLongAsyncGauge(
                "es.ml.anomaly_detectors.total.current",
                "Count of anomaly detection job configs, excluding jobs being deleted.",
                "jobs",
                measurement -> recordTotal(measurement, anomalyDetectorTotal)
            )
        );
        metrics.add(
            meterRegistry.registerLongAsyncGauge(
                "es.ml.datafeeds.total.current",
                "Count of datafeed configs.",
                "datafeeds",
                measurement -> recordTotal(measurement, datafeedTotal)
            )
        );
    }

    private void recordTotal(LongAsyncMeasurement measurement, long total) {
        if (total != TOTAL_UNAVAILABLE) {
            measurement.record(total, isMasterMap);
        }
    }

    private void recordExtractorTypeCounts(LongAsyncMeasurement measurement) {
        for (ExtractorType extractorType : ExtractorType.values()) {
            measurement.record(
                extractorTypeCounts.getOrDefault(extractorType, 0L),
                attributesWith("es_extractor_type", extractorType.attributeValue())
            );
        }
    }

    private void recordAuthTypeCounts(LongAsyncMeasurement measurement) {
        for (AuthType authType : AuthType.values()) {
            measurement.record(cpsCounts.countForAuthType(authType), attributesWith("es_auth_type", authType.attributeValue()));
        }
    }

    private void recordProjectRoutingCounts(LongAsyncMeasurement measurement) {
        for (ProjectRoutingBucket bucket : ProjectRoutingBucket.values()) {
            measurement.record(cpsCounts.countForRoutingBucket(bucket), attributesWith("es_routing_bucket", bucket.attributeValue()));
        }
    }

    private Map<String, Object> attributesWith(String key, String value) {
        Map<String, Object> attributes = new HashMap<>(isMasterMap);
        attributes.put(key, value);
        return attributes;
    }

    @Override
    protected void doStart() {
        if (hasMasterRole == false) {
            return;
        }
        scheduledPoll = threadPool.scheduleWithFixedDelay(
            this::pollIfMaster,
            pollInterval,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
    }

    @Override
    protected void doStop() {
        if (scheduledPoll != null) {
            scheduledPoll.cancel();
            scheduledPoll = null;
        }
    }

    @Override
    protected void doClose() {
        metrics.forEach(metric -> {
            try {
                metric.close();
            } catch (Exception e) {
                logger.warn("metrics close() method should not throw Exception", e);
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        isMasterMap = event.localNodeMaster() ? MASTER_TRUE_MAP : MASTER_FALSE_MAP;
        if (event.localNodeMaster() == false) {
            cpsCounts = CpsDatafeedCounts.EMPTY;
            extractorTypeCounts = emptyExtractorTypeCounts();
            resetTotals();
            return;
        }
        if (crossProjectMlEnabled() == false) {
            cpsCounts = CpsDatafeedCounts.EMPTY;
        }
    }

    void pollIfMaster() {
        if (clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
            cpsCounts = CpsDatafeedCounts.EMPTY;
            extractorTypeCounts = emptyExtractorTypeCounts();
            resetTotals();
            return;
        }
        if (pollInProgress.compareAndSet(false, true) == false) {
            return;
        }
        // pollTotals() runs after the scan whether it succeeds or fails; pollDatafeedTotal(), the last link of the chain, releases
        // pollInProgress.
        datafeedConfigProvider.expandDatafeedConfigs("_all", true, null, ActionListener.runAfter(ActionListener.wrap(builders -> {
            List<DatafeedConfig> configs = builders.stream().map(DatafeedConfig.Builder::build).toList();
            // The scan is async: this node may have lost mastership while the request was in flight.
            // clusterChanged() already reset counts on demotion; re-check here so a late response
            // can't overwrite that with stale non-empty counts on a now non-master node.
            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                extractorTypeCounts = computeExtractorTypeCounts(configs, xContentRegistry);
                cpsCounts = crossProjectMlEnabled() ? computeCounts(configs) : CpsDatafeedCounts.EMPTY;
            } else {
                cpsCounts = CpsDatafeedCounts.EMPTY;
                extractorTypeCounts = emptyExtractorTypeCounts();
            }
        }, e -> logger.warn("Failed to poll datafeed configs for ML config metrics", e)), this::pollTotals));
    }

    private void pollTotals() {
        jobConfigProvider.countJobs(ActionListener.runAfter(ActionListener.wrap(count -> {
            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                anomalyDetectorTotal = count;
            } else {
                resetTotals();
            }
        }, e -> logger.warn("Failed to count anomaly detector jobs for ML config metrics", e)), this::pollDatafeedTotal));
    }

    private void pollDatafeedTotal() {
        datafeedConfigProvider.countDatafeeds(ActionListener.runAfter(ActionListener.wrap(count -> {
            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                datafeedTotal = count;
            } else {
                resetTotals();
            }
        }, e -> logger.warn("Failed to count datafeeds for ML config metrics", e)), () -> pollInProgress.set(false)));
    }

    private void resetTotals() {
        anomalyDetectorTotal = TOTAL_UNAVAILABLE;
        datafeedTotal = TOTAL_UNAVAILABLE;
    }

    static Map<ExtractorType, Long> computeExtractorTypeCounts(Iterable<DatafeedConfig> configs, NamedXContentRegistry xContentRegistry) {
        Map<ExtractorType, Long> counts = emptyExtractorTypeCounts();
        for (DatafeedConfig config : configs) {
            ExtractorType extractorType = DatafeedSearchTelemetry.classifyExtractorType(config, xContentRegistry);
            counts.merge(extractorType, 1L, Long::sum);
        }
        return counts;
    }

    private static Map<ExtractorType, Long> emptyExtractorTypeCounts() {
        Map<ExtractorType, Long> counts = new EnumMap<>(ExtractorType.class);
        for (ExtractorType extractorType : ExtractorType.values()) {
            counts.put(extractorType, 0L);
        }
        return counts;
    }

    static CpsDatafeedCounts computeCounts(Iterable<DatafeedConfig> configs) {
        long internalCredentialCount = 0;
        long uiamAuthCount = 0;
        long legacyAuthCount = 0;
        Map<ProjectRoutingBucket, Long> routingCounts = CpsDatafeedCounts.emptyRoutingCounts();
        for (DatafeedConfig config : configs) {
            if (config.getCloudInternalCredential() != null) {
                internalCredentialCount++;
                uiamAuthCount++;
            } else if (config.getHeaders().isEmpty() == false) {
                legacyAuthCount++;
            }
            ProjectRoutingBucket bucket = routingBucket(config.getProjectRouting());
            routingCounts.merge(bucket, 1L, Long::sum);
        }
        return new CpsDatafeedCounts(internalCredentialCount, uiamAuthCount, legacyAuthCount, routingCounts);
    }

    static ProjectRoutingBucket routingBucket(String projectRouting) {
        if (projectRouting == null) {
            return ProjectRoutingBucket.UNQUALIFIED;
        }
        if (ProjectRoutingResolver.LOCAL_ONLY.equals(projectRouting) || ProjectRoutingResolver.ORIGIN.equals(projectRouting)) {
            return ProjectRoutingBucket.LOCAL_ONLY;
        }
        if (ROUTING_ALIAS_MATCH_ALL.equals(projectRouting)) {
            return ProjectRoutingBucket.ALL_PROJECTS;
        }
        if (projectRouting.startsWith("_alias:")) {
            return ProjectRoutingBucket.ALIAS_PATTERN;
        }
        return ProjectRoutingBucket.TAG_EXPRESSION;
    }

    private boolean crossProjectMlEnabled() {
        return crossProjectModeDecider.crossProjectEnabled() && CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled();
    }
}
