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
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final boolean hasMasterRole;
    private final List<AutoCloseable> metrics = new ArrayList<>();

    private volatile Map<String, Object> isMasterMap = MASTER_FALSE_MAP;
    private volatile CpsDatafeedCounts cpsCounts = CpsDatafeedCounts.EMPTY;
    private final AtomicBoolean pollInProgress = new AtomicBoolean(false);

    private Scheduler.Cancellable scheduledPoll;
    private volatile TimeValue pollInterval;

    public MlConfigMetrics(
        MeterRegistry meterRegistry,
        ClusterService clusterService,
        ThreadPool threadPool,
        DatafeedConfigProvider datafeedConfigProvider,
        Settings settings
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.datafeedConfigProvider = datafeedConfigProvider;
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
            meterRegistry.registerLongGauge(
                "es.ml.datafeeds.cps.internal_credentials.current",
                "Count of datafeed configs with a persisted cloud_internal_credential envelope.",
                "datafeeds",
                () -> new LongWithAttributes(cpsCounts.internalCredentialCount(), isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongsGauge(
                "es.ml.datafeeds.cps.auth_type.current",
                "Count of datafeed configs by CPS authentication type.",
                "datafeeds",
                this::observeAuthTypeCounts
            )
        );
        metrics.add(
            meterRegistry.registerLongsGauge(
                "es.ml.datafeeds.cps.project_routing.current",
                "Count of datafeed configs by project_routing bucket.",
                "datafeeds",
                this::observeProjectRoutingCounts
            )
        );
    }

    private Collection<LongWithAttributes> observeAuthTypeCounts() {
        List<LongWithAttributes> observations = new ArrayList<>(AuthType.values().length);
        for (AuthType authType : AuthType.values()) {
            observations.add(
                new LongWithAttributes(cpsCounts.countForAuthType(authType), attributesWith("auth_type", authType.attributeValue()))
            );
        }
        return observations;
    }

    private Collection<LongWithAttributes> observeProjectRoutingCounts() {
        List<LongWithAttributes> observations = new ArrayList<>(ProjectRoutingBucket.values().length);
        for (ProjectRoutingBucket bucket : ProjectRoutingBucket.values()) {
            observations.add(
                new LongWithAttributes(cpsCounts.countForRoutingBucket(bucket), attributesWith("routing_bucket", bucket.attributeValue()))
            );
        }
        return observations;
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
        if (event.localNodeMaster() == false || crossProjectMlEnabled() == false) {
            cpsCounts = CpsDatafeedCounts.EMPTY;
        }
    }

    void pollIfMaster() {
        if (clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
            cpsCounts = CpsDatafeedCounts.EMPTY;
            return;
        }
        if (crossProjectMlEnabled() == false) {
            cpsCounts = CpsDatafeedCounts.EMPTY;
            return;
        }
        if (pollInProgress.compareAndSet(false, true) == false) {
            return;
        }
        datafeedConfigProvider.expandDatafeedConfigs("_all", true, null, ActionListener.wrap(builders -> {
            try {
                List<DatafeedConfig> configs = builders.stream().map(DatafeedConfig.Builder::build).toList();
                // The scan is async: this node may have lost mastership while the request was in flight.
                // clusterChanged() already reset cpsCounts to EMPTY on demotion; re-check here so a late
                // response can't overwrite that with stale non-empty counts on a now non-master node.
                cpsCounts = clusterService.state().nodes().isLocalNodeElectedMaster() ? computeCounts(configs) : CpsDatafeedCounts.EMPTY;
            } finally {
                pollInProgress.set(false);
            }
        }, e -> {
            logger.warn("Failed to poll datafeed configs for CPS metrics", e);
            pollInProgress.set(false);
        }));
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
