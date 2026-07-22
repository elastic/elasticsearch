/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.crossproject.ProjectRoutingResolver;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlConfigMetricsTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private DatafeedConfigProvider datafeedConfigProvider;
    private MeterRegistry meterRegistry;
    private Supplier<LongWithAttributes> internalCredentialsObserver;
    private Supplier<Collection<LongWithAttributes>> authTypeObserver;
    private Supplier<Collection<LongWithAttributes>> projectRoutingObserver;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(
            new ScalingExecutorBuilder(MachineLearning.UTILITY_THREAD_POOL_NAME, 0, 1, TimeValue.timeValueMinutes(10), false)
        );
        clusterService = mock(ClusterService.class);
        datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        meterRegistry = mock(MeterRegistry.class);
        internalCredentialsObserver = captureLongGauge(meterRegistry, "es.ml.datafeeds.cps.internal_credentials.current");
        authTypeObserver = captureLongsGauge(meterRegistry, "es.ml.datafeeds.cps.auth_type.current");
        projectRoutingObserver = captureLongsGauge(meterRegistry, "es.ml.datafeeds.cps.project_routing.current");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        threadPool.close();
        super.tearDown();
    }

    public void testComputeCountsShouldBucketMixedConfigs() {
        assumeTrue("feature under test must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        PersistedCloudCredential uiamCredential = new PersistedCloudCredential("key-1", new SecureString("secret".toCharArray()));
        List<DatafeedConfig> configs = List.of(
            datafeed("flat", null, null, null),
            datafeed("local", ProjectRoutingResolver.LOCAL_ONLY, null, null),
            datafeed("origin", ProjectRoutingResolver.ORIGIN, null, null),
            datafeed("all", "_alias:*", null, null),
            datafeed("alias", "_alias:prod-*", null, null),
            datafeed("tag", "_project._region:us-*", null, null),
            datafeed("uiam", ProjectRoutingResolver.LOCAL_ONLY, uiamCredential, null),
            datafeed("legacy", null, null, Map.of("Authorization", "ApiKey abc"))
        );

        MlConfigMetrics.CpsDatafeedCounts counts = MlConfigMetrics.computeCounts(configs);

        assertThat(counts.internalCredentialCount(), equalTo(1L));
        assertThat(counts.countForAuthType(MlConfigMetrics.AuthType.UIAM), equalTo(1L));
        assertThat(counts.countForAuthType(MlConfigMetrics.AuthType.LEGACY), equalTo(1L));
        assertThat(counts.countForRoutingBucket(MlConfigMetrics.ProjectRoutingBucket.UNQUALIFIED), equalTo(2L));
        assertThat(counts.countForRoutingBucket(MlConfigMetrics.ProjectRoutingBucket.LOCAL_ONLY), equalTo(3L));
        assertThat(counts.countForRoutingBucket(MlConfigMetrics.ProjectRoutingBucket.ALL_PROJECTS), equalTo(1L));
        assertThat(counts.countForRoutingBucket(MlConfigMetrics.ProjectRoutingBucket.ALIAS_PATTERN), equalTo(1L));
        assertThat(counts.countForRoutingBucket(MlConfigMetrics.ProjectRoutingBucket.TAG_EXPRESSION), equalTo(1L));
    }

    public void testRoutingBucketShouldClassifyKnownRoutingValues() {
        assertThat(MlConfigMetrics.routingBucket(null), equalTo(MlConfigMetrics.ProjectRoutingBucket.UNQUALIFIED));
        assertThat(
            MlConfigMetrics.routingBucket(ProjectRoutingResolver.LOCAL_ONLY),
            equalTo(MlConfigMetrics.ProjectRoutingBucket.LOCAL_ONLY)
        );
        assertThat(MlConfigMetrics.routingBucket(ProjectRoutingResolver.ORIGIN), equalTo(MlConfigMetrics.ProjectRoutingBucket.LOCAL_ONLY));
        assertThat(MlConfigMetrics.routingBucket("_alias:*"), equalTo(MlConfigMetrics.ProjectRoutingBucket.ALL_PROJECTS));
        assertThat(MlConfigMetrics.routingBucket("_alias:prod-*"), equalTo(MlConfigMetrics.ProjectRoutingBucket.ALIAS_PATTERN));
        assertThat(MlConfigMetrics.routingBucket("_project._region:us-*"), equalTo(MlConfigMetrics.ProjectRoutingBucket.TAG_EXPRESSION));
    }

    public void testPollIfMasterShouldUpdateGaugeObservers() throws Exception {
        assumeTrue("feature under test must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        Settings settings = cpsMasterSettings();
        mockMasterClusterState();
        when(clusterService.state()).thenAnswer(invocation -> masterClusterState());

        PersistedCloudCredential uiamCredential = new PersistedCloudCredential("key-1", new SecureString("secret".toCharArray()));
        List<DatafeedConfig.Builder> builders = List.of(
            datafeedBuilder("uiam", ProjectRoutingResolver.LOCAL_ONLY, uiamCredential, null),
            datafeedBuilder("legacy", null, null, Map.of("Authorization", "ApiKey abc"))
        );
        stubExpandDatafeedConfigs(builders);

        MlConfigMetrics metrics = new MlConfigMetrics(meterRegistry, clusterService, threadPool, datafeedConfigProvider, settings);
        metrics.pollIfMaster();

        assertThat(internalCredentialsObserver.get().value(), equalTo(1L));
        assertThat(findObservation(authTypeObserver.get(), "auth_type", "uiam"), equalTo(1L));
        assertThat(findObservation(authTypeObserver.get(), "auth_type", "legacy"), equalTo(1L));
        assertThat(findObservation(projectRoutingObserver.get(), "routing_bucket", "local_only"), equalTo(1L));
        assertThat(findObservation(projectRoutingObserver.get(), "routing_bucket", "unqualified"), equalTo(1L));
        assertThat(internalCredentialsObserver.get().attributes().get("es.ml.is_master"), equalTo(Boolean.FALSE));

        metrics.clusterChanged(masterClusterChangedEvent());
        metrics.pollIfMaster();

        assertThat(internalCredentialsObserver.get().value(), equalTo(1L));
        assertThat(internalCredentialsObserver.get().attributes().get("es.ml.is_master"), equalTo(Boolean.TRUE));
    }

    public void testPollIfMasterOnNonMasterNodeShouldNotScanConfigs() {
        assumeTrue("feature under test must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        Settings settings = cpsMasterSettings();
        when(clusterService.state()).thenReturn(nonMasterClusterState());

        MlConfigMetrics metrics = new MlConfigMetrics(meterRegistry, clusterService, threadPool, datafeedConfigProvider, settings);
        metrics.pollIfMaster();

        verify(datafeedConfigProvider, never()).expandDatafeedConfigs(anyString(), eq(true), isNull(), any());
        assertThat(internalCredentialsObserver.get().value(), equalTo(0L));
    }

    public void testPollIfMasterOnNonMasterShouldClearCachedCounts() {
        assumeTrue("feature under test must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        Settings settings = cpsMasterSettings();
        when(clusterService.state()).thenAnswer(invocation -> masterClusterState());

        PersistedCloudCredential uiamCredential = new PersistedCloudCredential("key-1", new SecureString("secret".toCharArray()));
        stubExpandDatafeedConfigs(List.of(datafeedBuilder("uiam", ProjectRoutingResolver.LOCAL_ONLY, uiamCredential, null)));

        MlConfigMetrics metrics = new MlConfigMetrics(meterRegistry, clusterService, threadPool, datafeedConfigProvider, settings);
        metrics.clusterChanged(masterClusterChangedEvent());
        metrics.pollIfMaster();
        assertThat(internalCredentialsObserver.get().value(), equalTo(1L));

        when(clusterService.state()).thenReturn(nonMasterClusterState());
        metrics.pollIfMaster();

        assertThat(internalCredentialsObserver.get().value(), equalTo(0L));
    }

    public void testPollIfMasterShouldClearCountsWhenDemotedWhileScanInFlight() {
        assumeTrue("feature under test must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        Settings settings = cpsMasterSettings();
        when(clusterService.state()).thenAnswer(invocation -> masterClusterState());

        PersistedCloudCredential uiamCredential = new PersistedCloudCredential("key-1", new SecureString("secret".toCharArray()));
        List<DatafeedConfig.Builder> builders = List.of(datafeedBuilder("uiam", ProjectRoutingResolver.LOCAL_ONLY, uiamCredential, null));

        // Simulate the response arriving after this node lost mastership mid-flight: flip the
        // mocked cluster state to non-master before invoking the listener, matching the race
        // reported in https://github.com/elastic/elasticsearch/pull/153951#discussion_r3588720677.
        doAnswer(invocation -> {
            when(clusterService.state()).thenReturn(nonMasterClusterState());
            @SuppressWarnings("unchecked")
            ActionListener<List<DatafeedConfig.Builder>> listener = invocation.getArgument(3);
            listener.onResponse(builders);
            return null;
        }).when(datafeedConfigProvider).expandDatafeedConfigs(eq("_all"), eq(true), isNull(), any());

        MlConfigMetrics metrics = new MlConfigMetrics(meterRegistry, clusterService, threadPool, datafeedConfigProvider, settings);
        metrics.pollIfMaster();

        assertThat(internalCredentialsObserver.get().value(), equalTo(0L));
    }

    private static Settings cpsMasterSettings() {
        return Settings.builder().put("serverless.cross_project.enabled", true).put(DiscoveryNodeRole.MASTER_ROLE.roleName(), true).build();
    }

    private void mockMasterClusterState() {
        when(clusterService.state()).thenReturn(nonMasterClusterState());
    }

    private ClusterState masterClusterState() {
        var localNode = DiscoveryNodeUtils.create(
            "node-1",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(localNode)
            .localNodeId(localNode.getId())
            .masterNodeId(localNode.getId())
            .build();
        return ClusterState.builder(new ClusterName("test")).nodes(nodes).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
    }

    private ClusterState nonMasterClusterState() {
        var localNode = DiscoveryNodeUtils.create(
            "node-1",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        var masterNode = DiscoveryNodeUtils.create(
            "node-2",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(localNode)
            .add(masterNode)
            .localNodeId(localNode.getId())
            .masterNodeId(masterNode.getId())
            .build();
        return ClusterState.builder(new ClusterName("test")).nodes(nodes).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
    }

    private ClusterChangedEvent masterClusterChangedEvent() {
        ClusterState masterState = masterClusterState();
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeMaster()).thenReturn(true);
        when(event.state()).thenReturn(masterState);
        return event;
    }

    private void stubExpandDatafeedConfigs(List<DatafeedConfig.Builder> builders) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<List<DatafeedConfig.Builder>> listener = invocation.getArgument(3);
            listener.onResponse(builders);
            return null;
        }).when(datafeedConfigProvider).expandDatafeedConfigs(eq("_all"), eq(true), isNull(), any());
    }

    private static DatafeedConfig datafeed(
        String id,
        String projectRouting,
        PersistedCloudCredential credential,
        Map<String, String> headers
    ) {
        return datafeedBuilder(id, projectRouting, credential, headers).build();
    }

    private static DatafeedConfig.Builder datafeedBuilder(
        String id,
        String projectRouting,
        PersistedCloudCredential credential,
        Map<String, String> headers
    ) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(id, "job-" + id).setIndices(List.of("logs-*"));
        if (projectRouting != null) {
            builder.setProjectRouting(projectRouting);
        }
        if (credential != null) {
            builder.setCloudInternalCredential(credential);
        }
        if (headers != null) {
            builder.setHeaders(headers);
        }
        return builder;
    }

    private static long findObservation(Collection<LongWithAttributes> observations, String attributeKey, String attributeValue) {
        return observations.stream()
            .filter(observation -> attributeValue.equals(observation.attributes().get(attributeKey)))
            .mapToLong(LongWithAttributes::value)
            .findFirst()
            .orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private static Supplier<LongWithAttributes> captureLongGauge(MeterRegistry meterRegistry, String metricName) {
        AtomicReference<Supplier<LongWithAttributes>> observer = new AtomicReference<>();
        when(meterRegistry.registerLongGauge(eq(metricName), anyString(), anyString(), any())).thenAnswer(invocation -> {
            observer.set(invocation.getArgument(3));
            return mock(LongGauge.class);
        });
        return () -> observer.get().get();
    }

    @SuppressWarnings("unchecked")
    private static Supplier<Collection<LongWithAttributes>> captureLongsGauge(MeterRegistry meterRegistry, String metricName) {
        AtomicReference<Supplier<Collection<LongWithAttributes>>> observer = new AtomicReference<>();
        when(meterRegistry.registerLongsGauge(eq(metricName), anyString(), anyString(), any())).thenAnswer(invocation -> {
            observer.set(invocation.getArgument(3));
            return mock(LongGauge.class);
        });
        return () -> observer.get().get();
    }
}
