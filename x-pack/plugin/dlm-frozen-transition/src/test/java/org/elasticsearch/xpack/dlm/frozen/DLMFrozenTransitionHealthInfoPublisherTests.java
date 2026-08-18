/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo;
import org.elasticsearch.health.node.StalledIndices;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DLMFrozenTransitionHealthInfoPublisherTests extends ESTestCase {

    // One concurrent thread so a single blocking filler is sufficient to saturate the pool and force the
    // test's target task into QUEUED status.
    private static final int TEST_MAX_CONCURRENCY = 1;
    private static final int TEST_MAX_QUEUE_SIZE = 5;

    private final AtomicLong now = new AtomicLong();
    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private DLMFrozenTransitionSettings transitionSettings;
    private DLMFrozenTransitionExecutor transitionExecutor;
    private DLMFrozenTransitionService transitionService;
    private DataStreamLifecycleErrorStore errorStore;
    private CopyOnWriteArrayList<UpdateHealthInfoCacheAction.Request> clientSeenRequests;
    private DLMFrozenTransitionHealthInfoPublisher publisher;

    private final DiscoveryNode node1 = DiscoveryNodeUtils.builder("node_1")
        .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        .build();
    private final DiscoveryNode node2 = DiscoveryNodeUtils.builder("node_2")
        .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        .build();
    private final DiscoveryNode[] allNodes = new DiscoveryNode[] { node1, node2 };

    @Before
    public void setupServices() {
        setupServices(Settings.EMPTY);
    }

    private void setupServices(Settings nodeSettings) {
        now.set(System.currentTimeMillis());
        Set<Setting<?>> settingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.addAll(DLMFrozenTransitionSettings.ALL_SETTINGS);
        settingSet.add(DLMFrozenTransitionService.POLL_INTERVAL_SETTING);
        settingSet.add(DLMFrozenTransitionHealthInfoPublisher.PUBLISH_INTERVAL_SETTING);

        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                DLMFrozenTransitionPlugin.EXECUTOR_NAME,
                TEST_MAX_CONCURRENCY,
                TEST_MAX_QUEUE_SIZE,
                "dlm.frozen.transition.thread_pool",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
        clusterService = createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("node", "node"),
            nodeSettings,
            new ClusterSettings(nodeSettings, settingSet)
        );
        transitionSettings = DLMFrozenTransitionSettings.create(clusterService);
        errorStore = new DataStreamLifecycleErrorStore(now::get);
        transitionExecutor = new DLMFrozenTransitionExecutor(
            clusterService,
            TEST_MAX_CONCURRENCY + TEST_MAX_QUEUE_SIZE,
            transitionSettings,
            errorStore,
            threadPool.executor(DLMFrozenTransitionPlugin.EXECUTOR_NAME)
        );
        transitionService = new DLMFrozenTransitionService(
            clusterService,
            (indexName, pid) -> null,
            transitionExecutor,
            transitionSettings
        );
        clientSeenRequests = new CopyOnWriteArrayList<>();
        Client client = getTransportRequestsRecordingClient();
        publisher = new DLMFrozenTransitionHealthInfoPublisher(
            clusterService,
            client,
            transitionService,
            transitionExecutor,
            transitionSettings,
            now::get,
            0
        );
    }

    @After
    public void cleanup() throws Exception {
        publisher.close();
        transitionService.close();
        clusterService.close();
        terminate(threadPool);
    }

    public void testTransitionsEnabledAndServiceNotRunningByDefault() {
        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.transitionsEnabled(), is(true));
        assertThat(info.serviceRunning(), is(false));
    }

    public void testServiceRunningReflectsMasterElection() {
        transitionService.clusterChanged(createMasterEvent(true));
        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.serviceRunning(), is(true));

        transitionService.clusterChanged(createMasterEvent(false));
        info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.serviceRunning(), is(false));
    }

    public void testTransitionsDisabledReflectsSetting() {
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(DLMFrozenTransitionSettings.TRANSITION_ENABLED_SETTING.getKey(), false).build());
        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.transitionsEnabled(), is(false));
    }

    public void testDefaultRepositoryConfiguredViaDynamicClusterSetting() {
        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.defaultRepositoryConfigured(), is(false));

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), "my-repo").build());
        info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.defaultRepositoryConfigured(), is(true));
    }

    public void testDefaultRepositoryConfiguredViaNodeSettings() throws Exception {
        // Exercises the bug where reading from state.metadata().settings() missed repositories
        // configured in elasticsearch.yml (node settings). ClusterSettings.get() falls back to
        // node settings when no dynamic cluster update is in effect.
        cleanup();
        setupServices(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), "node-repo").build());

        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.defaultRepositoryConfigured(), is(true));
    }

    public void testMarkedIndicesCount() {
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        addDataStreamWithFrozenLifecycle(projectBuilder, "ds-1", oldIndexTime(), true, TimeValue.timeValueDays(30));
        addDataStreamWithFrozenLifecycle(projectBuilder, "ds-2", oldIndexTime(), true, TimeValue.timeValueDays(30));
        setProjectState(projectBuilder);

        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.markedIndicesCount(), is(2));
    }

    public void testEligibleUnmarkedIndicesReportedWhenStuckLongerThanThreshold() {
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        String oldIndexName = addDataStreamWithFrozenLifecycle(
            projectBuilder,
            "eligible-ds",
            oldIndexTime(),
            false,
            TimeValue.timeValueDays(30)
        );
        setProjectState(projectBuilder);

        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.eligibleUnmarked().totalCount(), is(1));
        assertThat(info.eligibleUnmarked().sample().stream().map(i -> i.indexName()).toList(), containsInAnyOrder(oldIndexName));
        assertThat(info.notStartedMarked(), is(StalledIndices.EMPTY));
        assertThat(info.queuedMarked(), is(StalledIndices.EMPTY));
    }

    public void testEligibleUnmarkedIndexNotYetPastThresholdIsNotReported() {
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        // eligible since (now - 1h) + frozenAfter is far less than the 24h default stuck threshold: barely eligible, not yet stuck
        long recentEligibleTime = now.get() - TimeValue.timeValueHours(1).millis();
        addDataStreamWithFrozenLifecycle(projectBuilder, "fresh-ds", recentEligibleTime, false, TimeValue.timeValueSeconds(1));
        setProjectState(projectBuilder);

        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.eligibleUnmarked().totalCount(), is(0));
    }

    public void testMarkedIndexNotStalledWhenTransitionIsRunning() throws Exception {
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        String markedIndexName = addDataStreamWithFrozenLifecycle(
            projectBuilder,
            "running-ds",
            oldIndexTime(),
            true,
            TimeValue.timeValueDays(30)
        );
        setProjectState(projectBuilder);

        var task = new DLMFrozenTransitionExecutorTestCase.TestDLMFrozenTransitionRunnable(markedIndexName, projectId);
        task.blockUntil = new CountDownLatch(1);
        try {
            transitionExecutor.submit(task);
            safeAwait(task.started);

            DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
            assertThat(info.notStartedMarked(), is(StalledIndices.EMPTY));
            assertThat(info.queuedMarked(), is(StalledIndices.EMPTY));
        } finally {
            task.blockUntil.countDown();
        }
    }

    public void testMarkedIndexReportedAsQueuedWhenTransitionIsQueued() throws Exception {
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        String markedIndexName = addDataStreamWithFrozenLifecycle(
            projectBuilder,
            "queued-ds",
            oldIndexTime(),
            true,
            TimeValue.timeValueDays(30)
        );
        setProjectState(projectBuilder);

        // Saturate the single thread (TEST_MAX_CONCURRENCY == 1) with a blocking filler task so the
        // target task lands in the queue (status QUEUED) rather than running immediately.
        CountDownLatch fillerRelease = new CountDownLatch(1);
        var filler = new DLMFrozenTransitionExecutorTestCase.TestDLMFrozenTransitionRunnable("filler", projectId);
        filler.blockUntil = fillerRelease;
        try {
            transitionExecutor.submit(filler);
            safeAwait(filler.started);

            var target = new DLMFrozenTransitionExecutorTestCase.TestDLMFrozenTransitionRunnable(markedIndexName, projectId);
            transitionExecutor.submit(target);

            DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
            assertThat(info.notStartedMarked(), is(StalledIndices.EMPTY));
            assertThat(info.queuedMarked().totalCount(), is(1));
            assertThat(info.queuedMarked().sample().stream().map(i -> i.indexName()).toList(), containsInAnyOrder(markedIndexName));
        } finally {
            fillerRelease.countDown();
        }
    }

    public void testErroringMarkedIndexIsReportedAsStalled() {
        // Recording an error against a marked index must not suppress the stall check; the index
        // should appear in notStartedMarked once the stall threshold elapses.
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        String markedIndexName = addDataStreamWithFrozenLifecycle(
            projectBuilder,
            "erroring-ds",
            oldIndexTime(),
            true,
            TimeValue.timeValueDays(30)
        );
        setProjectState(projectBuilder);
        errorStore.recordError(projectId, markedIndexName, new RuntimeException("some failure"));

        // Advance past the 24-hour stall threshold so the index is eligible for stall reporting.
        now.addAndGet(TimeValue.timeValueHours(25).millis());

        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.notStartedMarked().totalCount(), is(1));
        assertThat(info.queuedMarked(), is(StalledIndices.EMPTY));
    }

    public void testMasterTenureGracePeriodSuppressesStallReporting() {
        // With masterTenureStartMillis == 0 (node has never been master), a marked old index is reported as stalled.
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        addDataStreamWithFrozenLifecycle(projectBuilder, "grace-ds", oldIndexTime(), true, TimeValue.timeValueDays(30));
        setProjectState(projectBuilder);

        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.notStartedMarked().totalCount(), is(1));

        // When this node becomes master, onStart() records masterTenureStartMillis = now, resetting the stall clock.
        publisher.clusterChanged(createMasterEvent(true));
        info = publisher.buildHealthInfo(clusterService.state());
        assertThat(
            "freshly-elected master should not report stalled indices within the grace period",
            info.notStartedMarked().totalCount(),
            is(0)
        );

        // Advance the clock past the 24-hour default stuck threshold from the tenure start.
        now.addAndGet(TimeValue.timeValueHours(25).millis());
        info = publisher.buildHealthInfo(clusterService.state());
        assertThat("stall should be reported once the threshold elapses from tenure start", info.notStartedMarked().totalCount(), is(1));
    }

    public void testMaxIndicesToPublishCapIsEnforced() {
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        int overLimit = DLMFrozenTransitionHealthInfoPublisher.MAX_INDICES_TO_PUBLISH + 1;
        for (int i = 0; i < overLimit; i++) {
            addDataStreamWithFrozenLifecycle(projectBuilder, "cap-ds-" + i, oldIndexTime(), false, TimeValue.timeValueDays(30));
        }
        setProjectState(projectBuilder);

        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(info.eligibleUnmarked().totalCount(), is(overLimit));
        assertThat(info.eligibleUnmarked().sample(), hasSize(DLMFrozenTransitionHealthInfoPublisher.MAX_INDICES_TO_PUBLISH));
    }

    public void testCompletedTransitionsAreSkipped() {
        // A completed frozen transition index has DLM_CREATED_SETTING=true AND a searchable-snapshot store type.
        Settings completedSettings = settings(IndexVersion.current()).put(DataStreamLifecycleService.DLM_CREATED_SETTING_KEY, true)
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "snapshot")
            .build();
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        addDataStreamWithFrozenLifecycle(
            projectBuilder,
            "completed-ds",
            oldIndexTime(),
            false,
            TimeValue.timeValueDays(30),
            completedSettings
        );
        setProjectState(projectBuilder);

        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());
        assertThat(
            "completed frozen-transition index should be excluded from eligible-unmarked count",
            info.eligibleUnmarked().totalCount(),
            is(0)
        );
        assertThat(
            "completed frozen-transition index should be excluded from not-started-marked count",
            info.notStartedMarked().totalCount(),
            is(0)
        );
        assertThat(
            "completed frozen-transition index should be excluded from queued-marked count",
            info.queuedMarked().totalCount(),
            is(0)
        );
    }

    public void testMultipleProjectsAggregateCorrectly() {
        ProjectId projectId1 = randomProjectIdOrDefault();
        ProjectId projectId2 = randomValueOtherThan(projectId1, ESTestCase::randomProjectIdOrDefault);

        ProjectMetadata.Builder builder1 = ProjectMetadata.builder(projectId1);
        String indexName1 = addDataStreamWithFrozenLifecycle(builder1, "ds-project1", oldIndexTime(), false, TimeValue.timeValueDays(30));
        setProjectState(builder1);

        ProjectMetadata.Builder builder2 = ProjectMetadata.builder(projectId2);
        String indexName2 = addDataStreamWithFrozenLifecycle(builder2, "ds-project2", oldIndexTime(), false, TimeValue.timeValueDays(30));
        setProjectState(builder2);

        DlmFrozenTransitionsHealthInfo info = publisher.buildHealthInfo(clusterService.state());

        assertThat(info.eligibleUnmarked().totalCount(), is(2));
        assertThat(info.eligibleUnmarked().sample().stream().map(i -> i.indexName()).toList(), containsInAnyOrder(indexName1, indexName2));
    }

    public void testPublishHealthInfoSendsRequestToHealthNode() {
        ClusterState stateWithHealthNode = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        setState(clusterService, stateWithHealthNode);

        publisher.publishHealthInfo();

        assertThat(clientSeenRequests.size(), is(1));
        assertThat(clientSeenRequests.get(0).getDlmFrozenTransitionsHealthInfo(), is(notNullValue()));
    }

    public void testPublishHealthInfoNoHealthNode() {
        ClusterState stateNoHealthNode = ClusterStateCreationUtils.state(node1, node1, null, allNodes);
        setState(clusterService, stateNoHealthNode);

        publisher.publishHealthInfo();

        assertThat(clientSeenRequests.size(), is(0));
    }

    private long oldIndexTime() {
        return now.get() - TimeValue.timeValueDays(100).millis();
    }

    /**
     * Builds a 2-index data stream (an old non-write backing index plus a fresh write index) with a
     * {@code frozen_after} lifecycle, optionally marking the old index as a frozen-conversion candidate.
     *
     * @return the name of the old (non-write) index
     */
    private String addDataStreamWithFrozenLifecycle(
        ProjectMetadata.Builder projectBuilder,
        String dataStreamName,
        long oldIndexCreationDate,
        boolean marked,
        TimeValue frozenAfter
    ) {
        return addDataStreamWithFrozenLifecycle(
            projectBuilder,
            dataStreamName,
            oldIndexCreationDate,
            marked,
            frozenAfter,
            settings(IndexVersion.current()).build()
        );
    }

    private String addDataStreamWithFrozenLifecycle(
        ProjectMetadata.Builder projectBuilder,
        String dataStreamName,
        long oldIndexCreationDate,
        boolean marked,
        TimeValue frozenAfter,
        Settings oldIndexSettings
    ) {
        IndexMetadata.Builder oldIndexBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(oldIndexSettings)
            .creationDate(oldIndexCreationDate)
            .numberOfShards(1)
            .numberOfReplicas(0);
        if (marked) {
            oldIndexBuilder.putCustom(
                DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, "my-repo")
            );
        }
        IndexMetadata oldIndex = oldIndexBuilder.build();

        IndexMetadata writeIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        projectBuilder.put(oldIndex, true);
        projectBuilder.put(writeIndex, true);
        projectBuilder.put(
            DataStream.builder(dataStreamName, List.of(oldIndex.getIndex(), writeIndex.getIndex()))
                .setGeneration(2)
                .setLifecycle(DataStreamLifecycle.dataLifecycleBuilder().frozenAfter(frozenAfter).build())
                .build()
        );
        return oldIndex.getIndex().getName();
    }

    private void setProjectState(ProjectMetadata.Builder projectBuilder) {
        setState(clusterService, ClusterState.builder(clusterService.state()).putProjectMetadata(projectBuilder).build());
    }

    private ClusterChangedEvent createMasterEvent(boolean isMaster) {
        DiscoveryNode localNode = clusterService.localNode();
        DiscoveryNode otherNode = DiscoveryNodeUtils.create("other-node", "other-node");

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterService.state().nodes()).add(otherNode);
        nodesBuilder.masterNodeId(isMaster ? localNode.getId() : otherNode.getId());

        ClusterState newState = ClusterState.builder(clusterService.state())
            .nodes(nodesBuilder)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        ClusterState previousState = ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder(clusterService.state().nodes())
                    .add(otherNode)
                    .masterNodeId(isMaster ? otherNode.getId() : localNode.getId())
            )
            .build();

        return new ClusterChangedEvent("test", newState, previousState);
    }

    private Client getTransportRequestsRecordingClient() {
        return new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                clientSeenRequests.add((UpdateHealthInfoCacheAction.Request) request);
                listener.onResponse(null);
            }
        };
    }
}
