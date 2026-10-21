/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.multiproject.TestOnlyMultiProjectPlugin;
import org.elasticsearch.multiproject.action.PutProjectAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService.AUTOMATION_DISABLED_IMPACT_ID;
import static org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService.BACKUP_DISABLED_IMPACT_ID;
import static org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService.CONTACT_SUPPORT;
import static org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService.INGEST_DISABLED_IMPACT_ID;
import static org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService.NAME;
import static org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService.TROUBLESHOOT_DISCOVERY;
import static org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService.TROUBLESHOOT_UNSTABLE_CLUSTER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for {@link StableMasterHealthIndicatorService} covering green, yellow, and red health report outcomes.
 * Each test randomly runs as single-project or multi-project
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class StableMasterHealthIndicatorServiceIT extends ESIntegTestCase {

    private static final int MAX_NODES = 5;
    private static final int MAX_EXTRA_PROJECTS = 5;

    private static final List<HealthIndicatorImpact> EXPECTED_UNSTABLE_MASTER_IMPACTS = List.of(
        new HealthIndicatorImpact(NAME, INGEST_DISABLED_IMPACT_ID, 1, """
            The cluster cannot create, delete, or rebalance indices, and cannot insert or update documents.""", List.of(ImpactArea.INGEST)),
        new HealthIndicatorImpact(NAME, AUTOMATION_DISABLED_IMPACT_ID, 1, """
            Scheduled tasks such as Watcher, Index Lifecycle Management, and Snapshot Lifecycle Management will not work. \
            The _cat APIs will not work.""", List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)),
        new HealthIndicatorImpact(
            NAME,
            BACKUP_DISABLED_IMPACT_ID,
            3,
            """
                Snapshot and restore will not work. Your data will not be backed up, and searchable snapshots cannot be mounted.""",
            List.of(ImpactArea.BACKUP)
        )
    );

    private static final List<Diagnosis> EXPECTED_UNSTABLE_MASTER_DIAGNOSES = List.of(
        TROUBLESHOOT_DISCOVERY,
        TROUBLESHOOT_UNSTABLE_CLUSTER,
        CONTACT_SUPPORT
    );

    private boolean multiProject;

    @Before
    private void setUpTestCluster() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        multiProject = randomBoolean();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(MockTransportService.TestPlugin.class);
        if (multiProject) {
            plugins.add(TestOnlyMultiProjectPlugin.class);
        }
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (multiProject) {
            builder.put(TestOnlyMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true);
        }
        return builder.build();
    }

    @Override
    protected boolean multiProjectIntegrationTest() {
        // Enables multi-project cluster-state XContent params so teardown consistency checks don't hit MultiProjectPendingException.
        return multiProject;
    }

    public void testGreen() throws Exception {
        final int nodeCount = randomIntBetween(1, MAX_NODES);
        internalCluster().startNodes(nodeCount);
        ensureStableCluster(nodeCount);
        maybeCreateExtraProjects();

        assertMasterStability(internalCluster().client(), HealthStatus.GREEN, containsString("The cluster has a stable master node"));
    }

    /// Tests that the `master_is_stable` indicator returns yellow when the number of master identity changes exceeds
    /// {@link CoordinationDiagnosticsService#IDENTITY_CHANGES_THRESHOLD_SETTING}.
    ///
    /// Note that I have to emulate this via a network partition, isolating the elected master and then healing the partition so
    /// it can rejoin. If I were to simply kill the master node, it would no longer belong in the cluster, and so its ID
    /// would be filtered from the master history log. The health of the cluster would stay green, as master nodes dropping from the
    /// cluster does not count as instability
    public void testYellowWhenExceedsIdentityChangesThreshold() throws Exception {
        Settings settings = Settings.builder()
            .put(CoordinationDiagnosticsService.IDENTITY_CHANGES_THRESHOLD_SETTING.getKey(), 1)
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .build();

        // Need at least two master-eligible nodes so a new master can be elected while the old one is isolated.
        final int nodeCount = randomIntBetween(3, MAX_NODES);
        internalCluster().startNodes(nodeCount, settings);
        ensureStableCluster(nodeCount);
        maybeCreateExtraProjects();

        final String originalMaster = internalCluster().getMasterName();
        final Set<String> otherNodes = new HashSet<>(Set.of(internalCluster().getNodeNames()));
        otherNodes.remove(originalMaster);

        final NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Set.of(originalMaster), otherNodes),
            NetworkDisruption.DISCONNECT
        );
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        // Majority side elects a new master while the original master is isolated.
        ensureStableCluster(otherNodes.size(), randomFrom(otherNodes));
        awaitMasterNotFound(originalMaster);

        // Heal so the original master rejoins with the same ephemeral ID and counts toward identity-change detection.
        networkDisruption.stopDisrupting();
        ensureStableCluster(nodeCount);

        assertMasterStability(
            internalCluster().client(randomFrom(internalCluster().getNodeNames())),
            HealthStatus.YELLOW,
            containsString("The elected master node has changed")
        );
    }

    /// Tests that the `master_is_stable` indicator returns yellow when the number of master transitions exceeds
    /// {@link CoordinationDiagnosticsService#NO_MASTER_TRANSITIONS_THRESHOLD_SETTING}.
    ///
    /// Isolates the elected master. The majority elects a replacement (master → null → new master). The cluster is
    /// then healed. With no_master_transitions_threshold=1 that is enough for yellow via null-flapping.
    public void testYellowWhenExceedsNoMasterTransitionsThreshold() throws Exception {
        Settings settings = Settings.builder()
            .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1)
            // Keep this high to test only the NO_MASTER_TRANSITIONS_THRESHOLD_SETTING
            .put(CoordinationDiagnosticsService.IDENTITY_CHANGES_THRESHOLD_SETTING.getKey(), 100)
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .build();

        final int nodeCount = randomIntBetween(3, MAX_NODES);
        internalCluster().startNodes(nodeCount, settings);
        ensureStableCluster(nodeCount);
        maybeCreateExtraProjects();

        final String originalMaster = internalCluster().getMasterName();
        final Set<String> otherNodes = new HashSet<>(Set.of(internalCluster().getNodeNames()));
        otherNodes.remove(originalMaster);

        final NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Set.of(originalMaster), otherNodes),
            NetworkDisruption.DISCONNECT
        );
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        ensureStableCluster(otherNodes.size(), randomFrom(otherNodes));
        awaitMasterNotFound(originalMaster);

        networkDisruption.stopDisrupting();
        ensureStableCluster(nodeCount);

        assertMasterStability(
            internalCluster().client(internalCluster().getMasterName()),
            HealthStatus.YELLOW,
            containsString("no master multiple times")
        );
    }

    /// Have exactly one master-eligible node so stopping it leaves none
    public void testRedWhenNoMasterEligibleNodes() throws Exception {
        internalCluster().startMasterOnlyNodes(
            1,
            Settings.builder().put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1).build()
        );
        final int dataNodeCount = randomIntBetween(1, MAX_NODES - 1);
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(
            dataNodeCount,
            Settings.builder()
                .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1)
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
                .put(CoordinationDiagnosticsService.NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.getKey(), new TimeValue(1, TimeUnit.SECONDS))
                .build()
        );
        ensureStableCluster(1 + dataNodeCount);
        maybeCreateExtraProjects();
        internalCluster().stopCurrentMasterNode();

        assertMasterStability(
            internalCluster().client(randomFrom(dataNodes)),
            HealthStatus.RED,
            containsString("No master eligible nodes found in the cluster")
        );
        // Stop remaining nodes so suite teardown does not try to wipe indices with no master.
        for (String dataNode : dataNodes) {
            internalCluster().stopNode(dataNode);
        }
    }

    private void maybeCreateExtraProjects() {
        if (multiProject == false) {
            return;
        }
        final int extraProjects = randomIntBetween(1, MAX_EXTRA_PROJECTS);
        for (int i = 0; i < extraProjects; i++) {
            client().execute(
                PutProjectAction.INSTANCE,
                new PutProjectAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, randomUniqueProjectId())
            ).actionGet();
        }
    }

    private void assertMasterStability(Client client, HealthStatus expectedStatus, Matcher<String> expectedSymptom) throws Exception {
        assertBusy(() -> {
            GetHealthAction.Response healthResponse = client.execute(
                GetHealthAction.INSTANCE,
                new GetHealthAction.Request(NAME, true, 1000)
            ).get();
            HealthIndicatorResult indicator = healthResponse.findIndicator(NAME);
            assertThat(indicator.status(), equalTo(expectedStatus));
            assertThat(indicator.symptom(), expectedSymptom);
            if (expectedStatus.indicatesHealthProblem()) {
                assertThat(indicator.impacts(), equalTo(EXPECTED_UNSTABLE_MASTER_IMPACTS));
                assertThat(indicator.diagnosisList(), equalTo(EXPECTED_UNSTABLE_MASTER_DIAGNOSES));
            } else {
                assertThat(indicator.impacts(), empty());
                assertThat(indicator.diagnosisList(), empty());
            }
        });
    }
}
