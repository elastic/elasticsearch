/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction.TaskParams;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.TransportStartDataFrameAnalyticsAction.TaskExecutor;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportStartDataFrameAnalyticsActionTests extends ESTestCase {

    private static final String JOB_ID = "data_frame_id";

    // Cannot assign the node because upgrade mode is enabled
    public void testGetAssignment_UpgradeModeIsEnabled() {
        TaskExecutor executor = createTaskExecutor();
        TaskParams params = new TaskParams(JOB_ID, MlConfigVersion.CURRENT, false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isUpgradeMode(true).build()))
            .build();

        Assignment assignment = executor.getAssignment(params, clusterState.nodes().getAllNodes(), clusterState);
        assertThat(assignment.getExecutorNode(), is(nullValue()));
        assertThat(assignment.getExplanation(), is(equalTo("persistent task cannot be assigned while upgrade mode is enabled.")));
    }

    // Cannot assign the node because there are no existing nodes in the cluster state
    public void testGetAssignment_NoNodes() {
        TaskExecutor executor = createTaskExecutor();
        TaskParams params = new TaskParams(JOB_ID, MlConfigVersion.CURRENT, false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().build()))
            .build();

        Assignment assignment = executor.getAssignment(params, clusterState.nodes().getAllNodes(), clusterState);
        assertThat(assignment.getExecutorNode(), is(nullValue()));
        assertThat(assignment.getExplanation(), is(emptyString()));
    }

    // Cannot assign the node because none of the existing nodes is an ML node
    public void testGetAssignment_NoMlNodes() {
        TaskExecutor executor = createTaskExecutor();
        TaskParams params = new TaskParams(JOB_ID, MlConfigVersion.CURRENT, false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().build()))
            .nodes(
                DiscoveryNodes.builder()
                    .add(createNode(0, false, Version.CURRENT, MlConfigVersion.CURRENT))
                    .add(createNode(1, false, Version.CURRENT, MlConfigVersion.CURRENT))
                    .add(createNode(2, false, Version.CURRENT, MlConfigVersion.CURRENT))
            )
            .build();

        Assignment assignment = executor.getAssignment(params, clusterState.nodes().getAllNodes(), clusterState);
        assertThat(assignment.getExecutorNode(), is(nullValue()));
        assertThat(
            assignment.getExplanation(),
            allOf(
                containsString("Not opening job [data_frame_id] on node [_node_name0]. Reason: This node isn't a machine learning node."),
                containsString("Not opening job [data_frame_id] on node [_node_name1]. Reason: This node isn't a machine learning node."),
                containsString("Not opening job [data_frame_id] on node [_node_name2]. Reason: This node isn't a machine learning node.")
            )
        );
    }

    // The node can be assigned despite being newer than the job.
    // In such a case destination index will be created from scratch so that its mappings are up-to-date.
    public void testGetAssignment_MlNodeIsNewerThanTheMlJobButTheAssignmentSuceeds() {
        TaskExecutor executor = createTaskExecutor();
        TaskParams params = new TaskParams(JOB_ID, MlConfigVersion.V_7_9_0, false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().build()))
            .nodes(DiscoveryNodes.builder().add(createNode(0, true, Version.V_7_10_0, MlConfigVersion.V_7_10_0)))
            .build();

        Assignment assignment = executor.getAssignment(params, clusterState.nodes().getAllNodes(), clusterState);
        assertThat(assignment.getExecutorNode(), is(equalTo("_node_id0")));
        assertThat(assignment.getExplanation(), is(emptyString()));
    }

    private static TaskExecutor createTaskExecutor() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.newHashSet(
                MachineLearning.CONCURRENT_JOB_ALLOCATIONS,
                MachineLearning.MAX_MACHINE_MEMORY_PERCENT,
                MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT,
                MachineLearning.MAX_ML_NODE_SIZE,
                MachineLearning.MAX_LAZY_ML_NODES,
                MachineLearning.MAX_OPEN_JOBS_PER_NODE
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.threadPool()).thenReturn(mock(ThreadPool.class));

        return new TaskExecutor(
            Settings.EMPTY,
            mock(Client.class),
            clusterService,
            mock(DataFrameAnalyticsManager.class),
            mock(DataFrameAnalyticsAuditor.class),
            mock(MlMemoryTracker.class),
            TestIndexNameExpressionResolver.newInstance(),
            mock(XPackLicenseState.class)
        );
    }

    private static DiscoveryNode createNode(int i, boolean isMlNode, Version nodeVersion, MlConfigVersion mlConfigVersion) {
        return DiscoveryNodeUtils.builder("_node_id" + i)
            .name("_node_name" + i)
            .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + i))
            .attributes(
                isMlNode
                    ? Map.of(
                        "ml.machine_memory",
                        String.valueOf(ByteSizeValue.ofGb(1).getBytes()),
                        "ml.max_jvm_size",
                        String.valueOf(ByteSizeValue.ofMb(400).getBytes()),
                        MlConfigVersion.ML_CONFIG_VERSION_NODE_ATTR,
                        mlConfigVersion.toString()
                    )
                    : Map.of()
            )
            .roles(
                isMlNode
                    ? Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)
                    : Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)
            )
            .version(VersionInformation.inferVersions(nodeVersion))
            .build();
    }
}
