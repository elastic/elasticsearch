/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.net.InetAddress;
import java.util.Collections;

import static org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests.addJobTask;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class MlAssignmentNotifierTests extends ESTestCase {

    public void testClusterChanged_info() throws Exception {
        Auditor auditor = mock(Auditor.class);
        ClusterService clusterService = mock(ClusterService.class);
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(auditor, clusterService);
        notifier.onMaster();

        DiscoveryNode node =
                new DiscoveryNode("node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT);
        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE,
                        new PersistentTasksCustomMetaData(0L, Collections.emptyMap())))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", null, tasksBuilder);
        MetaData metaData = MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()).build();
        ClusterState state = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder().add(node))
                .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", state, previous));
        verify(auditor, times(1)).info(eq("job_id"), any());

        notifier.offMaster();
        notifier.clusterChanged(new ClusterChangedEvent("_test", state, previous));
        verifyNoMoreInteractions(auditor);
    }

    public void testClusterChanged_warning() throws Exception {
        Auditor auditor = mock(Auditor.class);
        ClusterService clusterService = mock(ClusterService.class);
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(auditor, clusterService);
        notifier.onMaster();

        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE,
                        new PersistentTasksCustomMetaData(0L, Collections.emptyMap())))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", null, null, tasksBuilder);
        MetaData metaData = MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()).build();
        ClusterState state = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", state, previous));
        verify(auditor, times(1)).warning(eq("job_id"), any());

        notifier.offMaster();
        notifier.clusterChanged(new ClusterChangedEvent("_test", state, previous));
        verifyNoMoreInteractions(auditor);
    }

}
