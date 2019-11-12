/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlDailyMaintenanceServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private Client client;
    private ClusterService clusterService;
    private MlAssignmentNotifier mlAssignmentNotifier;

    @Before
    public void setUpTests() {
        threadPool = new TestThreadPool("MlDailyMaintenanceServiceTests");
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        clusterService = mock(ClusterService.class);
        ClusterState state = ClusterState.builder(new ClusterName("MlDailyMaintenanceServiceTests"))
            .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, PersistentTasksCustomMetaData.builder().build()))
            .nodes(DiscoveryNodes.builder().build())
            .build();
        when(clusterService.state()).thenReturn(state);
        mlAssignmentNotifier = mock(MlAssignmentNotifier.class);
    }

    @After
    public void stop() {
        terminate(threadPool);
    }

    public void testScheduledTriggering() throws InterruptedException {
        int triggerCount = randomIntBetween(2, 4);
        CountDownLatch latch = new CountDownLatch(triggerCount);
        try (MlDailyMaintenanceService service = createService(latch, client)) {
            service.start();
            latch.await(5, TimeUnit.SECONDS);
        }

        verify(client, Mockito.atLeast(triggerCount - 1)).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier, Mockito.atLeast(triggerCount - 1)).auditUnassignedMlTasks(any(), any());
    }

    private MlDailyMaintenanceService createService(CountDownLatch latch, Client client) {
        return new MlDailyMaintenanceService(threadPool, client, clusterService, mlAssignmentNotifier, () -> {
                latch.countDown();
                return TimeValue.timeValueMillis(100);
            });
    }
}
