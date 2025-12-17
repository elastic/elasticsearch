/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

import static co.elastic.elasticsearch.stateless.autoscaling.DesiredClusterTopologyTestUtils.randomDesiredClusterTopology;

public class DesiredToplogyContextTests extends ESTestCase {

    public void testListenerCalledWhenTopologyPopulated() {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(new TestThreadPool(getTestName()));
        DesiredTopologyContext context = new DesiredTopologyContext(clusterService);

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        DesiredTopologyListener listener = topology -> { listenerCalled.set(true); };

        context.addListener(listener);
        assertNull(context.getDesiredClusterTopology());
        context.updateDesiredClusterTopology(randomDesiredClusterTopology());
        assertTrue("Listener should be called when topology transitions from null to populated", listenerCalled.get());

        listenerCalled.set(false);
        context.updateDesiredClusterTopology(randomDesiredClusterTopology());
        assertFalse("Listener should NOT be called when topology is updated after already being populated", listenerCalled.get());
    }

    public void testOnMasterClearsStaleTopology() {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(new TestThreadPool(getTestName()));
        DesiredTopologyContext context = new DesiredTopologyContext(clusterService);

        context.updateDesiredClusterTopology(randomDesiredClusterTopology());
        assertNotNull("Topology should be set", context.getDesiredClusterTopology());

        context.onMaster();
        assertNull("Topology should be cleared when node becomes master", context.getDesiredClusterTopology());
    }

    public void testOffMasterClearsTopology() {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(new TestThreadPool(getTestName()));
        DesiredTopologyContext context = new DesiredTopologyContext(clusterService);

        context.updateDesiredClusterTopology(randomDesiredClusterTopology());
        assertNotNull("Topology should be set", context.getDesiredClusterTopology());

        context.offMaster();
        assertNull("Topology should be cleared when node goes off master", context.getDesiredClusterTopology());
    }
}
