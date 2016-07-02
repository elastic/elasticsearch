/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.disruption;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Random;

import static org.elasticsearch.test.ESTestCase.randomFrom;

/**
 * A partition that breaks the cluster into two groups of nodes. The two groups are fully isolated
 * with the exception of a single node that can see and be seen by all nodes in both groups.
 */
public class BridgePartition extends NetworkPartition {

    String bridgeNode;
    final boolean unresponsive;

    public BridgePartition(Random random, boolean unresponsive) {
        super(random);
        this.unresponsive = unresponsive;
    }

    @Override
    public void applyToCluster(InternalTestCluster cluster) {
        bridgeNode = randomFrom(random, cluster.getNodeNames());
        this.cluster = cluster;
        for (String node: cluster.getNodeNames()) {
            if (node.equals(bridgeNode) == false) {
                super.applyToNode(node, cluster);
            }
        }
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return TimeValue.timeValueSeconds(0);
    }

    @Override
    void applyDisruption(MockTransportService transportService1, MockTransportService transportService2) {
        if (unresponsive) {
            transportService1.addUnresponsiveRule(transportService2);
            transportService2.addUnresponsiveRule(transportService1);
        } else {
            transportService1.addFailToSendNoConnectRule(transportService2);
            transportService2.addFailToSendNoConnectRule(transportService1);
        }
    }

    @Override
    protected String getPartitionDescription() {
        return "bridge (super connected node: [" + bridgeNode + "], unresponsive [" + unresponsive + "])";
    }
}
