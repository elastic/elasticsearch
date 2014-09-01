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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Random;
import java.util.Set;

public class NetworkDisconnectPartition extends NetworkPartition {


    public NetworkDisconnectPartition(Random random) {
        super(random);
    }

    public NetworkDisconnectPartition(String node1, String node2, Random random) {
        super(node1, node2, random);
    }

    public NetworkDisconnectPartition(Set<String> nodesSideOne, Set<String> nodesSideTwo, Random random) {
        super(nodesSideOne, nodesSideTwo, random);
    }

    @Override
    protected String getPartitionDescription() {
        return "disconnected";
    }

    @Override
    void applyDisruption(DiscoveryNode node1, MockTransportService transportService1,
                         DiscoveryNode node2, MockTransportService transportService2) {
        transportService1.addFailToSendNoConnectRule(node2);
        transportService2.addFailToSendNoConnectRule(node1);
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return TimeValue.timeValueSeconds(0);
    }
}
