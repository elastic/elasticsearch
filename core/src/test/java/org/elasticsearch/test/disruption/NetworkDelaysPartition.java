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

public class NetworkDelaysPartition extends NetworkPartition {

    static long DEFAULT_DELAY_MIN = 10000;
    static long DEFAULT_DELAY_MAX = 90000;


    final long delayMin;
    final long delayMax;

    TimeValue duration;

    public NetworkDelaysPartition(Random random) {
        this(random, DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX);
    }

    public NetworkDelaysPartition(Random random, long delayMin, long delayMax) {
        super(random);
        this.delayMin = delayMin;
        this.delayMax = delayMax;
    }

    public NetworkDelaysPartition(String node1, String node2, Random random) {
        this(node1, node2, DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX, random);
    }

    public NetworkDelaysPartition(String node1, String node2, long delayMin, long delayMax, Random random) {
        super(node1, node2, random);
        this.delayMin = delayMin;
        this.delayMax = delayMax;
    }

    public NetworkDelaysPartition(Set<String> nodesSideOne, Set<String> nodesSideTwo, Random random) {
        this(nodesSideOne, nodesSideTwo, DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX, random);
    }

    public NetworkDelaysPartition(Set<String> nodesSideOne, Set<String> nodesSideTwo, long delayMin, long delayMax, Random random) {
        super(nodesSideOne, nodesSideTwo, random);
        this.delayMin = delayMin;
        this.delayMax = delayMax;

    }

    @Override
    public synchronized void startDisrupting() {
        duration = new TimeValue(delayMin + random.nextInt((int) (delayMax - delayMin)));
        super.startDisrupting();
    }

    @Override
    void applyDisruption(DiscoveryNode node1, MockTransportService transportService1,
                         DiscoveryNode node2, MockTransportService transportService2) {
        transportService1.addUnresponsiveRule(node1, duration);
        transportService1.addUnresponsiveRule(node2, duration);
    }

    @Override
    protected String getPartitionDescription() {
        return "network delays for [" + duration + "]";
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return TimeValue.timeValueMillis(delayMax);
    }
}
