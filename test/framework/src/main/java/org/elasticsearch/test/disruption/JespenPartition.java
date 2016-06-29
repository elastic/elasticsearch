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

import org.elasticsearch.test.InternalTestCluster;

import java.util.Random;

import static org.elasticsearch.test.ESTestCase.randomFrom;

public class JespenPartition extends NetworkUnresponsivePartition {

    String supperConnectedNode;

    public JespenPartition(Random random) {
        super(random);
    }

    @Override
    public void applyToCluster(InternalTestCluster cluster) {
        supperConnectedNode = randomFrom(random, cluster.getNodeNames());
        for (String node: cluster.getNodeNames()) {
            if (node.equals(supperConnectedNode) == false) {
                super.applyToNode(node, cluster);
            }
        }
    }



    @Override
    protected String getPartitionDescription() {
        return "jepsen (super connected node: [" + supperConnectedNode + "]";
    }
}
