/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.net.InetAddress;
import java.util.Collections;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TransportMlInfoActionTests extends ESTestCase {

    public void testCalculateEffectiveMaxModelMemoryLimit() {

        int mlMemoryPercent = randomIntBetween(5, 90);
        long highestMlMachineMemory = -1;

        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = randomIntBetween(1, 10); i > 0; --i) {
            String nodeName = "_node_name" + i;
            String nodeId = "_node_id" + i;
            TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + i);
            if (randomBoolean()) {
                // Not an ML node
                builder.add(new DiscoveryNode(nodeName, nodeId, ta, Collections.emptyMap(), Collections.emptySet(), Version.CURRENT));
            } else {
                // ML node
                long machineMemory = randomLongBetween(2000000000L, 100000000000L);
                highestMlMachineMemory = Math.max(machineMemory, highestMlMachineMemory);
                builder.add(new DiscoveryNode(nodeName, nodeId, ta,
                    Collections.singletonMap(MachineLearning.MACHINE_MEMORY_NODE_ATTR, String.valueOf(machineMemory)),
                    Collections.emptySet(), Version.CURRENT));
            }
        }
        DiscoveryNodes nodes = builder.build();

        ByteSizeValue effectiveMaxModelMemoryLimit =
            TransportMlInfoAction.calculateEffectiveMaxModelMemoryLimit(mlMemoryPercent, nodes);

        if (highestMlMachineMemory < 0) {
            assertThat(effectiveMaxModelMemoryLimit, nullValue());
        } else {
            assertThat(effectiveMaxModelMemoryLimit, notNullValue());
            assertThat(effectiveMaxModelMemoryLimit.getBytes()
                    + Math.max(Job.PROCESS_MEMORY_OVERHEAD.getBytes(), DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes())
                    + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                lessThanOrEqualTo(highestMlMachineMemory * mlMemoryPercent / 100));
        }
    }
}
