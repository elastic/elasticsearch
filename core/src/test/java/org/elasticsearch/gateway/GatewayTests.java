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
package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;


public class GatewayTests extends ESTestCase {

    public void testCalcRequiredAllocations() {
        MockGateway gateway = new MockGateway(Settings.EMPTY, new TestClusterService());
        int nodeCount = randomIntBetween(1, 6);
        Map<String, Integer> expectedResult = new HashMap<>();
        expectedResult.put("quorum", nodeCount > 2 ? nodeCount / 2 + 1 : 1);
        expectedResult.put("quorum-1", nodeCount > 2 ? (nodeCount + 1) / 2 : 1);
        expectedResult.put("half", expectedResult.get("quorum-1"));
        expectedResult.put("one", 1);
        expectedResult.put("full", nodeCount);
        expectedResult.put("all", nodeCount);
        expectedResult.put("full-1", Math.max(1, nodeCount - 1));
        expectedResult.put("all-1", Math.max(1, nodeCount - 1));
        int i = randomIntBetween(1, 20);
        expectedResult.put("" + i, i);
        expectedResult.put(randomUnicodeOfCodepointLength(10), 1);
        for (String setting : expectedResult.keySet()) {
            assertThat("unexpected result for setting [" + setting + "]", gateway.calcRequiredAllocations(setting, nodeCount), equalTo(expectedResult.get(setting).intValue()));
        }

    }

    static class MockGateway extends Gateway {

        MockGateway(Settings settings, ClusterService clusterService) {
            super(settings, clusterService, null, null, null, ClusterName.DEFAULT);
        }

        @Override
        public int calcRequiredAllocations(String setting, int nodeCount) {
            return super.calcRequiredAllocations(setting, nodeCount);
        }
    }
}
