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

package org.elasticsearch.discovery.azure;

import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.TEST,
        numDataNodes = 2,
        transportClientRatio = 0.0)
public class AzureTwoStartedNodesTest extends AbstractAzureComputeServiceTest {

    public AzureTwoStartedNodesTest() {
        super(AzureComputeServiceTwoNodesMock.class);
    }

    @Test
    public void two_nodes_should_run() {
        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }
}
