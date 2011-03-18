/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class ClusterHealthTests extends AbstractNodesTests {

    @AfterMethod public void closeNodes() {
        closeAllNodes();
    }

    @Test public void testHealth() {
        Node node1 = startNode("node1");
        logger.info("--> running cluster health on an index that does not exists");
        ClusterHealthResponse healthResponse = node1.client().admin().cluster().prepareHealth("test").setWaitForYellowStatus().setTimeout("1s").execute().actionGet();
        assertThat(healthResponse.timedOut(), equalTo(true));
        assertThat(healthResponse.status(), equalTo(ClusterHealthStatus.RED));
    }
}