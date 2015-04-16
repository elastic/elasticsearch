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

package org.elasticsearch.rest.action.admin.indices.upgrade;

import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.bwcompat.StaticIndexBackwardCompatibilityTest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;

import java.net.InetSocketAddress;

public class UpgradeReallyOldIndexTest extends StaticIndexBackwardCompatibilityTest {

    public void testUpgrade_0_20() throws Exception {
        String indexName = "test";
        loadIndex("index-0.20.zip", indexName, InternalNode.HTTP_ENABLED, true);
        
        assertTrue(UpgradeTest.hasAncientSegments(httpClient(), indexName));
        UpgradeTest.assertNotUpgraded(httpClient(), indexName);
        UpgradeTest.runUpgrade(httpClient(), indexName, "wait_for_completion", "true", "only_ancient_segments", "true");
        assertFalse(UpgradeTest.hasAncientSegments(httpClient(), indexName));

        // This index has entirely ancient segments so the whole index should now be upgraded:
        UpgradeTest.assertUpgraded(httpClient(), indexName);
    }

    public void testUpgradeMixed_0_20_6_and_0_90_6() throws Exception {
        String indexName = "index-0.20.6-and-0.90.6";
        loadIndex(indexName + ".zip", indexName, InternalNode.HTTP_ENABLED, true);

        // Has ancient segments?:
        assertTrue(UpgradeTest.hasAncientSegments(httpClient(), indexName));

        // Also has "merely old" segments?:
        assertTrue(UpgradeTest.hasOldButNotAncientSegments(httpClient(), indexName));

        // Needs upgrading?
        UpgradeTest.assertNotUpgraded(httpClient(), indexName);
        
        // Now upgrade only the ancient ones:
        UpgradeTest.runUpgrade(httpClient(), indexName, "wait_for_completion", "true", "only_ancient_segments", "true");

        // No more ancient ones?
        assertFalse(UpgradeTest.hasAncientSegments(httpClient(), indexName));

        // We succeeded in upgrading only the ancient segments but leaving the "merely old" ones untouched:
        assertTrue(UpgradeTest.hasOldButNotAncientSegments(httpClient(), indexName));
    }
}
