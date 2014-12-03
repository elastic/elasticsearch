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
        loadIndex("index-0.20.zip", InternalNode.HTTP_ENABLED, true);
        
        HttpRequestBuilder httpClient = httpClient();
        UpgradeTest.assertNotUpgraded(httpClient, "test");
        UpgradeTest.runUpgrade(httpClient, "test", "wait_for_completion", "true");
        UpgradeTest.assertUpgraded(httpClient, "test");
    }

}
