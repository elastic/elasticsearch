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

package org.elasticsearch.discovery.zen;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

/**
 * Tests to verify that MasterService can be set through dependency injection
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 0, numClientNodes = 0)
public class MasterServiceTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(FaultDetection.SETTING_PING_TIMEOUT, "1s")
                .put(FaultDetection.SETTING_PING_RETRIES, "1")
                .put("discovery.type", "org.elasticsearch.discovery.zen.MasterServiceTestDiscoveryModule")
                .build();
    }
    
    @Test
    public void testCustomMasterService() throws Exception {
        Settings defaultSettings = indexSettings();

        TestMasterService.setMasterNode("node_s0");
        
        String node0 = internalCluster().startNode(defaultSettings);
        String node1 = internalCluster().startNode(defaultSettings);
        String node2 = internalCluster().startNode(defaultSettings);
        internalCluster().startNode(defaultSettings);

        String master = internalCluster().getMasterName();
        assertEquals(node0, master);

        TestMasterService.setMasterNode(node1);
        internalCluster().stopCurrentMasterNode();

        master = internalCluster().getMasterName();
        assertEquals(node1, master);
        
        TestMasterService.setMasterNode(node2);
        internalCluster().stopCurrentMasterNode();

        master = internalCluster().getMasterName();
        assertEquals(node2, master);
    }
}