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

package org.elasticsearch.discovery;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope=Scope.SUITE, numNodes=2)
public class DiscoveryTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder().put("discovery.zen.ping.multicast.enabled", false)
                .put("discovery.zen.ping.unicast.hosts", "localhost").put(super.nodeSettings(nodeOrdinal)).build();
    }
    
    @Test
    @LuceneTestCase.AwaitsFix(bugUrl = "Proposed fix: Each node maintains a list of endpoints that have pinged it " +
            "(UnicastZenPing#temporalResponses), a node will remove entries that are old. We can use this list to extend " +
            "'discovery.zen.ping.unicast.hosts' list of nodes to ping. If we do this then in the test both nodes will ping each " +
            "other, like in solution 1. The upside compared to solution 1, is that it won't go and ping 100 endpoints (based on the default port range), " +
            "just other nodes that have pinged it in addition to the already configured nodes in the 'discovery.zen.ping.unicast.hosts' list.")
    public void testUnicastDiscovery() {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
    }
}