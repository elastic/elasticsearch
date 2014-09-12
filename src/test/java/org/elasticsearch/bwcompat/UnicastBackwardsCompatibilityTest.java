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

package org.elasticsearch.bwcompat;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

public class UnicastBackwardsCompatibilityTest extends ElasticsearchBackwardsCompatIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("transport.tcp.port", 9380 + nodeOrdinal)
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("discovery.zen.ping.unicast.hosts", "localhost:9380,localhost:9381,localhost:9390,localhost:9391")
                .build();
    }

    @Override
    protected Settings externalNodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.externalNodeSettings(nodeOrdinal))
                .put("transport.tcp.port", 9390 + nodeOrdinal)
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("discovery.zen.ping.unicast.hosts", "localhost:9380,localhost:9381,localhost:9390,localhost:9391")
                .build();
    }

    @Test
    public void testUnicastDiscovery() {
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().get();
        assertThat(healthResponse.getNumberOfDataNodes(), equalTo(cluster().numDataNodes()));
    }
}
