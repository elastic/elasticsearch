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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NoopDiscovery;
import org.hamcrest.Matchers;

import java.io.IOException;

public class GatewayServiceTests extends ESTestCase {

    private GatewayService createService(Settings.Builder settings) {
        ClusterService clusterService = new ClusterService(Settings.builder().put("cluster.name", "GatewayServiceTests").build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                null);
        return new GatewayService(Settings.builder()
                .put("http.enabled", "false")
                .put("discovery.type", "local")
                .put(settings.build()).build(),
                null, clusterService, null, null, null, null, new NoopDiscovery(), null, null);
    }

    public void testDefaultRecoverAfterTime() throws IOException {
        // check that the default is not set
        GatewayService service = createService(Settings.builder());
        assertNull(service.recoverAfterTime());

        // ensure default is set when setting expected_nodes
        service = createService(Settings.builder().put("gateway.expected_nodes", 1));
        assertThat(service.recoverAfterTime(), Matchers.equalTo(GatewayService.DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET));

        // ensure default is set when setting expected_data_nodes
        service = createService(Settings.builder().put("gateway.expected_data_nodes", 1));
        assertThat(service.recoverAfterTime(), Matchers.equalTo(GatewayService.DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET));

        // ensure default is set when setting expected_master_nodes
        service = createService(Settings.builder().put("gateway.expected_master_nodes", 1));
        assertThat(service.recoverAfterTime(), Matchers.equalTo(GatewayService.DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET));

        // ensure settings override default
        TimeValue timeValue = TimeValue.timeValueHours(3);
        // ensure default is set when setting expected_nodes
        service = createService(Settings.builder().put("gateway.expected_nodes", 1).put("gateway.recover_after_time", timeValue.toString()));
        assertThat(service.recoverAfterTime().millis(), Matchers.equalTo(timeValue.millis()));
    }
}
