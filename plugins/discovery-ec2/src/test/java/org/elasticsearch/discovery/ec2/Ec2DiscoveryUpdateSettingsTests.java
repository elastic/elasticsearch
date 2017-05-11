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

package org.elasticsearch.discovery.ec2;


import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static org.hamcrest.CoreMatchers.is;

/**
 * Just an empty Node Start test to check eveything if fine when
 * starting.
 * This test requires AWS to run.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0, autoMinMasterNodes = false)
public class Ec2DiscoveryUpdateSettingsTests extends AbstractAwsTestCase {
    public void testMinimumMasterNodesStart() {
        Settings nodeSettings = Settings.builder()
                .put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "ec2")
                .build();
        internalCluster().startNode(nodeSettings);

        // We try to update minimum_master_nodes now
        ClusterUpdateSettingsResponse response = client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("discovery.zen.minimum_master_nodes", 1))
                .setTransientSettings(Settings.builder().put("discovery.zen.minimum_master_nodes", 1))
                .get();

        Integer min = response.getPersistentSettings().getAsInt("discovery.zen.minimum_master_nodes", null);
        assertThat(min, is(1));
    }
}
