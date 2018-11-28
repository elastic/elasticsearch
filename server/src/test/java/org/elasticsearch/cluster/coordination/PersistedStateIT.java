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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PersistedStateIT extends ESIntegTestCase {


    public void testPersistentSettingsOnFullRestart() throws Exception {
        internalCluster().startNodes(1);
        final int maxShardsPerNode = randomIntBetween(1000, 10000);

        client().admin().cluster().updateSettings(
                new ClusterUpdateSettingsRequest().persistentSettings(Settings.builder()
                        .put(MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode).build()))
                .actionGet();
        internalCluster().fullRestart();
        ensureStableCluster(1);
        MetaData metaData =
                client().admin().cluster().state(new ClusterStateRequest()).actionGet(30, TimeUnit.SECONDS).getState().metaData();
        assertThat(metaData.persistentSettings()
                        .get(MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()), equalTo(Integer.toString(maxShardsPerNode)));

    }

    public void testIndexIsPreservedOnFullClusterRestart() throws Exception {
        List<String> nodes = internalCluster().startNodes(2);
        createIndex("test");

        internalCluster().fullRestart();
        ensureGreen("test");

    }
}
