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

package org.elasticsearch.gce.itest;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cloud.gce.AbstractGceTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * This test needs GCE to run and -Dtests.gce=true to be set
 * and -Des.config=/path/to/elasticsearch.yml
 * TODO: By now, it will only work from GCE platform as we don't support yet external auth.
 * See https://github.com/elasticsearch/elasticsearch-cloud-gce/issues/10
 * @see org.elasticsearch.cloud.gce.AbstractGceTest
 */
@AbstractGceTest.GceTest
@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.SUITE,
        numDataNodes = 1,
        transportClientRatio = 0.0)
public class GceSimpleITest extends AbstractGceTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @Test
    public void one_node_should_run() {
        // Do nothing... Just start :-)
        // but let's check that we have at least 1 node (local node)
        ClusterStateResponse clusterState = client().admin().cluster().prepareState().execute().actionGet();

        assertThat(clusterState.getState().getNodes().getSize(), Matchers.greaterThanOrEqualTo(1));
    }

    @Override
    public Settings indexSettings() {
        // During restore we frequently restore index to exactly the same state it was before, that might cause the same
        // checksum file to be written twice during restore operation
        return ImmutableSettings.builder().put(super.indexSettings())
                .build();
    }
}
