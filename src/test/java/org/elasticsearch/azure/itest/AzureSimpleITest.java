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

package org.elasticsearch.azure.itest;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cloud.azure.AbstractAzureTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * This test needs Azure to run and -Dtests.azure=true to be set
 * and -Des.config=/path/to/elasticsearch.yml
 * @see org.elasticsearch.cloud.azure.AbstractAzureTest
 */
@AbstractAzureTest.AzureTest
@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.TEST,
        numDataNodes = 1,
        numClientNodes = 0,
        transportClientRatio = 0.0)
public class AzureSimpleITest extends AbstractAzureTest {

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
