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
package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.seal.SealIndicesResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.equalTo;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class SealTests extends ElasticsearchIntegrationTest {

    @Test
    public void testUnallocatedShardsDoesNotHang() throws InterruptedException {
        Settings.Builder settingsBuilder = Settings.builder()
                .put("node.data", false)
                .put("node.master", true)
                .put("path.data", createTempDir().toString());
        internalCluster().startNode(settingsBuilder.build());
        //  create an index but because no data nodes are available no shards will be allocated
        createIndex("test");
        // this should not hang but instead immediately return with empty result set
        SealIndicesResponse sealIndicesResponse = client().admin().indices().prepareSealIndices("test").get();
        // just to make sure the test actually tests the right thing
        int numShards = client().admin().indices().prepareGetSettings("test").get().getIndexToSettings().get("test").getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, -1);
        assertThat(sealIndicesResponse.results().size(), equalTo(numShards));
        assertThat(sealIndicesResponse.results().iterator().next().failureReason(), equalTo("no active primary available"));
    }
}
