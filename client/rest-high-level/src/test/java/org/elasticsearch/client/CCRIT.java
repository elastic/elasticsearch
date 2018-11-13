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

package org.elasticsearch.client;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ccr.PauseFollowRequest;
import org.elasticsearch.client.ccr.PutFollowRequest;
import org.elasticsearch.client.ccr.PutFollowResponse;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CCRIT extends ESRestHighLevelClientTestCase {

    @Before
    public void setupRemoteClusterConfig() throws IOException {
        // Configure local cluster as remote cluster:
        // TODO: replace with nodes info highlevel rest client code when it is available:
        final Request request = new Request("GET", "/_nodes");
        Map<?, ?> nodesResponse = (Map<?, ?>) toMap(client().performRequest(request)).get("nodes");
        // Select node info of first node (we don't know the node id):
        nodesResponse = (Map<?, ?>) nodesResponse.get(nodesResponse.keySet().iterator().next());
        String transportAddress = (String) nodesResponse.get("transport_address");

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(Collections.singletonMap("cluster.remote.local.seeds", transportAddress));
        ClusterUpdateSettingsResponse updateSettingsResponse =
            highLevelClient().cluster().putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
        assertThat(updateSettingsResponse.isAcknowledged(), is(true));
    }

    public void testCCR() throws Exception {
        CcrClient ccrClient = highLevelClient().ccr();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("leader");
        createIndexRequest.settings(Collections.singletonMap("index.soft_deletes.enabled", true));
        CreateIndexResponse response = highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertThat(response.isAcknowledged(), is(true));

        PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", "follower");
        PutFollowResponse putFollowResponse = execute(putFollowRequest, ccrClient::putFollow, ccrClient::putFollowAsync);
        assertThat(putFollowResponse.isFollowIndexCreated(), is(true));
        assertThat(putFollowResponse.isFollowIndexShardsAcked(), is(true));
        assertThat(putFollowResponse.isIndexFollowingStarted(), is(true));

        IndexRequest indexRequest = new IndexRequest("leader", "_doc")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source("{}", XContentType.JSON);
        highLevelClient().index(indexRequest, RequestOptions.DEFAULT);

        SearchRequest leaderSearchRequest = new SearchRequest("leader");
        SearchResponse leaderSearchResponse = highLevelClient().search(leaderSearchRequest, RequestOptions.DEFAULT);
        assertThat(leaderSearchResponse.getHits().getTotalHits(), equalTo(1L));

        assertBusy(() -> {
            SearchRequest followerSearchRequest = new SearchRequest("follower");
            SearchResponse followerSearchResponse = highLevelClient().search(followerSearchRequest, RequestOptions.DEFAULT);
            assertThat(followerSearchResponse.getHits().getTotalHits(), equalTo(1L));
        });

        PauseFollowRequest pauseFollowRequest = new PauseFollowRequest("follower");
        AcknowledgedResponse pauseFollowResponse = execute(pauseFollowRequest, ccrClient::pauseFollow, ccrClient::pauseFollowAsync);
        assertThat(pauseFollowResponse.isAcknowledged(), is(true));
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
