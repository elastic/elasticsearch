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

package org.elasticsearch.env;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeEnvironmentIT extends ESIntegTestCase {
    public void testStartFailureOnDataForNonDataNode() throws Exception {
        logger.info("--> starting two nodes");

        final String node_1 = internalCluster().startNodes(1).get(0);

        logger.info("--> creating index");
        prepareCreate("test", Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
        ).get();

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
        }
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> restarting the node with node.data=true");
        internalCluster().restartNode(node_1, new InternalTestCluster.RestartCallback());

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> restarting the node with node.data=false");
        try {
            internalCluster().restartNode(node_1, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    return Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build();
                }
            });
            fail("Starting a node with node.data=false that has index/shard data must fail");
        } catch (IllegalArgumentException e) {
            // EXPECTED
        }
    }
}
