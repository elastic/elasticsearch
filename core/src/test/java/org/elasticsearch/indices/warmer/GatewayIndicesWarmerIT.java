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

package org.elasticsearch.indices.warmer;

import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.elasticsearch.test.ESIntegTestCase.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ClusterScope(numDataNodes =0, scope= Scope.TEST)
public class GatewayIndicesWarmerIT extends ESIntegTestCase {

    private final ESLogger logger = Loggers.getLogger(GatewayIndicesWarmerIT.class);

    @Test
    public void testStatePersistence() throws Exception {

        logger.info("--> starting 1 nodes");
        internalCluster().startNode();

        logger.info("--> putting two templates");
        createIndex("test");

        ensureYellow();

        assertAcked(client().admin().indices().preparePutWarmer("warmer_1")
                .setSearchRequest(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "value1"))));
        assertAcked(client().admin().indices().preparePutWarmer("warmer_2")
                .setSearchRequest(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "value2"))));

        logger.info("--> put template with warmer");
        client().admin().indices().preparePutTemplate("template_1")
                .setSource("{\n" +
                        "    \"template\" : \"xxx\",\n" +
                        "    \"warmers\" : {\n" +
                        "        \"warmer_1\" : {\n" +
                        "            \"types\" : [],\n" +
                        "            \"source\" : {\n" +
                        "                \"query\" : {\n" +
                        "                    \"match_all\" : {}\n" +
                        "                }\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}")
                .execute().actionGet();


        logger.info("--> verify warmers are registered in cluster state");
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexWarmersMetaData warmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
        assertThat(warmersMetaData, Matchers.notNullValue());
        assertThat(warmersMetaData.entries().size(), equalTo(2));

        IndexWarmersMetaData templateWarmers = clusterState.metaData().templates().get("template_1").custom(IndexWarmersMetaData.TYPE);
        assertThat(templateWarmers, Matchers.notNullValue());
        assertThat(templateWarmers.entries().size(), equalTo(1));

        logger.info("--> restarting the node");
        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return Settings.EMPTY;
            }
        });

        ensureYellow();

        logger.info("--> verify warmers are recovered");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexWarmersMetaData recoveredWarmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
        assertThat(recoveredWarmersMetaData.entries().size(), equalTo(warmersMetaData.entries().size()));
        for (int i = 0; i < warmersMetaData.entries().size(); i++) {
            assertThat(recoveredWarmersMetaData.entries().get(i).name(), equalTo(warmersMetaData.entries().get(i).name()));
            assertThat(recoveredWarmersMetaData.entries().get(i).source(), equalTo(warmersMetaData.entries().get(i).source()));
        }

        logger.info("--> verify warmers in template are recovered");
        IndexWarmersMetaData recoveredTemplateWarmers = clusterState.metaData().templates().get("template_1").custom(IndexWarmersMetaData.TYPE);
        assertThat(recoveredTemplateWarmers.entries().size(), equalTo(templateWarmers.entries().size()));
        for (int i = 0; i < templateWarmers.entries().size(); i++) {
            assertThat(recoveredTemplateWarmers.entries().get(i).name(), equalTo(templateWarmers.entries().get(i).name()));
            assertThat(recoveredTemplateWarmers.entries().get(i).source(), equalTo(templateWarmers.entries().get(i).source()));
        }


        logger.info("--> delete warmer warmer_1");
        DeleteWarmerResponse deleteWarmerResponse = client().admin().indices().prepareDeleteWarmer().setIndices("test").setNames("warmer_1").execute().actionGet();
        assertThat(deleteWarmerResponse.isAcknowledged(), equalTo(true));

        logger.info("--> verify warmers (delete) are registered in cluster state");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        warmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
        assertThat(warmersMetaData, Matchers.notNullValue());
        assertThat(warmersMetaData.entries().size(), equalTo(1));

        logger.info("--> restarting the node");
        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return Settings.EMPTY;
            }
        });

        ensureYellow();

        logger.info("--> verify warmers are recovered");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        recoveredWarmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
        assertThat(recoveredWarmersMetaData.entries().size(), equalTo(warmersMetaData.entries().size()));
        for (int i = 0; i < warmersMetaData.entries().size(); i++) {
            assertThat(recoveredWarmersMetaData.entries().get(i).name(), equalTo(warmersMetaData.entries().get(i).name()));
            assertThat(recoveredWarmersMetaData.entries().get(i).source(), equalTo(warmersMetaData.entries().get(i).source()));
        }
    }
}
