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

package org.elasticsearch.stresstest.manyindices;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.Date;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 *
 */
public class ManyIndicesStressTest {

    private static final ESLogger logger = Loggers.getLogger(ManyIndicesStressTest.class);

    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.prefix", "");

        int numberOfIndices = 100;
        int numberOfDocs = 100;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.shard.check_on_startup", false)
                .put("gateway.type", "local")
                .put("index.number_of_shards", 1)
                .build();
        Node node = NodeBuilder.nodeBuilder().settings(settings).node();

        for (int i = 0; i < numberOfIndices; i++) {
            logger.info("START index [{}] ...", i);
            node.client().admin().indices().prepareCreate("index_" + i).execute().actionGet();

            for (int j = 0; j < numberOfDocs; j++) {
                node.client().prepareIndex("index_" + i, "type").setSource("field1", "test", "field2", 2, "field3", new Date()).execute().actionGet();
            }
            logger.info("DONE  index [{}] ...", i);
        }

        logger.info("closing node...");
        node.close();
        logger.info("node closed");

        logger.info("starting node...");
        node = NodeBuilder.nodeBuilder().settings(settings).node();

        ClusterHealthResponse health = node.client().admin().cluster().prepareHealth().setTimeout("5m").setWaitForYellowStatus().execute().actionGet();
        logger.info("health: " + health.getStatus());
        logger.info("active shards: " + health.getActiveShards());
        logger.info("active primary shards: " + health.getActivePrimaryShards());
        if (health.isTimedOut()) {
            logger.error("Timed out on health...");
        }

        ClusterState clusterState = node.client().admin().cluster().prepareState().execute().actionGet().getState();
        for (int i = 0; i < numberOfIndices; i++) {
            if (clusterState.blocks().indices().containsKey("index_" + i)) {
                logger.error("index [{}] has blocks: {}", i, clusterState.blocks().indices().get("index_" + i));
            }
        }

        for (int i = 0; i < numberOfIndices; i++) {
            long count = node.client().prepareCount("index_" + i).setQuery(matchAllQuery()).execute().actionGet().getCount();
            if (count == numberOfDocs) {
                logger.info("VERIFIED [{}], count [{}]", i, count);
            } else {
                logger.error("FAILED [{}], expected [{}], got [{}]", i, numberOfDocs, count);
            }
        }

        logger.info("closing node...");
        node.close();
        logger.info("node closed");
    }
}
