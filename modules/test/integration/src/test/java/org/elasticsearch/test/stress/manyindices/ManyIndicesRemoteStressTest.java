/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.stress.manyindices;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.Date;

/**
 * @author kimchy (shay.banon)
 */
public class ManyIndicesRemoteStressTest {

    private static final ESLogger logger = Loggers.getLogger(ManyIndicesRemoteStressTest.class);

    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.prefix", "");

        int numberOfShards = 1;
        int numberOfReplicas = 1;
        int numberOfIndices = 1000;
        int numberOfDocs = 0;

        Node node = NodeBuilder.nodeBuilder().client(true).node();

        for (int i = 0; i < numberOfIndices; i++) {
            logger.info("START index [{}] ...", i);
            node.client().admin().indices().prepareCreate("index_" + i)
                    .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", numberOfShards).put("index.number_of_replicas", numberOfReplicas))
                    .execute().actionGet();

            for (int j = 0; j < numberOfDocs; j++) {
                node.client().prepareIndex("index_" + i, "type").setSource("field1", "test", "field2", 2, "field3", new Date()).execute().actionGet();
            }
            logger.info("DONE  index [{}]", i);
        }

        node.client().admin().indices().prepareGatewaySnapshot().execute().actionGet();

        logger.info("closing node...");
        node.close();
        logger.info("node closed");
    }
}
