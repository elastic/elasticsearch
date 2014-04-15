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

package org.elasticsearch.stresstest.indexing;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.concurrent.ThreadLocalRandom;

/**
 */
public class BulkIndexingStressTest {

    public static void main(String[] args) {
        final int NUMBER_OF_NODES = 4;
        final int NUMBER_OF_INDICES = 600;
        final int BATCH = 300;

        final Settings nodeSettings = ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2).build();

//            ESLogger logger = Loggers.getLogger("org.elasticsearch");
//            logger.setLevel("DEBUG");
        Node[] nodes = new Node[NUMBER_OF_NODES];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeBuilder.nodeBuilder().settings(nodeSettings).node();
        }

        Client client = nodes.length == 1 ? nodes[0].client() : nodes[1].client();

        while (true) {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (int i = 0; i < BATCH; i++) {
                bulkRequest.add(Requests.indexRequest("test" + ThreadLocalRandom.current().nextInt(NUMBER_OF_INDICES)).type("type").source("field", "value"));
            }
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                for (BulkItemResponse item : bulkResponse) {
                    if (item.isFailed()) {
                        System.out.println("failed response:" + item.getFailureMessage());
                    }
                }

                throw new RuntimeException("Failed responses");
            }
            ;
        }
    }
}
