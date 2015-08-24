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

package org.elasticsearch.benchmark.mapping;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.bootstrap.BootstrapForTesting;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 */
public class ManyMappingsBenchmark {

    private static final String MAPPING = "{\n" +
            "        \"dynamic_templates\": [\n" +
            "          {\n" +
            "            \"t1\": {\n" +
            "              \"mapping\": {\n" +
            "                \"store\": false,\n" +
            "                \"norms\": {\n" +
            "                  \"enabled\": false\n" +
            "                },\n" +
            "                \"type\": \"string\"\n" +
            "              },\n" +
            "              \"match\": \"*_ss\"\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"t2\": {\n" +
            "              \"mapping\": {\n" +
            "                \"store\": false,\n" +
            "                \"type\": \"date\"\n" +
            "              },\n" +
            "              \"match\": \"*_dt\"\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"t3\": {\n" +
            "              \"mapping\": {\n" +
            "                \"store\": false,\n" +
            "                \"type\": \"integer\"\n" +
            "              },\n" +
            "              \"match\": \"*_i\"\n" +
            "            }\n" +
            "          }\n" +
            "        ],\n" +
            "        \"_source\": {\n" +
            "          \"enabled\": false\n" +
            "        },\n" +
            "        \"properties\": {}\n" +
            "      }";

    private static final String INDEX_NAME = "index";
    private static final String TYPE_NAME = "type";
    private static final int FIELD_COUNT = 100000;
    private static final int DOC_COUNT = 10000000;
    private static final boolean TWO_NODES = true;

    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.prefix", "");
        BootstrapForTesting.ensureInitialized();
        Settings settings = settingsBuilder()
                .put("")
                .put(SETTING_NUMBER_OF_SHARDS, 5)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = ManyMappingsBenchmark.class.getSimpleName();
        Node node = nodeBuilder().clusterName(clusterName)
                .settings(settingsBuilder().put(settings))
                .node();
        if (TWO_NODES) {
            Node node2 = nodeBuilder().clusterName(clusterName)
                    .settings(settingsBuilder().put(settings))
                    .node();
        }

        Client client = node.client();

        client.admin().indices().prepareDelete(INDEX_NAME)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .get();
        client.admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, MAPPING)
                .get();

        BulkRequestBuilder builder = client.prepareBulk();
        int fieldCount = 0;
        long time = System.currentTimeMillis();
        final int PRINT = 1000;
        for (int i = 0; i < DOC_COUNT; i++) {
            XContentBuilder sourceBuilder = jsonBuilder().startObject();
            sourceBuilder.field(++fieldCount + "_ss", "xyz");
            sourceBuilder.field(++fieldCount + "_dt", System.currentTimeMillis());
            sourceBuilder.field(++fieldCount + "_i", i % 100);
            sourceBuilder.endObject();

            if (fieldCount >= FIELD_COUNT) {
                fieldCount = 0;
                System.out.println("dynamic fields rolled up");
            }

            builder.add(
                    client.prepareIndex(INDEX_NAME, TYPE_NAME, String.valueOf(i))
                            .setSource(sourceBuilder)
            );

            if (builder.numberOfActions() >= 1000) {
                builder.get();
                builder = client.prepareBulk();
            }

            if (i % PRINT == 0) {
                long took = System.currentTimeMillis() - time;
                time = System.currentTimeMillis();
                System.out.println("Indexed " + i +  " docs, in " + TimeValue.timeValueMillis(took));
            }
        }
        if (builder.numberOfActions() > 0) {
            builder.get();
        }



    }

}