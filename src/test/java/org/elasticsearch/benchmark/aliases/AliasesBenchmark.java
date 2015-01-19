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

package org.elasticsearch.benchmark.aliases;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.util.List;

/**
 */
public class AliasesBenchmark {

    private final static String INDEX_NAME = "my-index";

    public static void main(String[] args) throws IOException {
        int NUM_ADDITIONAL_NODES = 0;
        int BASE_ALIAS_COUNT = 100000;
        int NUM_ADD_ALIAS_REQUEST = 1000;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.master", false).build();
        Node node1 = NodeBuilder.nodeBuilder().settings(
                ImmutableSettings.settingsBuilder().put(settings).put("node.master", true)
        ).node();

        Node[] otherNodes = new Node[NUM_ADDITIONAL_NODES];
        for (int i = 0; i < otherNodes.length; i++) {
            otherNodes[i] = NodeBuilder.nodeBuilder().settings(settings).node();
        }

        Client client = node1.client();
        try {
            client.admin().indices().prepareCreate(INDEX_NAME).execute().actionGet();
        } catch (IndexAlreadyExistsException e) {}
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        int numberOfAliases = countAliases(client);
        System.out.println("Number of aliases: " + numberOfAliases);

        if (numberOfAliases < BASE_ALIAS_COUNT) {
            int diff = BASE_ALIAS_COUNT - numberOfAliases;
            System.out.println("Adding " + diff + " more aliases to get to the start amount of " + BASE_ALIAS_COUNT + " aliases");
            IndicesAliasesRequestBuilder builder = client.admin().indices().prepareAliases();
            for (int i = 1; i <= diff; i++) {
                builder.addAlias(INDEX_NAME, Strings.randomBase64UUID());
                if (i % 1000 == 0) {
                    builder.execute().actionGet();
                    builder = client.admin().indices().prepareAliases();
                }
            }
            if (!builder.request().getAliasActions().isEmpty()) {
                builder.execute().actionGet();
            }
        } else if (numberOfAliases > BASE_ALIAS_COUNT) {
            IndicesAliasesRequestBuilder builder = client.admin().indices().prepareAliases();
            int diff = numberOfAliases - BASE_ALIAS_COUNT;
            System.out.println("Removing " + diff + " aliases to get to the start amount of " + BASE_ALIAS_COUNT + " aliases");
            List<AliasMetaData> aliases= client.admin().indices().prepareGetAliases("*")
                    .addIndices(INDEX_NAME)
                    .execute().actionGet().getAliases().get(INDEX_NAME);
            for (int i = 0; i <= diff; i++) {
                builder.removeAlias(INDEX_NAME, aliases.get(i).alias());
                if (i % 1000 == 0) {
                    builder.execute().actionGet();
                    builder = client.admin().indices().prepareAliases();
                }
            }
            if (!builder.request().getAliasActions().isEmpty()) {
                builder.execute().actionGet();
            }
        }

        numberOfAliases = countAliases(client);
        System.out.println("Number of aliases: " + numberOfAliases);

        long totalTime = 0;
        int max = numberOfAliases + NUM_ADD_ALIAS_REQUEST;
        for (int i = numberOfAliases; i <= max; i++) {
            if (i != numberOfAliases && i % 100 == 0) {
                long avgTime = totalTime / 100;
                System.out.println("Added [" + (i - numberOfAliases) + "] aliases. Avg create time: "  + avgTime + " ms");
                totalTime = 0;
            }

            long time = System.currentTimeMillis();
//            String filter = termFilter("field" + i, "value" + i).toXContent(XContentFactory.jsonBuilder(), null).string();
            client.admin().indices().prepareAliases().addAlias(INDEX_NAME, Strings.randomBase64UUID()/*, filter*/)
                    .execute().actionGet();
            totalTime += System.currentTimeMillis() - time;
        }
        System.out.println("Number of aliases: " + countAliases(client));

        client.close();
        node1.close();
        for (Node otherNode : otherNodes) {
            otherNode.close();
        }
    }

    private static int countAliases(Client client) {
        GetAliasesResponse response = client.admin().indices().prepareGetAliases("*")
                .addIndices(INDEX_NAME)
                .execute().actionGet();
        if (response.getAliases().isEmpty()) {
            return 0;
        } else {
            return response.getAliases().get(INDEX_NAME).size();
        }
    }

}
