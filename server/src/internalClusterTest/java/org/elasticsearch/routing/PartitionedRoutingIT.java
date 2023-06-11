/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.routing;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.mockito.internal.util.collections.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;

public class PartitionedRoutingIT extends ESIntegTestCase {

    public void testVariousPartitionSizes() throws Exception {
        for (int shards = 1; shards <= 4; shards++) {
            for (int partitionSize = 1; partitionSize < shards; partitionSize++) {
                String index = "index_" + shards + "_" + partitionSize;

                indicesAdmin().prepareCreate(index)
                    .setSettings(
                        Settings.builder()
                            .put("index.number_of_shards", shards)
                            .put("index.number_of_routing_shards", shards)
                            .put("index.routing_partition_size", partitionSize)
                    )
                    .setMapping("{\"_routing\":{\"required\":true}}")
                    .execute()
                    .actionGet();
                ensureGreen();

                Map<String, Set<String>> routingToDocumentIds = generateRoutedDocumentIds(index);

                verifyGets(index, routingToDocumentIds);
                verifyBroadSearches(index, routingToDocumentIds, shards);
                verifyRoutedSearches(index, routingToDocumentIds, Sets.newSet(partitionSize));
            }
        }
    }

    public void testShrinking() throws Exception {
        assumeFalse("http://github.com/elastic/elasticsearch/issues/33857", Constants.WINDOWS);

        // creates random routing groups and repeatedly halves the index until it is down to 1 shard
        // verifying that the count is correct for each shrunken index
        final int partitionSize = 3;
        final int originalShards = 8;
        int currentShards = originalShards;
        String index = "index_" + currentShards;

        indicesAdmin().prepareCreate(index)
            .setSettings(
                indexSettings(currentShards, numberOfReplicas()).put("index.number_of_routing_shards", currentShards)
                    .put("index.routing_partition_size", partitionSize)
            )
            .setMapping("{\"_routing\":{\"required\":true}}")
            .execute()
            .actionGet();
        ensureGreen();

        Map<String, Set<String>> routingToDocumentIds = generateRoutedDocumentIds(index);

        while (true) {
            int factor = originalShards / currentShards;

            verifyGets(index, routingToDocumentIds);
            verifyBroadSearches(index, routingToDocumentIds, currentShards);

            // we need the floor and ceiling of the routing_partition_size / factor since the partition size of the shrunken
            // index will be one of those, depending on the routing value
            verifyRoutedSearches(
                index,
                routingToDocumentIds,
                Math.floorDiv(partitionSize, factor) == 0
                    ? Sets.newSet(1, 2)
                    : Sets.newSet(Math.floorDiv(partitionSize, factor), -Math.floorDiv(-partitionSize, factor))
            );

            updateIndexSettings(
                Settings.builder()
                    .put(
                        "index.routing.allocation.require._name",
                        clusterAdmin().prepareState().get().getState().nodes().getDataNodes().values().toArray(DiscoveryNode[]::new)[0]
                            .getName()
                    )
                    .put("index.blocks.write", true),
                index
            );
            ensureGreen();

            currentShards = Math.floorDiv(currentShards, 2);

            if (currentShards == 0) {
                break;
            }

            String previousIndex = index;
            index = "index_" + currentShards;

            logger.info("--> shrinking index [" + previousIndex + "] to [" + index + "]");
            indicesAdmin().prepareResizeIndex(previousIndex, index)
                .setSettings(indexSettings(currentShards, numberOfReplicas()).putNull("index.routing.allocation.require._name").build())
                .get();
            ensureGreen();
        }
    }

    public void testUnableToUpdateIndexRoutingPartitionSizes() throws Exception {
        Settings currentSettings = Settings.builder().put("index.routing_partition_size", 2).build();
        IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
            currentSettings,
            Set.of(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING)
        );
        Settings newSettings = Settings.builder().put("index.routing_partition_size", 3).build();

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> indexScopedSettings.updateDynamicSettings(
                newSettings,
                Settings.builder().put(currentSettings),
                Settings.builder(),
                "indexMetadata"
            )
        );
        assertThat(exc.getMessage(), containsString("final indexMetadata setting [index.routing_partition_size]"));
    }

    private void verifyRoutedSearches(String index, Map<String, Set<String>> routingToDocumentIds, Set<Integer> expectedShards) {
        for (Map.Entry<String, Set<String>> routingEntry : routingToDocumentIds.entrySet()) {
            String routing = routingEntry.getKey();
            int expectedDocuments = routingEntry.getValue().size();

            SearchResponse response = client().prepareSearch()
                .setQuery(QueryBuilders.termQuery("_routing", routing))
                .setRouting(routing)
                .setIndices(index)
                .setSize(100)
                .execute()
                .actionGet();

            logger.info(
                "--> routed search on index ["
                    + index
                    + "] visited ["
                    + response.getTotalShards()
                    + "] shards for routing ["
                    + routing
                    + "] and got hits ["
                    + response.getHits().getTotalHits().value
                    + "]"
            );

            assertTrue(
                response.getTotalShards() + " was not in " + expectedShards + " for " + index,
                expectedShards.contains(response.getTotalShards())
            );
            assertEquals(expectedDocuments, response.getHits().getTotalHits().value);

            Set<String> found = new HashSet<>();
            response.getHits().forEach(h -> found.add(h.getId()));

            assertEquals(routingEntry.getValue(), found);
        }
    }

    private void verifyBroadSearches(String index, Map<String, Set<String>> routingToDocumentIds, int expectedShards) {
        for (Map.Entry<String, Set<String>> routingEntry : routingToDocumentIds.entrySet()) {
            String routing = routingEntry.getKey();
            int expectedDocuments = routingEntry.getValue().size();

            SearchResponse response = client().prepareSearch()
                .setQuery(QueryBuilders.termQuery("_routing", routing))
                .setIndices(index)
                .setSize(100)
                .execute()
                .actionGet();

            assertEquals(expectedShards, response.getTotalShards());
            assertEquals(expectedDocuments, response.getHits().getTotalHits().value);

            Set<String> found = new HashSet<>();
            response.getHits().forEach(h -> found.add(h.getId()));

            assertEquals(routingEntry.getValue(), found);
        }
    }

    private void verifyGets(String index, Map<String, Set<String>> routingToDocumentIds) {
        for (Map.Entry<String, Set<String>> routingEntry : routingToDocumentIds.entrySet()) {
            String routing = routingEntry.getKey();

            for (String id : routingEntry.getValue()) {
                assertTrue(client().prepareGet(index, id).setRouting(routing).execute().actionGet().isExists());
            }
        }
    }

    private Map<String, Set<String>> generateRoutedDocumentIds(String index) {
        Map<String, Set<String>> routingToDocumentIds = new HashMap<>();
        int numRoutingValues = randomIntBetween(5, 15);

        for (int i = 0; i < numRoutingValues; i++) {
            String routingValue = String.valueOf(i);
            int numDocuments = randomIntBetween(10, 100);
            routingToDocumentIds.put(String.valueOf(routingValue), new HashSet<>());

            for (int k = 0; k < numDocuments; k++) {
                String id = routingValue + "_" + String.valueOf(k);
                routingToDocumentIds.get(routingValue).add(id);

                client().prepareIndex(index).setId(id).setRouting(routingValue).setSource("foo", "bar").get();
            }
        }

        client().admin().indices().prepareRefresh(index).get();

        return routingToDocumentIds;
    }

}
