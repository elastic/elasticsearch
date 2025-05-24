/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;

/**
 * Tests that index sorting works correctly after a rolling upgrade.
 */
public class IndexSortUpgradeIT extends AbstractRollingUpgradeTestCase {

    public IndexSortUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testIndexSortForNumericTypes() throws Exception {
        record IndexConfig(String indexName, String fieldName, String fieldType) {}
        var configs = new IndexConfig[] {
            new IndexConfig("index_byte", "byte_field", "byte"),
            new IndexConfig("index_short", "short_field", "short"),
            new IndexConfig("index_int", "int_field", "integer") };

        if (isOldCluster()) {
            int numShards = randomIntBetween(1, 3);
            for (var config : configs) {
                createIndex(
                    config.indexName(),
                    Settings.builder()
                        .put("index.number_of_shards", numShards)
                        .put("index.number_of_replicas", 0)
                        .put("index.sort.field", config.fieldName())
                        .put("index.sort.order", "desc")
                        .build(),
                    """
                        {
                            "properties": {
                                "%s": {
                                    "type": "%s"
                                }
                            }
                        }
                        """.formatted(config.fieldName(), config.fieldType())
                );
            }
        }

        final int numDocs = randomIntBetween(10, 25);
        for (var config : configs) {
            var bulkRequest = new Request("POST", "/" + config.indexName() + "/_bulk");
            StringBuilder bulkBody = new StringBuilder();
            for (int i = 0; i < numDocs; i++) {
                bulkBody.append("{\"index\": {}}\n");
                bulkBody.append("{\"" + config.fieldName() + "\": ").append(i).append("}\n");
            }
            bulkRequest.setJsonEntity(bulkBody.toString());
            bulkRequest.addParameter("refresh", "true");
            var bulkResponse = client().performRequest(bulkRequest);
            assertOK(bulkResponse);

            var searchRequest = new Request("GET", "/" + config.indexName() + "/_search");
            searchRequest.setJsonEntity("""
                {
                    "query": {
                        "match_all": {}
                    },
                    "sort": {
                        "%s": {
                            "order": "desc"
                        }
                    }
                }
                """.formatted(config.fieldName()));
            var searchResponse = client().performRequest(searchRequest);
            assertOK(searchResponse);
        }
    }
}
