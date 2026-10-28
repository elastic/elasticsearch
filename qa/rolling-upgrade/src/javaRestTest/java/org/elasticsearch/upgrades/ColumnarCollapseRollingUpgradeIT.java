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
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A strict-columnar keyword field writes its doc values as a binary blob whose framing the mapping settles when the
 * index is created. Every phase adds documents, so the final collapse reads documents written during each of them. Only
 * the fully upgraded cluster asserts, since nodes still on the old version reject collapse.
 */
public class ColumnarCollapseRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String INDEX = "test-columnar-collapse";
    private static final String OLD_TIMESTAMP = "2024-01-01T00:00:00Z";
    private static final String OLD_DATE = "2024-01-01T00:00:00";

    public ColumnarCollapseRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testCollapseOnColumnarKeyword() throws Exception {
        if (isOldCluster()) {
            if (clusterHasCapability("PUT", "/{index}", List.of(), List.of("columnar_index_modes")).orElse(false) == false) {
                return;
            }
            createIndex(
                INDEX,
                Settings.builder()
                    .put("index.mode", "columnar")
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build(),
                """
                    {
                        "properties": {
                            "@timestamp": { "type": "date" },
                            "host.name": { "type": "keyword" }
                        }
                    }
                    """
            );
        }

        if (indexExists(INDEX) == false) {
            assumeTrue("the old cluster did not support columnar index modes", isOldCluster());
            return;
        }

        indexHosts(phase());

        if (isUpgradedCluster()) {
            Request search = new Request("POST", "/" + INDEX + "/_search");
            search.setJsonEntity("""
                {
                  "collapse": { "field": "host.name" },
                  "sort": [ { "@timestamp": "asc" } ],
                  "size": 10
                }""");
            Map<String, Object> response = entityAsMap(client().performRequest(search));

            List<?> hits = ObjectPath.evaluate(response, "hits.hits");
            List<String> groups = new ArrayList<>();
            for (Object hit : hits) {
                groups.add(ObjectPath.evaluate(hit, "fields.host\\.name.0"));
            }
            assertThat(groups, Matchers.containsInAnyOrder("shared", "old", "mixed-first", "mixed-rest", "upgraded"));
            assertThat(ObjectPath.<Integer>evaluate(response, "hits.total.value"), Matchers.equalTo(8));
            int shared = groups.indexOf("shared");
            assertThat(ObjectPath.<String>evaluate(hits.get(shared), "_source.@timestamp"), Matchers.startsWith(OLD_DATE));
        }
    }

    private static String phase() {
        if (isOldCluster()) {
            return "old";
        }
        if (isUpgradedCluster()) {
            return "upgraded";
        }
        return isFirstMixedCluster() ? "mixed-first" : "mixed-rest";
    }

    /**
     * The shared key's winning document is the one the old cluster wrote, which carries the earliest timestamp.
     */
    private void indexHosts(String phase) throws IOException {
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        String sharedTimestamp = switch (phase) {
            case "old" -> OLD_TIMESTAMP;
            case "mixed-first" -> "2024-01-02T00:00:00Z";
            case "mixed-rest" -> "2024-01-03T00:00:00Z";
            default -> "2024-01-04T00:00:00Z";
        };
        bulk.setJsonEntity(Strings.format("""
            {"index": {}}
            {"@timestamp": "%s", "host.name": "shared"}
            {"index": {}}
            {"@timestamp": "%s", "host.name": "%s"}
            """, sharedTimestamp, sharedTimestamp, phase));
        Response response = client().performRequest(bulk);
        assertOK(response);
        assertThat(entityAsMap(response).get("errors"), Matchers.is(false));
    }
}
