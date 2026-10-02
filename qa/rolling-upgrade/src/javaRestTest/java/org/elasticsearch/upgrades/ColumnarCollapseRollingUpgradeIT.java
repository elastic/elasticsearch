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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A strict-columnar keyword field writes its doc values as a binary blob whose framing the mapping settles when the
 * index is created. Every phase here adds documents, so the segments collapse finally reads were written by a mix of
 * node versions. Only the fully upgraded cluster asserts the collapse, since nodes still on the old version reject it.
 */
public class ColumnarCollapseRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String INDEX = "test-columnar-collapse";

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

        indexHosts();

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
            assertThat(hits, Matchers.hasSize(2));
            assertThat(ObjectPath.<String>evaluate(hits.get(0), "fields.host\\.name.0"), Matchers.equalTo("host-a"));
            assertThat(ObjectPath.<String>evaluate(hits.get(1), "fields.host\\.name.0"), Matchers.equalTo("host-b"));
        }
    }

    private void indexHosts() throws IOException {
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            body.append("{\"index\": {}}\n");
            body.append("{\"@timestamp\": \"2024-01-01T00:00:0").append(i % 10).append("Z\", \"host.name\": \"host-a\"}\n");
            body.append("{\"index\": {}}\n");
            body.append("{\"@timestamp\": \"2024-01-02T00:00:0").append(i % 10).append("Z\", \"host.name\": \"host-b\"}\n");
        }
        bulk.setJsonEntity(body.toString());
        Response response = client().performRequest(bulk);
        assertOK(response);
        assertThat(entityAsMap(response).get("errors"), Matchers.is(false));
    }
}
