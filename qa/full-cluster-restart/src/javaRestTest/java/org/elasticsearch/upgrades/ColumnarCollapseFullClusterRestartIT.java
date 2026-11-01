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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A strict-columnar keyword field writes its doc values as a binary blob whose framing is decided by the index
 * version and settings in force when the mapping was created, and then persisted. An upgraded node has to read back
 * the framing the old version chose, so collapse is exercised here against an index this cluster did not write.
 */
public class ColumnarCollapseFullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    private static final String INDEX = "test-columnar-collapse";

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .build();

    public ColumnarCollapseFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testCollapseOnColumnarKeyword() throws IOException {
        if (isRunningAgainstOldCluster()) {
            if (clusterHasCapability("PUT", "/{index}", List.of(), List.of("columnar_index_modes")).orElse(false) == false) {
                return;
            }
            Request create = new Request("PUT", "/" + INDEX);
            create.setJsonEntity("""
                {
                  "settings": {
                    "index.mode": "columnar",
                    "index.number_of_shards": 1,
                    "index.number_of_replicas": 0
                  },
                  "mappings": {
                    "properties": {
                      "@timestamp": { "type": "date" },
                      "host.name": { "type": "keyword" }
                    }
                  }
                }""");
            assertOK(client().performRequest(create));

            Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity("""
                { "index": {} }
                { "@timestamp": "2024-01-01T00:00:00Z", "host.name": "host-a" }
                { "index": {} }
                { "@timestamp": "2024-01-02T00:00:00Z", "host.name": "host-b" }
                { "index": {} }
                { "@timestamp": "2024-01-03T00:00:00Z", "host.name": "host-a" }
                """);
            Response bulkResponse = client().performRequest(bulk);
            assertOK(bulkResponse);
            assertThat(entityAsMap(bulkResponse).get("errors"), Matchers.is(false));
        } else {
            assumeTrue("the old cluster did not support columnar index modes", indexExists(INDEX));
            Request search = new Request("POST", "/" + INDEX + "/_search");
            search.setJsonEntity("""
                {
                  "collapse": { "field": "host.name" },
                  "sort": [ { "@timestamp": "asc" } ],
                  "size": 10
                }""");
            Map<String, Object> response = entityAsMap(client().performRequest(search));

            assertThat(ObjectPath.<Integer>evaluate(response, "hits.total.value"), Matchers.equalTo(3));
            List<?> hits = ObjectPath.evaluate(response, "hits.hits");
            assertThat(hits, Matchers.hasSize(2));
            assertThat(ObjectPath.<String>evaluate(hits.get(0), "fields.host\\.name.0"), Matchers.equalTo("host-a"));
            assertThat(ObjectPath.<String>evaluate(hits.get(1), "fields.host\\.name.0"), Matchers.equalTo("host-b"));
        }
    }
}
