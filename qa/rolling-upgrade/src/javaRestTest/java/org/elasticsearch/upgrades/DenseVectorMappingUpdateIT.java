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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;

/**
 * This IT indexes some dense vector on an old node, then update its mapping and, once upgraded, checks that KNN search still works
 * before and after further data indexing.
 */
public class DenseVectorMappingUpdateIT extends AbstractRollingUpgradeTestCase {

    private static final String BULK1 = """
                    {"index": {"_id": "1"}}
                    {"embedding": [1, 1, 1, 1]}
                    {"index": {"_id": "2"}}
                    {"embedding": [1, 1, 1, 2]}
                    {"index": {"_id": "3"}}
                    {"embedding": [1, 1, 1, 3]}
                    {"index": {"_id": "4"}}
                    {"embedding": [1, 1, 1, 4]}
                    {"index": {"_id": "5"}}
                    {"embedding": [1, 1, 1, 5]}
                    {"index": {"_id": "6"}}
                    {"embedding": [1, 1, 1, 6]}
                    {"index": {"_id": "7"}}
                    {"embedding": [1, 1, 1, 7]}
                    {"index": {"_id": "8"}}
                    {"embedding": [1, 1, 1, 8]}
                    {"index": {"_id": "9"}}
                    {"embedding": [1, 1, 1, 9]}
                    {"index": {"_id": "10"}}
                    {"embedding": [1, 1, 1, 10]}
        """;

    private static final String BULK2 = """
                    {"index": {"_id": "11"}}
                    {"embedding": [1, 0, 1, 1]}
                    {"index": {"_id": "12"}}
                    {"embedding": [1, 2, 1, 1]}
                    {"index": {"_id": "13"}}
                    {"embedding": [1, 3, 1, 1]}
                    {"index": {"_id": "14"}}
                    {"embedding": [1, 4, 1, 1]}
                    {"index": {"_id": "15"}}
                    {"embedding": [1, 5, 1, 1]}
                    {"index": {"_id": "16"}}
                    {"embedding": [1, 6, 1, 1]}
                    {"index": {"_id": "17"}}
                    {"embedding": [1, 7, 1, 1]}
                    {"index": {"_id": "18"}}
                    {"embedding": [1, 8, 1, 1]}
                    {"index": {"_id": "19"}}
                    {"embedding": [1, 9, 1, 1]}
                    {"index": {"_id": "20"}}
                    {"embedding": [1, 10, 1, 1]}
        """;

    public DenseVectorMappingUpdateIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testDenseVectorMappingUpdateOnOldCluster() throws IOException {
        if (getOldClusterTestVersion().after(Version.V_8_7_0.toString())) {
            String indexName = "test_index";
            if (isOldCluster()) {
                Request createIndex = new Request("PUT", "/" + indexName);
                boolean useSyntheticSource = randomBoolean() && getOldClusterTestVersion().after(Version.V_8_12_0.toString());
                boolean useIndexSetting = SourceFieldMapper.onOrAfterDeprecateModeVersion(getOldClusterIndexVersion());
                XContentBuilder payload = XContentBuilder.builder(XContentType.JSON.xContent()).startObject();
                if (useSyntheticSource) {
                    if (useIndexSetting) {
                        payload.startObject("settings").field("index.mapping.source.mode", "synthetic").endObject();
                    }
                }
                payload.startObject("mappings");
                if (useIndexSetting == false) {
                    payload.startObject("_source");
                    payload.field("mode", "synthetic");
                    payload.endObject();
                }
                payload.startObject("properties")
                    .startObject("embedding")
                    .field("type", "dense_vector")
                    .field("index", "true")
                    .field("dims", 4)
                    .field("similarity", "cosine")
                    .startObject("index_options")
                    .field("type", "hnsw")
                    .field("m", "16")
                    .field("ef_construction", "100")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
                createIndex.setJsonEntity(Strings.toString(payload));
                client().performRequest(createIndex);
                Request index = new Request("POST", "/" + indexName + "/_bulk/");
                index.addParameter("refresh", "true");
                index.setJsonEntity(BULK1);
                client().performRequest(index);
            }

            int expectedCount = 10;

            assertCount(indexName, expectedCount);

            if (isUpgradedCluster() && clusterSupportsDenseVectorTypeUpdate()) {
                Request updateMapping = new Request("PUT", "/" + indexName + "/_mapping");
                XContentBuilder mappings = XContentBuilder.builder(XContentType.JSON.xContent())
                    .startObject()
                    .startObject("properties")
                    .startObject("embedding")
                    .field("type", "dense_vector")
                    .field("index", "true")
                    .field("dims", 4)
                    .field("similarity", "cosine")
                    .startObject("index_options")
                    .field("type", "int8_hnsw")
                    .field("m", "16")
                    .field("ef_construction", "100")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
                updateMapping.setJsonEntity(Strings.toString(mappings));
                assertOK(client().performRequest(updateMapping));
                Request index = new Request("POST", "/" + indexName + "/_bulk/");
                index.addParameter("refresh", "true");
                index.setJsonEntity(BULK2);
                assertOK(client().performRequest(index));
                expectedCount = 20;
                assertCount(indexName, expectedCount);
            }
        }
    }

    private void assertCount(String index, int count) throws IOException {
        Request searchTestIndexRequest = new Request("POST", "/" + index + "/_search");
        searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        searchTestIndexRequest.addParameter("filter_path", "hits.total");
        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        assertEquals(
            "{\"hits\":{\"total\":" + count + "}}",
            EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8)
        );
    }

    private boolean clusterSupportsDenseVectorTypeUpdate() throws IOException {
        Map<?, ?> response = entityAsMap(client().performRequest(new Request("GET", "_nodes")));
        Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");

        Predicate<Map<?, ?>> nodeSupportsBulkApi = n -> Version.fromString(n.get("version").toString()).onOrAfter(Version.V_8_15_0);

        return nodes.values().stream().map(o -> (Map<?, ?>) o).allMatch(nodeSupportsBulkApi);
    }

}
