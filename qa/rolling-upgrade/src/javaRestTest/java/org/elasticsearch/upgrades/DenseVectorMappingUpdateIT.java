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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.MapperFeatures;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;

/**
 * This IT indexes some dense vector on an old node, then update its mapping and, once upgraded, checks that KNN search still works
 * before and after further data indexing.
 */
public class DenseVectorMappingUpdateIT extends AbstractRollingUpgradeTestCase {

    private static final String SYNTHETIC_SOURCE_FEATURE = "gte_v8.12.0";

    private static final String BULK1 = """
                    {"index": {"_id": "1"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 1]}
                    {"index": {"_id": "2"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 2]}
                    {"index": {"_id": "3"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 3]}
                    {"index": {"_id": "4"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 4]}
                    {"index": {"_id": "5"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 5]}
                    {"index": {"_id": "6"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 6]}
                    {"index": {"_id": "7"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 7]}
                    {"index": {"_id": "8"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 8]}
                    {"index": {"_id": "9"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 9]}
                    {"index": {"_id": "10"}}
                    {"embedding": [1, 1, 1, 1, 1, 1, 1, 10]}
        """;

    private static final String BULK1_BIT = """
                    {"index": {"_id": "1"}}
                    {"embedding": [1]}
                    {"index": {"_id": "2"}}
                    {"embedding": [2]}
                    {"index": {"_id": "3"}}
                    {"embedding": [3]}
                    {"index": {"_id": "4"}}
                    {"embedding": [4]}
                    {"index": {"_id": "5"}}
                    {"embedding": [5]}
                    {"index": {"_id": "6"}}
                    {"embedding": [6]}
                    {"index": {"_id": "7"}}
                    {"embedding": [7]}
                    {"index": {"_id": "8"}}
                    {"embedding": [8]}
                    {"index": {"_id": "9"}}
                    {"embedding": [9]}
                    {"index": {"_id": "10"}}
                    {"embedding": [10]}
        """;

    private static final String BULK2 = """
                    {"index": {"_id": "11"}}
                    {"embedding": [1, 1, 1, 1, 1, 0, 1, 1]}
                    {"index": {"_id": "12"}}
                    {"embedding": [1, 1, 1, 1, 1, 2, 1, 1]}
                    {"index": {"_id": "13"}}
                    {"embedding": [1, 1, 1, 1, 1, 3, 1, 1]}
                    {"index": {"_id": "14"}}
                    {"embedding": [1, 1, 1, 1, 1, 4, 1, 1]}
                    {"index": {"_id": "15"}}
                    {"embedding": [1, 1, 1, 1, 1, 5, 1, 1]}
                    {"index": {"_id": "16"}}
                    {"embedding": [1, 1, 1, 1, 1, 6, 1, 1]}
                    {"index": {"_id": "17"}}
                    {"embedding": [1, 1, 1, 1, 1, 7, 1, 1]}
                    {"index": {"_id": "18"}}
                    {"embedding": [1, 1, 1, 1, 1, 8, 1, 1]}
                    {"index": {"_id": "19"}}
                    {"embedding": [1, 1, 1, 1, 1, 9, 1, 1]}
                    {"index": {"_id": "20"}}
                    {"embedding": [1, 1, 1, 1, 1, 10, 1, 1]}
        """;
    private static final String BULK2_BIT = """
                    {"index": {"_id": "11"}}
                    {"embedding": [101]}
                    {"index": {"_id": "12"}}
                    {"embedding": [102]}
                    {"index": {"_id": "13"}}
                    {"embedding": [103]}
                    {"index": {"_id": "14"}}
                    {"embedding": [104]}
                    {"index": {"_id": "15"}}
                    {"embedding": [105]}
                    {"index": {"_id": "16"}}
                    {"embedding": [106]}
                    {"index": {"_id": "17"}}
                    {"embedding": [107]}
                    {"index": {"_id": "18"}}
                    {"embedding": [108]}
                    {"index": {"_id": "19"}}
                    {"embedding": [109]}
                    {"index": {"_id": "20"}}
                    {"embedding": [110]}
        """;

    public DenseVectorMappingUpdateIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testDenseVectorMappingUpdateOnOldCluster() throws IOException {
        String indexName = "test_index_type_change";
        if (isOldCluster()) {
            Request createIndex = new Request("PUT", "/" + indexName);
            boolean useSyntheticSource = randomBoolean();

            XContentBuilder payload = XContentBuilder.builder(XContentType.JSON.xContent()).startObject();
            if (useSyntheticSource) {
                payload.startObject("settings").field("index.mapping.source.mode", "synthetic").endObject();
            }
            payload.startObject("mappings");
            payload.startObject("properties")
                .startObject("embedding")
                .field("type", "dense_vector")
                .field("index", "true")
                .field("dims", 8)
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

        if (isUpgradedCluster()) {
            Request updateMapping = new Request("PUT", "/" + indexName + "/_mapping");
            XContentBuilder mappings = XContentBuilder.builder(XContentType.JSON.xContent())
                .startObject()
                .startObject("properties")
                .startObject("embedding")
                .field("type", "dense_vector")
                .field("index", "true")
                .field("dims", 8)
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

    private record Index(String type, Set<String> elementTypes) {}

    private static final Set<String> ALL_ELEMENT_TYPES = Stream.of(DenseVectorFieldMapper.ElementType.values())
        .map(Object::toString)
        .collect(Collectors.toUnmodifiableSet());
    private static final Set<Index> INDEXES = Set.of(
        new Index(null, ALL_ELEMENT_TYPES),
        new Index("hnsw", ALL_ELEMENT_TYPES),
        new Index("int8_hnsw", Set.of("float", "bfloat16")),
        new Index("int4_hnsw", Set.of("float", "bfloat16")),
        new Index("flat", ALL_ELEMENT_TYPES),
        new Index("int8_flat", Set.of("float", "bfloat16")),
        new Index("int4_flat", Set.of("float", "bfloat16"))
        // new Index("bbq_hnsw", Set.of("float", "bfloat16")),
        // new Index("bbq_flat", Set.of("float", "bfloat16"))
    );

    public void testDenseVectorIndexOverUpgrade() throws IOException {
        if (isOldCluster()) {
            boolean useSyntheticSource = randomBoolean();

            for (Index i : INDEXES) {
                for (String elementType : i.elementTypes()) {
                    if (clusterSupportsIndex(i.type(), elementType) == false) {
                        continue;
                    }

                    String indexName = "test_index_" + i.type() + "_" + elementType;
                    Request createIndex = new Request("PUT", "/" + indexName);

                    XContentBuilder payload = XContentBuilder.builder(XContentType.JSON.xContent()).startObject();
                    if (useSyntheticSource) {
                        payload.startObject("settings").field("index.mapping.source.mode", "synthetic").endObject();
                    }
                    payload.startObject("mappings");
                    payload.startObject("properties")
                        .startObject("embedding")
                        .field("type", "dense_vector")
                        .field("element_type", elementType)
                        .field("index", "true")
                        .field("dims", 8)
                        .field("similarity", "l2_norm");
                    if (i.type() != null) {
                        payload.startObject("index_options").field("type", i.type()).endObject();
                    }
                    payload.endObject().endObject().endObject().endObject();
                    createIndex.setJsonEntity(Strings.toString(payload));
                    client().performRequest(createIndex);
                    Request index = new Request("POST", "/" + indexName + "/_bulk/");
                    index.addParameter("refresh", "true");
                    index.setJsonEntity(elementType.equals("bit") ? BULK1_BIT : BULK1);
                    client().performRequest(index);

                    assertCount(indexName, 10);
                }
            }
        }

        if (isUpgradedCluster()) {
            for (Index i : INDEXES) {
                for (String elementType : i.elementTypes()) {
                    if (clusterSupportsIndex(i.type(), elementType) == false) {
                        continue;
                    }
                    String indexName = "test_index_" + i.type() + "_" + elementType;

                    Request index = new Request("POST", "/" + indexName + "/_bulk/");
                    index.addParameter("refresh", "true");
                    index.setJsonEntity(elementType.equals("bit") ? BULK2_BIT : BULK2);
                    assertOK(client().performRequest(index));

                    assertCount(indexName, 20);
                }
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

    private static boolean clusterSupportsIndex(String type, String elementType) {
        if (elementType.equals("bfloat16") && oldClusterHasFeature(MapperFeatures.GENERIC_VECTOR_FORMAT) == false) {
            return false;
        }
        return true;
    }
}
