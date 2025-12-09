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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;

/**
 * This IT indexes some dense vector on an old node, then update its mapping and, once upgraded, checks that KNN search still works
 * before and after further data indexing.
 */
public class DenseVectorMappingUpdateIT extends AbstractRollingUpgradeTestCase {

    private static String generateBulkData(int upgradedNodes, int dimensions) {
        StringBuilder sb = new StringBuilder();

        int[] vals = new int[dimensions];
        Arrays.fill(vals, 1);

        // 1-10, 11-20, 21-30...
        IntStream docs = IntStream.rangeClosed(1 + (upgradedNodes * 10), (upgradedNodes + 1) * 10);

        for (var it = docs.iterator(); it.hasNext();) {
            vals[upgradedNodes]++;

            sb.append("{\"index\": {\"_id\": \"").append(it.nextInt()).append("\"}}");
            sb.append(System.lineSeparator());
            sb.append("{\"embedding\": ").append(Arrays.toString(vals)).append("}");
            sb.append(System.lineSeparator());
        }

        return sb.toString();
    }

    private final int upgradedNodes;

    public DenseVectorMappingUpdateIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
        this.upgradedNodes = upgradedNodes;
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
            index.setJsonEntity(generateBulkData(upgradedNodes, 8));
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
            index.setJsonEntity(generateBulkData(upgradedNodes, 8));
            assertOK(client().performRequest(index));
            expectedCount = 20;
            assertCount(indexName, expectedCount);
        }
    }

    private record Index(boolean index, String type, Collection<ElementType> elementTypes, boolean[] directIO) {}

    private static final List<ElementType> ALL_ELEMENT_TYPES = List.of(ElementType.values());
    private static final List<ElementType> FLOAT_ELEMENT_TYPES = List.of(ElementType.FLOAT, ElementType.BFLOAT16);
    private static final boolean[] SUPPORTS_DIRECT_IO = new boolean[] { false, true };
    private static final boolean[] NO_DIRECT_IO = new boolean[] { false };

    private static final List<Index> INDEXES = List.of(
        new Index(false, null, ALL_ELEMENT_TYPES, NO_DIRECT_IO),
        new Index(true, null, ALL_ELEMENT_TYPES, NO_DIRECT_IO),
        new Index(true, "hnsw", ALL_ELEMENT_TYPES, NO_DIRECT_IO),
        new Index(true, "int8_hnsw", FLOAT_ELEMENT_TYPES, SUPPORTS_DIRECT_IO),
        new Index(true, "int4_hnsw", FLOAT_ELEMENT_TYPES, SUPPORTS_DIRECT_IO),
        new Index(true, "flat", ALL_ELEMENT_TYPES, NO_DIRECT_IO),
        new Index(true, "int8_flat", FLOAT_ELEMENT_TYPES, NO_DIRECT_IO),
        new Index(true, "int4_flat", FLOAT_ELEMENT_TYPES, NO_DIRECT_IO),
        new Index(true, "bbq_hnsw", FLOAT_ELEMENT_TYPES, SUPPORTS_DIRECT_IO),
        new Index(true, "bbq_flat", FLOAT_ELEMENT_TYPES, NO_DIRECT_IO)
        //new Index(true, "bbq_disk", FLOAT_ELEMENT_TYPES, NO_DIRECT_IO)
    );

    public void testDenseVectorIndexOverUpgrade() throws IOException {
        if (isOldCluster()) {
            boolean useSyntheticSource = randomBoolean();

            for (Index i : INDEXES) {
                for (ElementType elementType : i.elementTypes()) {
                    for (boolean directIO : i.directIO()) {
                        var dims = getDimensions(i.type(), elementType, directIO);
                        if (dims.isEmpty()) {
                            continue;
                        }
                        Request createIndex = new Request("PUT", "/" + indexName(i, elementType, directIO));

                        XContentBuilder payload = XContentBuilder.builder(XContentType.JSON.xContent()).startObject();
                        if (useSyntheticSource) {
                            payload.startObject("settings").field("index.mapping.source.mode", "synthetic").endObject();
                        }
                        payload.startObject("mappings");
                        payload.startObject("properties")
                            .startObject("embedding")
                            .field("type", "dense_vector")
                            .field("element_type", elementType)
                            .field("index", i.index())
                            .field("dims", elementType == ElementType.BIT ? dims.getAsInt() * 8 : dims.getAsInt());
                        if (i.index()) {
                            payload.field("similarity", "l2_norm");
                        }
                        if (i.type() != null) {
                            payload.startObject("index_options").field("type", i.type());
                            if (directIO) {
                                payload.field("on_disk_rescore", true);
                            }
                                payload.endObject();
                        }
                        payload.endObject().endObject().endObject().endObject();
                        createIndex.setJsonEntity(Strings.toString(payload));
                        client().performRequest(createIndex);
                    }
                }
            }
        }

        for (Index i : INDEXES) {
            for (ElementType elementType : i.elementTypes()) {
                for (boolean directIO : i.directIO()) {
                    var dims = getDimensions(i.type(), elementType, directIO);
                    if (dims.isEmpty()) {
                        continue;
                    }
                    String indexName = indexName(i, elementType, directIO);

                    Request index = new Request("POST", "/" + indexName + "/_bulk/");
                    index.addParameter("refresh", "true");
                    index.setJsonEntity(generateBulkData(upgradedNodes, dims.getAsInt()));
                    assertOK(client().performRequest(index));

                    assertCount(indexName, (upgradedNodes + 1) * 10);
                }
            }
        }
    }

    private static String indexName(Index i, ElementType elementType, boolean directIO) {
        if (i.index() == false) {
            return "test_nonindexed_" + elementType;
        }
        String index = "test_index_" + i.type() + "_" + elementType;
        if (directIO) {
            index += "_directio";
        }
        return index;
    }

    private static OptionalInt getDimensions(String type, ElementType elementType, boolean directIO) {
        if (type != null && type.equals("bbq_disk") && oldClusterHasFeature(MapperFeatures.BBQ_DISK_SUPPORT) == false) {
            return OptionalInt.empty();
        }
        if (oldClusterHasFeature(MapperFeatures.GENERIC_VECTOR_FORMAT) == false && (directIO || elementType == ElementType.BFLOAT16)) {
            return OptionalInt.empty();
        }
        if (type != null && type.startsWith("bbq_")) {
            return OptionalInt.of(64);
        }
        return OptionalInt.of(8);
    }

    private void assertCount(String index, int count) throws IOException {
        Request searchTestIndexRequest = new Request("POST", "/" + index + "/_search");
        searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        searchTestIndexRequest.addParameter("filter_path", "hits.total");
        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        assertEquals(
            "Failed on index " + index,
            "{\"hits\":{\"total\":" + count + "}}",
            EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8)
        );
    }
}
