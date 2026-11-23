/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedChildren;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * The query Lucene can filter, and the same query where nested queries are refused, return the same parents. The index
 * holds a null price, a match past one page of children, and a second index whose keyword normalizer would make a
 * pushed term agree with a different value than the one the compute path reads.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class NestedAnyOptimizedMatchesFallbackIT extends AbstractEsqlIntegTestCase {
    private static final String PRODUCTS = """
        FROM products
        | EVAL h = NESTED_ANY(items, items.sku == "a" AND items.qty * items.price > 100)
        | KEEP id, h
        | SORT id""";
    private static final String MIXED = """
        FROM plain,normalized
        | EVAL h = NESTED_ANY(items, items.sku == "A" AND items.qty * items.price > 100)
        | KEEP id, h
        | SORT id""";

    private static final String MAPPING = """
        {
          "properties": {
            "id": {"type": "keyword"},
            "items": {
              "type": "nested",
              "properties": {
                "sku": {"type": "keyword"},
                "qty": {"type": "long"},
                "price": {"type": "long"}
              }
            }
          }
        }
        """;

    @Before
    public void createIndices() {
        assumeTrue("NESTED_ANY is snapshot-only", Build.current().isSnapshot());
        assertAcked(prepareCreate("products").setSettings(shards()).setMapping(MAPPING));
        put("products", "empty", "{\"id\":\"empty\"}");
        put("products", "match", "{\"id\":\"match\",\"items\":[{\"sku\":\"a\",\"qty\":10,\"price\":20}]}");
        put("products", "nullprice", "{\"id\":\"nullprice\",\"items\":[{\"sku\":\"a\",\"qty\":10}]}");
        put(
            "products",
            "split",
            "{\"id\":\"split\",\"items\":[" + "{\"sku\":\"a\",\"qty\":1,\"price\":10}," + "{\"sku\":\"b\",\"qty\":50,\"price\":50}" + "]}"
        );
        put("products", "page", pageBoundaryDocument());

        assertAcked(prepareCreate("plain").setSettings(shards()).setMapping(MAPPING));
        put("plain", "plain", "{\"id\":\"plain\",\"items\":[{\"sku\":\"A\",\"qty\":10,\"price\":20}]}");

        assertAcked(
            prepareCreate("normalized").setSettings(
                Settings.builder()
                    .put(shards())
                    .put("index.analysis.normalizer.lowercase_normalizer.type", "custom")
                    .putList("index.analysis.normalizer.lowercase_normalizer.filter", "lowercase")
                    .build()
            ).setMapping(normalizedMapping())
        );
        put("normalized", "normalized", "{\"id\":\"normalized\",\"items\":[{\"sku\":\"A\",\"qty\":10,\"price\":20}]}");
        refresh("products", "plain", "normalized");
    }

    @After
    public void resetExpensiveQueries() {
        updateClusterSettings(Settings.builder().putNull(SearchService.ALLOW_EXPENSIVE_QUERIES.getKey()));
    }

    public void testOptimizedNestedAnyMatchesTheComputeFallback() {
        List<List<Object>> products = List.of(
            List.of("empty", false),
            List.of("match", true),
            List.of("nullprice", false),
            List.of("page", true),
            List.of("split", false)
        );
        assertThat(rows(PRODUCTS), equalTo(products));
        updateClusterSettings(Settings.builder().put(SearchService.ALLOW_EXPENSIVE_QUERIES.getKey(), false));
        assertThat(rows(PRODUCTS), equalTo(products));

        updateClusterSettings(Settings.builder().putNull(SearchService.ALLOW_EXPENSIVE_QUERIES.getKey()));
        List<List<Object>> mixed = List.of(List.of("normalized", false), List.of("plain", true));
        assertThat(rows(MIXED), equalTo(mixed));
        updateClusterSettings(Settings.builder().put(SearchService.ALLOW_EXPENSIVE_QUERIES.getKey(), false));
        assertThat(rows(MIXED), equalTo(mixed));
    }

    private static Settings shards() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }

    /** The child that satisfies both is the first one past {@link NestedChildren#PAGE_SIZE}. */
    private static String pageBoundaryDocument() {
        StringBuilder source = new StringBuilder("{\"id\":\"page\",\"items\":[");
        for (int i = 0; i < NestedChildren.PAGE_SIZE; i++) {
            if (i > 0) {
                source.append(',');
            }
            source.append("{\"sku\":\"b\",\"qty\":1,\"price\":1}");
        }
        source.append(",{\"sku\":\"a\",\"qty\":10,\"price\":20}]}");
        return source.toString();
    }

    private static String normalizedMapping() {
        return MAPPING.replace(
            "\"sku\": {\"type\": \"keyword\"}",
            "\"sku\": {\"type\": \"keyword\", \"normalizer\": \"lowercase_normalizer\"}"
        );
    }

    private void put(String index, String id, String source) {
        prepareIndex(index).setId(id).setSource(source, XContentType.JSON).get();
    }

    private List<List<Object>> rows(String query) {
        try (EsqlQueryResponse response = run(query)) {
            return getValuesList(response);
        }
    }
}
