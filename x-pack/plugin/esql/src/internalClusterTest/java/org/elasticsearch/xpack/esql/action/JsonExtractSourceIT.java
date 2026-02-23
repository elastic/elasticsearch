/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

/**
 * Integration tests for {@code JSON_EXTRACT} with {@code _source} metadata field.
 * Tests that JSON_EXTRACT correctly handles _source stored in all four XContent encodings.
 */
public class JsonExtractSourceIT extends AbstractEsqlIntegTestCase {

    public void testJsonExtractFromSourceJson() throws IOException {
        doTestJsonExtractFromSource(XContentType.JSON);
    }

    public void testJsonExtractFromSourceSmile() throws IOException {
        doTestJsonExtractFromSource(XContentType.SMILE);
    }

    public void testJsonExtractFromSourceCbor() throws IOException {
        doTestJsonExtractFromSource(XContentType.CBOR);
    }

    public void testJsonExtractFromSourceYaml() throws IOException {
        doTestJsonExtractFromSource(XContentType.YAML);
    }

    private void doTestJsonExtractFromSource(XContentType xContentType) throws IOException {
        String index = "test_json_extract_" + xContentType.name().toLowerCase();
        createIndex(index, Settings.builder().put("index.number_of_shards", 1).build());

        // Index documents using the specified XContent encoding
        BytesReference doc1 = buildSource(xContentType, b -> {
            b.field("name", "Alice");
            b.field("age", 30);
            b.startObject("address");
            {
                b.field("city", "London");
                b.field("zip", "EC1A");
            }
            b.endObject();
            b.startArray("tags");
            {
                b.value("admin");
                b.value("user");
            }
            b.endArray();
        });

        BytesReference doc2 = buildSource(xContentType, b -> {
            b.field("name", "Bob");
            b.field("age", 25);
            b.startObject("address");
            {
                b.field("city", "Paris");
                b.field("zip", "75001");
            }
            b.endObject();
            b.startArray("tags");
            {
                b.value("user");
            }
            b.endArray();
        });

        client().prepareBulk(index)
            .add(new IndexRequest(index).id("1").source(doc1, xContentType))
            .add(new IndexRequest(index).id("2").source(doc2, xContentType))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        ensureYellow(index);

        // Simple field extraction
        try (var resp = run("FROM " + index + " METADATA _source | EVAL n = JSON_EXTRACT(_source, \"name\") | KEEP n | SORT n")) {
            assertColumnNames(resp.columns(), List.of("n"));
            assertColumnTypes(resp.columns(), List.of("keyword"));
            assertValues(resp.values(), List.of(List.of("Alice"), List.of("Bob")));
        }

        // Numeric field extraction (returned as keyword string)
        try (var resp = run("FROM " + index + " METADATA _source | EVAL a = JSON_EXTRACT(_source, \"age\") | KEEP a | SORT a")) {
            assertColumnNames(resp.columns(), List.of("a"));
            assertValues(resp.values(), List.of(List.of("25"), List.of("30")));
        }

        // Nested field extraction
        try (
            var resp = run(
                "FROM " + index + " METADATA _source | EVAL city = JSON_EXTRACT(_source, \"address.city\") | KEEP city | SORT city"
            )
        ) {
            assertValues(resp.values(), List.of(List.of("London"), List.of("Paris")));
        }

        // Array index extraction
        try (var resp = run("FROM " + index + " METADATA _source | EVAL tag = JSON_EXTRACT(_source, \"tags[0]\") | KEEP tag | SORT tag")) {
            assertValues(resp.values(), List.of(List.of("admin"), List.of("user")));
        }

        // Object extraction (returned as JSON string)
        try (
            var resp = run("FROM " + index + " METADATA _source | EVAL addr = JSON_EXTRACT(_source, \"address\") | KEEP addr | SORT addr")
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), org.hamcrest.Matchers.equalTo(2));
            // Both should be valid JSON strings containing city and zip
            String addr1 = (String) values.get(0).get(0);
            String addr2 = (String) values.get(1).get(0);
            assertThat(addr1, org.hamcrest.Matchers.containsString("\"city\""));
            assertThat(addr1, org.hamcrest.Matchers.containsString("\"zip\""));
            assertThat(addr2, org.hamcrest.Matchers.containsString("\"city\""));
            assertThat(addr2, org.hamcrest.Matchers.containsString("\"zip\""));
        }

        assertAcked(indicesAdmin().prepareDelete(index));
    }

    private static BytesReference buildSource(XContentType xContentType, CheckedConsumer<XContentBuilder> content) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            content.accept(builder);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    @FunctionalInterface
    private interface CheckedConsumer<T> {
        void accept(T t) throws IOException;
    }
}
