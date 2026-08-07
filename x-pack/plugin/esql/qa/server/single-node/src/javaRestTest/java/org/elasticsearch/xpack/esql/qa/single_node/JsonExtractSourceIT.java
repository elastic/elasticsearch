/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for {@code JSON_EXTRACT} with {@code _source} metadata field.
 * Tests that JSON_EXTRACT correctly handles _source stored in all four XContent encodings.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class JsonExtractSourceIT extends RestEsqlTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @ParametersFactory(argumentFormatting = "%1s")
    public static List<Object[]> modes() {
        return Arrays.stream(Mode.values()).map(m -> new Object[] { m }).toList();
    }

    public JsonExtractSourceIT(Mode mode) {
        super(mode);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testJsonExtractFromSourceJson() throws IOException {
        verifyJsonExtractFromSource(XContentType.JSON);
    }

    public void testJsonExtractFromSourceSmile() throws IOException {
        verifyJsonExtractFromSource(XContentType.SMILE);
    }

    public void testJsonExtractFromSourceCbor() throws IOException {
        verifyJsonExtractFromSource(XContentType.CBOR);
    }

    public void testJsonExtractFromSourceYaml() throws IOException {
        verifyJsonExtractFromSource(XContentType.YAML);
    }

    private void verifyJsonExtractFromSource(XContentType xContentType) throws IOException {
        String index = "test_json_extract_" + xContentType.name().toLowerCase(Locale.ROOT);
        createIndex(index);

        indexDoc(index, "1", xContentType, b -> {
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

        indexDoc(index, "2", xContentType, b -> {
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

        // Simple field extraction
        var result = runEsql(
            requestObjectBuilder().query("FROM " + index + " METADATA _source | EVAL n = JSON_EXTRACT(_source, \"name\") | KEEP n | SORT n")
        );
        assertResultMap(result, List.of(Map.of("name", "n", "type", "keyword")), List.of(List.of("Alice"), List.of("Bob")));

        // Numeric field extraction (returned as keyword string)
        result = runEsql(
            requestObjectBuilder().query("FROM " + index + " METADATA _source | EVAL a = JSON_EXTRACT(_source, \"age\") | KEEP a | SORT a")
        );
        assertResultMap(result, List.of(Map.of("name", "a", "type", "keyword")), List.of(List.of("25"), List.of("30")));

        // Nested field extraction
        result = runEsql(
            requestObjectBuilder().query(
                "FROM " + index + " METADATA _source | EVAL city = JSON_EXTRACT(_source, \"address.city\") | KEEP city | SORT city"
            )
        );
        assertResultMap(result, List.of(Map.of("name", "city", "type", "keyword")), List.of(List.of("London"), List.of("Paris")));

        // Array index extraction
        result = runEsql(
            requestObjectBuilder().query(
                "FROM " + index + " METADATA _source | EVAL tag = JSON_EXTRACT(_source, \"tags[0]\") | KEEP tag | SORT tag"
            )
        );
        assertResultMap(result, List.of(Map.of("name", "tag", "type", "keyword")), List.of(List.of("admin"), List.of("user")));

        // Object extraction (returned as JSON string)
        result = runEsql(
            requestObjectBuilder().query(
                "FROM " + index + " METADATA _source | EVAL addr = JSON_EXTRACT(_source, \"address\") | KEEP addr | SORT addr"
            )
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values.size(), equalTo(2));
        String addr1 = (String) values.get(0).get(0);
        String addr2 = (String) values.get(1).get(0);
        assertThat(addr1, containsString("\"city\""));
        assertThat(addr1, containsString("\"zip\""));
        assertThat(addr2, containsString("\"city\""));
        assertThat(addr2, containsString("\"zip\""));

        assertThat(deleteIndex(index).isAcknowledged(), equalTo(true));
    }

    private void indexDoc(String index, String id, XContentType xContentType, CheckedConsumer<XContentBuilder> content) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            content.accept(builder);
            builder.endObject();
            BytesReference bytes = BytesReference.bytes(builder);
            Request request = new Request("PUT", "/" + index + "/_doc/" + id);
            request.addParameter("refresh", "true");
            request.setEntity(
                new InputStreamEntity(bytes.streamInput(), bytes.length(), ContentType.create(xContentType.mediaTypeWithoutParameters()))
            );
            assertOK(client().performRequest(request));
        }
    }

    @FunctionalInterface
    private interface CheckedConsumer<T> {
        void accept(T t) throws IOException;
    }
}
