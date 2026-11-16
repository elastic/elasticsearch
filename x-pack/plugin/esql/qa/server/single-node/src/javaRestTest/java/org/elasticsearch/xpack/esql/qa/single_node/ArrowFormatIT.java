/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ArrowFormatIT extends ESRestTestCase {

    private static final RootAllocator ALLOCATOR = new RootAllocator();

    @AfterClass
    public static void afterClass() {
        ALLOCATOR.close();
    }

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    @Before
    public void initIndex() throws IOException {
        Request request = new Request("PUT", "/arrow-test");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "value": {
                    "type": "integer"
                  },
                  "description": {
                    "type": "keyword"
                  },
                  "ip": {
                    "type": "ip"
                  },
                  "v": {
                    "type": "version"
                  }
                }
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/_bulk?index=arrow-test&refresh=true");
        // 4 documents with a null in the middle, leading to 3 ESQL pages and 3 Arrow batches
        request.setJsonEntity("""
            {"index": {"_id": "1"}}
            {"value": 1, "ip": "192.168.0.1", "v": "1.0.1", "description": "number one"}
            {"index": {"_id": "2"}}
            {"value": 2, "ip": "192.168.0.2", "v": "1.0.2", "description": "number two"}
            {"index": {"_id": "3"}}
            {"value": 3, "ip": "2001:db8::1:0:0:1"}
            {"index": {"_id": "4"}}
            {"value": 4, "ip": "::afff:4567:890a", "v": "1.0.4", "description": "number four"}
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private VectorSchemaRoot esql(String query) throws IOException {
        return esql(query, false);
    }

    private VectorSchemaRoot esql(String query, boolean dropNullColumns) throws IOException {
        Request request = new Request("POST", "/_query?format=arrow&drop_null_columns=" + dropNullColumns);
        request.setJsonEntity(query);
        Response response = client().performRequest(request);

        assertEquals("application/vnd.apache.arrow.stream", response.getEntity().getContentType().getValue());
        return readArrow(response.getEntity().getContent());
    }

    public void testInteger() throws Exception {
        try (VectorSchemaRoot root = esql("""
            {
                "query": "FROM arrow-test | SORT value | LIMIT 100 | KEEP value"
            }""")) {
            List<Field> fields = root.getSchema().getFields();
            assertEquals(1, fields.size());

            assertValues(root);
        }
    }

    public void testString() throws Exception {
        try (VectorSchemaRoot root = esql("""
            {
                "query": "FROM arrow-test | SORT value | LIMIT 100 | KEEP description"
            }""")) {
            List<Field> fields = root.getSchema().getFields();
            assertEquals(1, fields.size());

            assertDescription(root);
        }
    }

    public void testIp() throws Exception {
        try (VectorSchemaRoot root = esql("""
            {
                "query": "FROM arrow-test | SORT value | LIMIT 100 | KEEP ip"
            }""")) {
            List<Field> fields = root.getSchema().getFields();
            assertEquals(1, fields.size());

            assertIp(root);
        }
    }

    public void testVersion() throws Exception {
        try (VectorSchemaRoot root = esql("""
            {
                "query": "FROM arrow-test | SORT value | LIMIT 100 | KEEP v"
            }""")) {
            List<Field> fields = root.getSchema().getFields();
            assertEquals(1, fields.size());

            assertVersion(root);
        }
    }

    public void testEverything() throws Exception {
        try (VectorSchemaRoot root = esql("""
            {
                "query": "FROM arrow-test | SORT value | LIMIT 100"
            }""")) {
            List<Field> fields = root.getSchema().getFields();
            assertEquals(4, fields.size());

            assertDescription(root);
            assertValues(root);
            assertIp(root);
            assertVersion(root);
        }
    }

    public void testDropNullColumns() throws Exception {
        try (VectorSchemaRoot root = esql("""
            {
                "query": "FROM arrow-test | SORT value | LIMIT 100 | EVAL empty = null::keyword | KEEP empty, description, value"
            }""", true)) {
            List<Field> fields = root.getSchema().getFields();
            assertEquals(List.of("description", "value"), fields.stream().map(Field::getName).toList());

            assertDescription(root);
            assertValues(root);
        }
    }

    public void testAggregateMetricDouble() throws Exception {
        Request request = new Request("PUT", "/arrow-aggregate-test");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "value": { "type": "integer" },
                  "metric": { "type": "aggregate_metric_double", "metrics": ["min", "max", "sum", "value_count"] }
                }
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/_bulk?index=arrow-aggregate-test&refresh=true");
        request.setJsonEntity("""
            {"index": {"_id": "1"}}
            {"value": 1, "metric": {"min": 1.0, "max": 3.0, "sum": 6.0, "value_count": 3}}
            {"index": {"_id": "2"}}
            {"value": 2}
            {"index": {"_id": "3"}}
            {"value": 3, "metric": {"min": -2.0, "max": 0.0, "sum": -2.0, "value_count": 2}}
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        try (VectorSchemaRoot root = esql("""
            {
                "query": "FROM arrow-aggregate-test | SORT value | LIMIT 100 | KEEP metric"
            }""")) {
            assertEquals(3, root.getRowCount());
            assertEquals(1, root.getSchema().getFields().size());
            StructVector metric = (StructVector) root.getVector("metric");
            Field field = metric.getField();
            assertEquals(Types.MinorType.STRUCT.getType(), field.getType());
            assertTrue(field.isNullable());
            assertEquals("aggregate_metric_double", field.getMetadata().get("elastic:type"));
            assertEquals(List.of("min", "max", "sum", "value_count"), field.getChildren().stream().map(Field::getName).toList());
            for (Field child : field.getChildren()) {
                assertTrue(child.isNullable());
                assertEquals(
                    child.getName().equals("value_count") ? Types.MinorType.INT.getType() : Types.MinorType.FLOAT8.getType(),
                    child.getType()
                );
            }

            Float8Vector min = (Float8Vector) metric.getChild("min");
            Float8Vector max = (Float8Vector) metric.getChild("max");
            Float8Vector sum = (Float8Vector) metric.getChild("sum");
            IntVector count = (IntVector) metric.getChild("value_count");
            assertFalse(metric.isNull(0));
            assertEquals(1.0, min.get(0), 0.0);
            assertEquals(3.0, max.get(0), 0.0);
            assertEquals(6.0, sum.get(0), 0.0);
            assertEquals(3, count.get(0));
            assertTrue(metric.isNull(1));
            assertTrue(min.isNull(1));
            assertTrue(max.isNull(1));
            assertTrue(sum.isNull(1));
            assertTrue(count.isNull(1));
            assertFalse(metric.isNull(2));
            assertEquals(-2.0, min.get(2), 0.0);
            assertEquals(0.0, max.get(2), 0.0);
            assertEquals(-2.0, sum.get(2), 0.0);
            assertEquals(2, count.get(2));
        }
    }

    private VectorSchemaRoot readArrow(InputStream input) throws IOException {
        try (
            ArrowStreamReader reader = new ArrowStreamReader(input, ALLOCATOR);
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        ) {
            VectorSchemaRoot root = VectorSchemaRoot.create(readerRoot.getSchema(), ALLOCATOR);
            root.allocateNew();

            while (reader.loadNextBatch()) {
                VectorSchemaRootAppender.append(root, readerRoot);
            }

            return root;
        }
    }

    private void assertValues(VectorSchemaRoot root) {
        var valueVector = (IntVector) root.getVector("value");
        assertEquals(1, valueVector.get(0));
        assertEquals(2, valueVector.get(1));
        assertEquals(3, valueVector.get(2));
        assertEquals(4, valueVector.get(3));
    }

    private void assertDescription(VectorSchemaRoot root) {
        var descVector = (VarCharVector) root.getVector("description");
        assertEquals("number one", descVector.getObject(0).toString());
        assertEquals("number two", descVector.getObject(1).toString());
        assertTrue(descVector.isNull(2));
        assertEquals("number four", descVector.getObject(3).toString());
    }

    private void assertIp(VectorSchemaRoot root) {
        // Test data that has been transformed during output (ipV4 truncated to 32bits)
        var ipVector = (VarBinaryVector) root.getVector("ip");
        assertArrayEquals(new byte[] { (byte) 192, (byte) 168, 0, 1 }, ipVector.getObject(0));
        assertArrayEquals(new byte[] { (byte) 192, (byte) 168, 0, 2 }, ipVector.getObject(1));
        assertArrayEquals(
            new byte[] { 0x20, 0x01, 0x0d, (byte) 0xb8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 },
            ipVector.getObject(2)
        );
        assertArrayEquals(
            new byte[] {
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                (byte) 0xaf,
                (byte) 0xff,
                0x45,
                0x67,
                (byte) 0x89,
                0x0A },
            ipVector.getObject(3)
        );
    }

    private void assertVersion(VectorSchemaRoot root) {
        // Version is binary-encoded in ESQL vectors, turned into a string in arrow output
        var versionVector = (VarCharVector) root.getVector("v");
        assertEquals("1.0.1", versionVector.getObject(0).toString());
        assertEquals("1.0.2", versionVector.getObject(1).toString());
        assertTrue(versionVector.isNull(2));
        assertEquals("1.0.4", versionVector.getObject(3).toString());
    }
}
