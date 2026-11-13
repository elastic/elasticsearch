/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.h3.H3;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
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

    @Before
    public void initGeoIndex() throws IOException {
        Request request = new Request("PUT", "/arrow-geo-test");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "value": { "type": "integer" },
                  "location": { "type": "geo_point" }
                }
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/_bulk?index=arrow-geo-test&refresh=true");
        // Document 3 has no location, so that all geo grid columns contain a null
        request.setJsonEntity("""
            {"index": {"_id": "1"}}
            {"value": 1, "location": {"lon": 4.9, "lat": 52.37}}
            {"index": {"_id": "2"}}
            {"value": 2, "location": {"lon": -122.42, "lat": 37.77}}
            {"index": {"_id": "3"}}
            {"value": 3}
            {"index": {"_id": "4"}}
            {"value": 4, "location": {"lon": 151.21, "lat": -33.87}}
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

    /**
     * Geohash is an unsigned 64-bit long (60 bits of hash and 4 bits of level). The expected value is
     * computed from the string representation of the JSON output.
     */
    public void testGeohash() throws Exception {
        var expected = geoGridStrings("ST_GEOHASH(location, 5)");
        try (VectorSchemaRoot root = esql(geoGridQuery("ST_GEOHASH(location, 5)"))) {
            assertEquals(1, root.getSchema().getFields().size());

            var vector = (UInt8Vector) root.getVector("cell");
            assertEquals(4, vector.getValueCount());
            for (int i = 0; i < expected.size(); i++) {
                if (expected.get(i) == null) {
                    assertTrue("row " + i, vector.isNull(i));
                } else {
                    assertEquals("row " + i, Geohash.longEncode(expected.get(i)), vector.get(i));
                }
            }
        }
    }

    /**
     * Geotile is a struct of non-nullable zoom:uint8, x:uint32 and y:uint32 fields. Only the struct itself can be null.
     */
    public void testGeotile() throws Exception {
        var expected = geoGridStrings("ST_GEOTILE(location, 6)");
        try (VectorSchemaRoot root = esql(geoGridQuery("ST_GEOTILE(location, 6)"))) {
            assertEquals(1, root.getSchema().getFields().size());

            var vector = (StructVector) root.getVector("cell");
            assertEquals(List.of("zoom", "x", "y"), vector.getField().getChildren().stream().map(Field::getName).toList());
            for (Field child : vector.getField().getChildren()) {
                assertFalse("field [" + child.getName() + "] must not be nullable", child.isNullable());
            }

            var zoom = (UInt1Vector) vector.getChild("zoom");
            var x = (UInt4Vector) vector.getChild("x");
            var y = (UInt4Vector) vector.getChild("y");

            assertEquals(4, vector.getValueCount());
            for (int i = 0; i < expected.size(); i++) {
                if (expected.get(i) == null) {
                    assertTrue("row " + i, vector.isNull(i));
                } else {
                    int[] zxy = GeoTileUtils.parseHash(expected.get(i));
                    assertFalse("row " + i, vector.isNull(i));
                    assertEquals("row " + i, zxy[0], Byte.toUnsignedInt(zoom.get(i)));
                    assertEquals("row " + i, zxy[1], x.get(i));
                    assertEquals("row " + i, zxy[2], y.get(i));
                }
            }
        }
    }

    public void testGeohex() throws Exception {
        var expected = geoGridStrings("ST_GEOHEX(location, 3)");
        try (VectorSchemaRoot root = esql(geoGridQuery("ST_GEOHEX(location, 3)"))) {
            assertEquals(1, root.getSchema().getFields().size());

            var vector = (BigIntVector) root.getVector("cell");
            assertEquals(4, vector.getValueCount());
            for (int i = 0; i < expected.size(); i++) {
                if (expected.get(i) == null) {
                    assertTrue("row " + i, vector.isNull(i));
                } else {
                    assertEquals("row " + i, H3.stringToH3(expected.get(i)), vector.get(i));
                }
            }
        }
    }

    private static String geoGridQuery(String gridExpression) {
        return "{\"query\": \"FROM arrow-geo-test | SORT value | LIMIT 100 | EVAL cell = " + gridExpression + " | KEEP cell\"}";
    }

    /** The string representation of grid cells, as output by the JSON format, with a null for missing values. */
    private List<String> geoGridStrings(String gridExpression) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(geoGridQuery(gridExpression));
        List<List<String>> rows = ObjectPath.createFromResponse(client().performRequest(request)).evaluate("values");
        return rows.stream().map(row -> row.get(0)).toList();
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
