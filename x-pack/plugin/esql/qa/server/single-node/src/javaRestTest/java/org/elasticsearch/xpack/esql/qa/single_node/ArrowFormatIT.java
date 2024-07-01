/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
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

    public void testArrowFormat() throws Exception {

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
            {"value": 1, "description": "number one"}
            {"index": {"_id": "2"}}
            {"value": 2, "description": "number two"}
            {"index": {"_id": "3"}}
            {"value": 3}
            {"index": {"_id": "4"}}
            {"value": 4, "description": "number four"}
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/_query?format=arrow");
        request.setJsonEntity("""
            {
                "query": "from arrow-test | limit 100"
            }
            """);

        Response response = client().performRequest(request);

        assertEquals("application/vnd.apache.arrow.stream", response.getEntity().getContentType().getValue());

        try (VectorSchemaRoot root = readArrow(response.getEntity().getContent())) {

            List<Field> fields = root.getSchema().getFields();
            assertEquals(2, fields.size());

            var descVector = (VarCharVector) root.getVector("description");
            assertEquals("number one", descVector.getObject(0).toString());
            assertEquals("number two", descVector.getObject(1).toString());
            assertTrue(descVector.isNull(2));
            assertEquals("number four", descVector.getObject(3).toString());

            var valueVector = (IntVector) root.getVector("value");
            assertEquals(1, valueVector.get(0));
            assertEquals(2, valueVector.get(1));
            assertEquals(3, valueVector.get(2));
            assertEquals(4, valueVector.get(3));
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
}
