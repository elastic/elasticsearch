/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk.arrow;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.arrow.Arrow;
import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;

import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * End-to-end test for Arrow bulk ingestion. Tests for the various Arrow datatypes and
 * bulk actions are in {@code ArrowBulkIncrementalParserTests}
 */
public class BulkArrowIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "basic")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testBulk() throws Exception {

        // Create a dataframe with two columns: integer and string
        Field intField = new Field("ints", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field strField = new Field("strings", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(List.of(intField, strField));

        int batchCount = 7;
        int rowCount = 11;

        byte[] payload;

        // Create vectors and write them to a byte array
        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            var root = VectorSchemaRoot.create(schema, allocator);
        ) {
            var baos = new ByteArrayOutputStream();
            IntVector intVector = (IntVector) root.getVector(0);
            VarCharVector stringVector = (VarCharVector) root.getVector(1);

            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                for (int batch = 0; batch < batchCount; batch++) {
                    intVector.allocateNew(rowCount);
                    stringVector.allocateNew(rowCount);
                    for (int row = 0; row < rowCount; row++) {
                        int globalRow = row + batch * rowCount;
                        intVector.set(row, globalRow);
                        stringVector.set(row, new Text("row" + globalRow));
                    }
                    root.setRowCount(rowCount);
                    writer.writeBatch();
                }
            }
            payload = baos.toByteArray();
        }

        {
            // Bulk insert the arrow stream
            var request = new Request("POST", "/arrow_bulk_test/_bulk");
            request.addParameter("refresh", "wait_for");
            request.setEntity(new ByteArrayEntity(payload, ContentType.create(Arrow.MEDIA_TYPE)));

            var response = client().performRequest(request);
            var result = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, response.getEntity().getContent())
                .map();

            assertEquals(Boolean.FALSE, result.get("errors"));
            assertEquals(batchCount * rowCount, ((List<?>)result.get("items")).size());
        }

        {
            // Check that the index effectively contains what we sent
            var request = new Request("GET", "/arrow_bulk_test/_count");
            var response = client().performRequest(request);
            var result = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, response.getEntity().getContent())
                .map();

            assertEquals(batchCount * rowCount, result.get("count"));
        }
    }

    public void testDictionary() throws Exception {

        //DictionaryEncoding encoding = new DictionaryEncoding()

        // Create a dataframe with two columns: integer and string
        Field intField = new Field("ints", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field strField = new Field("strings", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(List.of(intField, strField));

        int batchCount = 7;
        int rowCount = 11;

        byte[] payload;

        // Create vectors and write them to a byte array
        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            var root = VectorSchemaRoot.create(schema, allocator);
        ) {
            var baos = new ByteArrayOutputStream();
            IntVector intVector = (IntVector) root.getVector(0);
            VarCharVector stringVector = (VarCharVector) root.getVector(1);

            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                for (int batch = 0; batch < batchCount; batch++) {
                    intVector.allocateNew(rowCount);
                    stringVector.allocateNew(rowCount);
                    for (int row = 0; row < rowCount; row++) {
                        int globalRow = row + batch * rowCount;
                        intVector.set(row, globalRow);
                        stringVector.set(row, new Text("row" + globalRow));
                    }
                    root.setRowCount(rowCount);
                    writer.writeBatch();
                }
            }
            payload = baos.toByteArray();
        }

        {
            // Bulk insert the arrow stream
            var request = new Request("POST", "/arrow_bulk_test/_bulk");
            request.addParameter("refresh", "wait_for");
            request.setEntity(new ByteArrayEntity(payload, ContentType.create(Arrow.MEDIA_TYPE)));

            var response = client().performRequest(request);
            var result = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, response.getEntity().getContent())
                .map();

            assertEquals(Boolean.FALSE, result.get("errors"));
            assertEquals(batchCount * rowCount, ((List<?>)result.get("items")).size());
        }

        {
            // Check that the index effectively contains what we sent
            var request = new Request("GET", "/arrow_bulk_test/_count");
            var response = client().performRequest(request);
            var result = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, response.getEntity().getContent())
                .map();

            assertEquals(batchCount * rowCount, result.get("count"));
        }
    }
}
