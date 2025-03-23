/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

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
import org.elasticsearch.arrow.ArrowPlugin;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.libs.arrow.Arrow;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end test for Arrow bulk ingestion. Tests for the various Arrow datatypes and
 * bulk actions are in {@code ArrowBulkIncrementalParserTests}
 */
public class ArrowBulkActionIT extends ESSingleNodeRestTestCase {

    private RestClient restClient;

    @Before
    public void init() {
        restClient = createRestClient();
    }

    @After
    public void cleanup() throws IOException {
        restClient.close();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(ArrowPlugin.class);
    }

    /**
     * An end-to-end test that checks that Arrow data is correctly indexed and can be searched.
     */
    public void testBulk() throws Exception {

        String index = "arrow_bulk_test";

        {
            // Check that the index doesn't exist
            var request = new Request("HEAD", "/" + index);
            var response = restClient.performRequest(request);
            assertEquals(404, response.getStatusLine().getStatusCode());
        }

        // Create a dataframe with two columns: integer and string
        Field intField = new Field("ints", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field strField = new Field("strings", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(List.of(intField, strField));

        int batchCount = randomInt(10);
        int rowCount = randomInt(10);

        byte[] payload;

        // Create vectors and write them to a byte array
        try (var allocator = Arrow.newChildAllocator("test", 0, Long.MAX_VALUE); var root = VectorSchemaRoot.create(schema, allocator);) {
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
            var request = new Request("POST", "/_arrow/" + index + "/_bulk");
            request.addParameter("refresh", "wait_for");
            request.setEntity(new ByteArrayEntity(payload, ContentType.create(Arrow.MEDIA_TYPE)));

            var response = restClient.performRequest(request);
            var result = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, response.getEntity().getContent())
                .map();

            assertEquals(Boolean.FALSE, result.get("errors"));
            assertEquals(batchCount * rowCount, ((List<?>) result.get("items")).size());
        }

        {
            // Check that the index effectively contains what we sent
            var request = new Request("GET", "/" + index + "/_count");
            var response = restClient.performRequest(request);
            var result = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, response.getEntity().getContent())
                .map();

            assertEquals(batchCount * rowCount, result.get("count"));
        }
    }
}
