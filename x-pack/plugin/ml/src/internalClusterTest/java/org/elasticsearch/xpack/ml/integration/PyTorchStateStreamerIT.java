/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.pytorch.ModelStorage;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchStateStreamer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class PyTorchStateStreamerIT extends MlSingleNodeTestCase {

    private static final String SOURCE_INDEX = "model_source";
    private static final String SOURCE_FIELD = "model_data";
    private static final String DOC_PREFIX = "model_chunk";

    public void testRestoreState() throws IOException {
        int numChunks = 5;
        int chunkSize = 100;
        int modelSize = numChunks * chunkSize;

        List<byte[]> chunks = new ArrayList<>(numChunks);
        for (int i=0; i<numChunks; i++) {
            chunks.add(randomByteArrayOfLength(chunkSize));
        }

        putState(chunks);

        String modelId = "test-state-streamer-restore";
        ModelStorage storageInfo = new ModelStorage(modelId, DOC_PREFIX, Instant.now(),
            SOURCE_FIELD, null, numChunks, modelSize);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(modelSize);
        PyTorchStateStreamer stateStreamer = new PyTorchStateStreamer(client());
        stateStreamer.writeStateToStream(storageInfo, outputStream);

        byte[] writtenData = outputStream.toByteArray();

        int writtenSize = ByteBuffer.wrap(writtenData, 0, 4).getInt();
        assertEquals(modelSize, writtenSize);

        byte[] writtenChunk = new byte[chunkSize];
        for (int i=0; i<numChunks; i++) {
            logger.info("comparing ");
            System.arraycopy(writtenData, i * chunkSize + 4, writtenChunk, 0, chunkSize);
            assertArrayEquals(chunks.get(i), writtenChunk);
        }
    }

    private void putState(List<byte[]> stateChunks) {
        client().admin().indices().prepareCreate(SOURCE_INDEX).setMapping(SOURCE_FIELD, "type=binary").get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < stateChunks.size(); i++) {
            IndexRequest indexRequest = new IndexRequest(SOURCE_INDEX);

            String encodedData = new String(Base64.getEncoder().encode(stateChunks.get(i)), StandardCharsets.UTF_8);

            indexRequest.source(SOURCE_FIELD, encodedData)
                .id(DOC_PREFIX + i)
                .opType(DocWriteRequest.OpType.CREATE);
            bulkRequestBuilder.add(indexRequest);
        }

        BulkResponse bulkResponse = bulkRequestBuilder
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        if (bulkResponse.hasFailures()) {
            int failures = 0;
            for (BulkItemResponse itemResponse : bulkResponse) {
                if (itemResponse.isFailed()) {
                    failures++;
                    logger.error("Item response failure [{}]", itemResponse.getFailureMessage());
                }
            }
            fail("Bulk response contained " + failures + " failures");
        }
        logger.debug("Indexed [{}] documents", stateChunks.size());
    }
}
