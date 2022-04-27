/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchStateStreamer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class PyTorchStateStreamerIT extends MlSingleNodeTestCase {

    public void testRestoreState() throws IOException, InterruptedException {
        int numChunks = 5;
        int chunkSize = 100;
        int modelSize = numChunks * chunkSize;

        String modelId = "test-state-streamer-restore";

        List<byte[]> chunks = new ArrayList<>(numChunks);
        for (int i = 0; i < numChunks; i++) {
            chunks.add(randomByteArrayOfLength(chunkSize));
        }

        List<TrainedModelDefinitionDoc> docs = createModelDefinitionDocs(chunks, modelId);
        putModelDefinition(docs);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(modelSize);
        PyTorchStateStreamer stateStreamer = new PyTorchStateStreamer(
            client(),
            client().threadPool().executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
            xContentRegistry()
        );

        AtomicReference<Boolean> onSuccess = new AtomicReference<>();
        AtomicReference<Exception> onFailure = new AtomicReference<>();
        blockingCall(
            listener -> stateStreamer.writeStateToStream(modelId, InferenceIndexConstants.LATEST_INDEX_NAME, outputStream, listener),
            onSuccess,
            onFailure
        );

        byte[] writtenData = outputStream.toByteArray();

        // the first 4 bytes are the model size
        int writtenSize = ByteBuffer.wrap(writtenData, 0, 4).getInt();
        assertEquals(modelSize, writtenSize);

        byte[] writtenChunk = new byte[chunkSize];
        for (int i = 0; i < numChunks; i++) {
            System.arraycopy(writtenData, i * chunkSize + 4, writtenChunk, 0, chunkSize);
            assertArrayEquals(chunks.get(i), writtenChunk);
        }
    }

    private List<TrainedModelDefinitionDoc> createModelDefinitionDocs(List<byte[]> binaryChunks, String modelId) {

        int totalLength = binaryChunks.stream().map(arr -> arr.length).reduce(0, Integer::sum);

        List<TrainedModelDefinitionDoc> docs = new ArrayList<>();
        for (int i = 0; i < binaryChunks.size(); i++) {
            String encodedData = new String(Base64.getEncoder().encode(binaryChunks.get(i)), StandardCharsets.UTF_8);

            docs.add(
                new TrainedModelDefinitionDoc.Builder().setDocNum(i)
                    .setCompressedString(encodedData)
                    .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
                    .setTotalDefinitionLength(totalLength)
                    .setDefinitionLength(encodedData.length())
                    .setEos(i == binaryChunks.size() - 1)
                    .setModelId(modelId)
                    .build()
            );
        }
        return docs;
    }

    private void putModelDefinition(List<TrainedModelDefinitionDoc> docs) throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < docs.size(); i++) {
            TrainedModelDefinitionDoc doc = docs.get(i);
            try (XContentBuilder xContentBuilder = doc.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
                IndexRequestBuilder indexRequestBuilder = client().prepareIndex(InferenceIndexConstants.LATEST_INDEX_NAME)
                    .setSource(xContentBuilder)
                    .setId(TrainedModelDefinitionDoc.docId(doc.getModelId(), i));

                bulkRequestBuilder.add(indexRequestBuilder);
            }
        }

        BulkResponse bulkResponse = bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
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
        logger.debug("Indexed [{}] documents", docs.size());
    }
}
