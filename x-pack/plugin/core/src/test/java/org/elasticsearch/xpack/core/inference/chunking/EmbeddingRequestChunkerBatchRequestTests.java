/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceObjectRamBytesUsedTest;
import org.elasticsearch.inference.InferenceString;

import java.util.List;

public class EmbeddingRequestChunkerBatchRequestTests extends InferenceObjectRamBytesUsedTest<EmbeddingRequestChunker.BatchRequest> {

    private static final EmbeddingRequestChunker.Request REQUEST = new EmbeddingRequestChunker.Request(
        0,
        0,
        new Chunker.ChunkOffset(0, 5),
        new InferenceString(DataType.TEXT, "document")
    );

    @Override
    public EmbeddingRequestChunker.BatchRequest objectToEstimate() {
        return new EmbeddingRequestChunker.BatchRequest(List.of(REQUEST));
    }

    @Override
    public List<EmbeddingRequestChunker.BatchRequest> objectsToEstimateWithLargerInput() {
        return List.of(
            // Request with larger chunk
            new EmbeddingRequestChunker.BatchRequest(
                List.of(
                    new EmbeddingRequestChunker.Request(
                        REQUEST.inputIndex(),
                        REQUEST.chunkIndex(),
                        new Chunker.ChunkOffset(0, 10),
                        REQUEST.input()
                    )
                )
            ),
            // More requests
            new EmbeddingRequestChunker.BatchRequest(List.of(REQUEST, REQUEST))
        );
    }

    @Override
    public boolean checkDoNotUnderAccount() {
        // RamUsageTester inside testRamBytesUsed_DoesNotUnderAccount measures the whole object disregarding, that we only need to account
        // for the chunking overhead. Therefore, checking for do not under account doesn't make sense in this case.
        return false;
    }
}
