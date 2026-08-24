/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceObjectRamBytesUsedTest;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;

import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class EmbeddingRequestChunkerRequestTests extends InferenceObjectRamBytesUsedTest<EmbeddingRequestChunker.Request> {

    private static final String INPUT = "document_test";

    @Override
    public EmbeddingRequestChunker.Request objectToEstimate() {
        return new EmbeddingRequestChunker.Request(0, 0, new Chunker.ChunkOffset(0, 5), new InferenceString(DataType.TEXT, INPUT));
    }

    @Override
    public List<EmbeddingRequestChunker.Request> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger chunk — chunkChars=10 crosses the 8-byte alignment boundary versus chunkChars=5
            new EmbeddingRequestChunker.Request(0, 0, new Chunker.ChunkOffset(0, 10), new InferenceString(DataType.TEXT, INPUT)),
            // Whole-input chunk
            new EmbeddingRequestChunker.Request(0, 0, new Chunker.ChunkOffset(0, 13), new InferenceString(DataType.TEXT, INPUT))
        );
    }

    public void testRamBytesUsed_EmptyInput_AttributesWholeInputString() {
        var emptyInput = new InferenceString(DataType.TEXT, "");
        var request = new EmbeddingRequestChunker.Request(0, 0, new Chunker.ChunkOffset(0, 0), emptyInput);

        assertThat(request.ramBytesUsed(), greaterThanOrEqualTo(emptyInput.ramBytesUsed()));
    }

    public void testRamBytesUsed_CoversMaterializedChunk() {
        var request = objectToEstimate();
        assertThat(request.ramBytesUsed(), greaterThanOrEqualTo(RamUsageTester.ramUsed(new InferenceStringGroup(request.chunkText()))));
    }

    @Override
    public boolean checkDoNotUnderAccount() {
        // Proportional accounting intentionally attributes only a fraction of the shared InferenceString
        // to each Request. A standalone Request therefore under-accounts relative to RamUsageTester,
        // which traverses the full object graph including the entire InferenceString.
        return false;
    }
}
