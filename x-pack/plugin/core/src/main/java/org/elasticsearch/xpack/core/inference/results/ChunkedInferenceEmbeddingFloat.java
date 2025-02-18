/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public record ChunkedInferenceEmbeddingFloat(List<FloatEmbeddingChunk> chunks) {

    public record FloatEmbeddingChunk(float[] embedding, String matchedText, ChunkedInference.TextOffset offset)
        implements
            EmbeddingResults.EmbeddingChunk {

        public ChunkedInference.Chunk toChunk(XContent xcontent) throws IOException {
            return new ChunkedInference.Chunk(matchedText, offset, toBytesReference(xcontent, embedding));
        }

        /**
         * Serialises the {@code value} array, according to the provided {@link XContent}, into a {@link BytesReference}.
         */
        private static BytesReference toBytesReference(XContent xContent, float[] value) throws IOException {
            XContentBuilder b = XContentBuilder.builder(xContent);
            b.startArray();
            for (float v : value) {
                b.value(v);
            }
            b.endArray();
            return BytesReference.bytes(b);
        }
    }
}
