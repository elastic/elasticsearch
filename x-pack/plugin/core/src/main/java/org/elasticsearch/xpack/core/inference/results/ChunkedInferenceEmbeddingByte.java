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

public record ChunkedInferenceEmbeddingByte() {
    /**
     * Serialises the {@code value} array, according to the provided {@link XContent}, into a {@link BytesReference}.
     */

    public record ByteEmbeddingChunk(byte[] embedding, String matchedText, ChunkedInference.TextOffset offset)
        implements
            EmbeddingResults.EmbeddingChunk {

        public ChunkedInference.Chunk toChunk(XContent xcontent) throws IOException {
            return new ChunkedInference.Chunk(matchedText, offset, toBytesReference(xcontent, embedding));
        }

        private static BytesReference toBytesReference(XContent xContent, byte[] value) throws IOException {
            XContentBuilder builder = XContentBuilder.builder(xContent);
            builder.startArray();
            for (byte v : value) {
                builder.value(v);
            }
            builder.endArray();
            return BytesReference.bytes(builder);
        }
    }
}
