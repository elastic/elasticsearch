/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

public interface InferenceServiceResults extends NamedWriteable, ChunkedToXContent {

    /**
     * <p>Transform the result to match the format required for the TransportCoordinatedInferenceAction.
     * For the inference plugin TextEmbeddingResults, the {@link #transformToLegacyFormat()} transforms the
     * results into an intermediate format only used by the plugin's return value. It doesn't align with what the
     * TransportCoordinatedInferenceAction expects. TransportCoordinatedInferenceAction expects an ml plugin
     * TextEmbeddingResults.</p>
     *
     * <p>For other results like SparseEmbeddingResults, this method can be a pass through to the transformToLegacyFormat.</p>
     */
    default List<? extends InferenceResults> transformToCoordinationFormat() {
        throw new UnsupportedOperationException("transformToCoordinationFormat() is not implemented");
    }

    /**
     * Transform the result to match the format required for versions prior to
     * {@link org.elasticsearch.TransportVersions#V_8_12_0}
     */
    default List<? extends InferenceResults> transformToLegacyFormat() {
        throw new UnsupportedOperationException("transformToLegacyFormat() is not implemented");
    }

    /**
     * Convert the result to a map to aid with test assertions
     */
    default Map<String, Object> asMap() {
        throw new UnsupportedOperationException("asMap() is not implemented");
    }

    default String getWriteableName() {
        assert isStreaming() : "This must be implemented when isStreaming() == false";
        throw new UnsupportedOperationException("This must be implemented when isStreaming() == false");
    }

    default void writeTo(StreamOutput out) throws IOException {
        assert isStreaming() : "This must be implemented when isStreaming() == false";
        throw new UnsupportedOperationException("This must be implemented when isStreaming() == false");
    }

    default Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        assert isStreaming() : "This must be implemented when isStreaming() == false";
        throw new UnsupportedOperationException("This must be implemented when isStreaming() == false");
    }

    /**
     * Returns {@code true} if these results are streamed as chunks, or {@code false} if these results contain the entire payload.
     * Defaults to {@code false}.
     */
    default boolean isStreaming() {
        return false;
    }

    /**
     * When {@link #isStreaming()} is {@code true}, the InferenceAction.Results will subscribe to this publisher.
     * Implementations should follow the {@link java.util.concurrent.Flow.Publisher} spec to stream the chunks.
     */
    default Flow.Publisher<? extends Result> publisher() {
        assert isStreaming() == false : "This must be implemented when isStreaming() == true";
        throw new UnsupportedOperationException("This must be implemented when isStreaming() == true");
    }

    interface Result extends NamedWriteable, ChunkedToXContent {}
}
