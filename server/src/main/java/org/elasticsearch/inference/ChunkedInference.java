/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContent;

import java.io.IOException;
import java.util.Iterator;

public interface ChunkedInference {

    /**
     * Implementations of this function serialize their embeddings to {@link BytesReference} for storage in semantic text fields.
     *
     * @param xcontent provided by the SemanticTextField
     * @return an iterator of the serialized {@link Chunk} which includes the offset into the input text and bytes reference
     * (output/embedding).
     */
    Iterator<Chunk> chunksAsByteReference(XContent xcontent) throws IOException;

    /**
     * A chunk of inference results containing the substring location in the original text and the bytes reference.
     * @param textOffset
     * @param bytesReference
     */
    record Chunk(TextOffset textOffset, BytesReference bytesReference) {}

    record TextOffset(int start, int end) {}
}
