/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

/**
 * Encodes source documents into a {@link SourceBatch}.
 *
 * <p>Usage: call {@link #addDocument} for each document (in any order); once all documents are
 * encoded, call {@link #build} to obtain the finished batch. The encoder is single-use: {@link #build}
 * must be called exactly once, after which the encoder should be {@link #close}d.
 */
public interface SourceBatchEncoder extends Releasable {

    /**
     * Encodes {@code source} into the batch and returns its zero-based row index.
     *
     * <p>The returned index is stable: it can be stored on the originating request and used to
     * recover the row from the finished batch via
     * {@link org.elasticsearch.action.index.IndexSource#setSourceRow}.
     */
    int addDocument(BytesReference source, XContentType xContentType) throws IOException;

    /**
     * Returns the number of documents added so far (the total across successful and aborted parses).
     */
    int docCount();

    /**
     * Finalizes all columns and returns the finished {@link SourceBatch}. The batch owns the
     * underlying buffers; close it to release them. Must be called at most once.
     */
    SourceBatch build();
}
