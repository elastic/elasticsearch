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
 * Encodes source documents into a {@link SourceBatch}, fanning rows out to one or more partitions
 * (typically one per destination shard).
 *
 * <p>Usage: {@link #parseToScratch} stages a single document, then {@link #commitScratchTo} appends
 * it to a partition; once all documents are committed, {@link #buildPartition} produces the batch for
 * a partition. {@link #addDocument} is the single-step convenience.
 */
public interface SourceBatchEncoder extends Releasable {

    /**
     * Parses {@code source} into the encoder's scratch buffers, firing {@code sink} for each primitive
     * leaf. The staged row is appended by the next {@link #commitScratchTo} call; calling this twice
     * without an intervening commit discards the previously staged row.
     */
    void parseToScratch(BytesReference source, XContentType xContentType, LeafSink sink) throws IOException;

    /**
     * Appends the currently staged row (from {@link #parseToScratch}) to {@code partitionKey} and
     * returns its row index within that partition.
     */
    int commitScratchTo(int partitionKey) throws IOException;

    /** Builds the {@link SourceBatch} for {@code partitionKey} from all committed rows. */
    SourceBatch buildPartition(int partitionKey);

    /** The number of documents committed to {@code partitionKey}. */
    int docCount(int partitionKey);

    /** Whether {@code partitionKey} has received any committed rows. */
    boolean hasPartition(int partitionKey);

    /** The cached dotted path for a schema leaf column. */
    String columnPath(int columnIndex);

    /** Parses and commits {@code source} to {@code partition} in one step, with no leaf sink. */
    default void addDocument(BytesReference source, XContentType xContentType, int partition) throws IOException {
        parseToScratch(source, xContentType, LeafSink.NO_OP);
        commitScratchTo(partition);
    }
}
