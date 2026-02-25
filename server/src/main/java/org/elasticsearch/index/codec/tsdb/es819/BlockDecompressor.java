/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.IOException;

/**
 * A decompressor abstraction for block-oriented decompression that allows implementations
 * to amortize the cost of resource allocation (e.g. native buffers) across multiple
 * {@link #decompress} calls.
 */
abstract class BlockDecompressor implements Closeable {

    /**
     * Decompress bytes that were stored in the given DataInput.
     *
     * @param in the input containing compressed data
     * @param originalLength the original (uncompressed) length
     * @param offset the offset into the decompressed data to start copying from
     * @param length the number of decompressed bytes to copy
     * @param bytes the output BytesRef that receives the decompressed data
     */
    abstract void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException;
}
