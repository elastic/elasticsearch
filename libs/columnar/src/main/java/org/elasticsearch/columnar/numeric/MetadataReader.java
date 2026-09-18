/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import java.io.IOException;

/**
 * Read-side view of per-block stage metadata for a {@link BlockTransform}. The symmetric
 * counterpart to {@link MetadataWriter}: each read call consumes exactly the bytes that the
 * corresponding write produced, in the same order.
 *
 * @see MetadataWriter
 * @see DataInputMetadataReader
 */
public interface MetadataReader {

    /** Reads a single byte. */
    byte readByte() throws IOException;

    /** Reads a fixed-width 4-byte big-endian integer. */
    int readInt() throws IOException;

    /** Reads a fixed-width 8-byte big-endian long. */
    long readLong() throws IOException;

    /** Reads an unsigned variable-length integer. */
    int readVInt() throws IOException;

    /** Reads an unsigned variable-length long. */
    long readVLong() throws IOException;

    /** Reads a signed variable-length integer using zigzag encoding. */
    int readZInt() throws IOException;

    /** Reads a signed variable-length long using zigzag encoding. */
    long readZLong() throws IOException;
}
