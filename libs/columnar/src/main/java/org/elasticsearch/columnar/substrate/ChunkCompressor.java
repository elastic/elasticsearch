/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/** Writes chunks for one {@link ChunkedBytesWriter}; holds that writer's buffers and is not shared. */
public interface ChunkCompressor {

    /** Writes {@code src[0, length)} as one chunk and returns how many bytes it occupies in {@code out}. */
    int write(byte[] src, int length, IndexOutput out) throws IOException;
}
