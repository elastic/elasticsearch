/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * Turns a (transformed) block of {@code long}s into bytes and back. Its {@code id} is frozen once shipped.
 */
public interface BlockTerminal {

    /** Frozen stage id, unique among terminals and stable on disk. */
    byte id();

    /**
     * Serializes {@code block}. Only the first {@code valueCount} entries carry real data; a
     * fixed-width terminal is responsible for handling the {@code [valueCount, block.length)} tail
     * itself (e.g. by zero-filling it) rather than relying on the caller to pad.
     */
    void encode(long[] block, int valueCount, DataOutput out) throws IOException;

    /** Reconstructs {@code block} from bytes written by {@link #encode}. */
    void decode(DataInput in, int valueCount, long[] block) throws IOException;
}
