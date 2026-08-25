/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.foreign.adapter.MemorySegmentAdapter;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdjson.JsonParsingException;
import org.elasticsearch.simdjson.SimdJsonSupport;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import static java.lang.foreign.MemorySegment.ofArray;

/**
 * Delegates stage 1 structural indexing to the native simdjson C++ library,
 * loaded through {@link SimdJsonSupport}.
 *
 * <p> Each instance holds a native context ({@code es_stage1_ctx*}) that is reused across
 * calls. Instances are <strong>not thread-safe</strong> - each thread should own its own
 * instance.
 */
public final class StructuralIndexer implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(StructuralIndexer.class);

    private static final SimdJsonLibrary LIB = SimdJsonSupport.LIB;

    /** True when the native simdjson C++ library is loaded and available. */
    public static boolean available() {
        return LIB != null;
    }

    private MemorySegment ctx;
    private final int[] outCount = new int[1];

    public StructuralIndexer(int initialCapacity) {
        Objects.requireNonNull(LIB, "Native simdjson is not available");
        this.ctx = LIB.create(initialCapacity);
        if (ctx.equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Native es_stage1_create returned null");
        }
    }

    /**
     * Runs native stage 1 over {@code buffer[0..len)} and writes the resulting structural
     * indices directly into {@code bitIndexes}.
     *
     * @throws JsonParsingException on invalid UTF-8 or other structural errors detected by simdjson
     */
    public void index(byte[] buffer, int len, BitIndexes bitIndexes) {
        index(buffer, 0, len, bitIndexes);
    }

    /**
     * Runs native stage 1 over {@code buffer[offset..offset+len)} and writes structural
     * indices into {@code bitIndexes}. The indices are absolute positions within
     * {@code buffer} (i.e. they include {@code offset}).
     *
     * @throws JsonParsingException on invalid UTF-8 or other structural errors detected by simdjson
     */
    public void index(byte[] buffer, int offset, int len, BitIndexes bitIndexes) {
        Objects.checkFromIndexSize(offset, len, buffer.length);
        bitIndexes.ensureCapacity(len + 1);
        bitIndexes.reset();

        int[] rawIndexes = bitIndexes.rawIndexes();
        int err = LIB.stage1(ctx, ofArray(buffer), offset, len, ofArray(rawIndexes), rawIndexes.length, ofArray(outCount));
        if (err != 0) {
            throw new JsonParsingException("Native simdjson stage 1 failed: " + readErrorMessage(err));
        }
        int count = outCount[0];
        bitIndexes.setWriteIdx(count);
    }

    private static String readErrorMessage(int err) {
        MemorySegment ptr = LIB.errorMessage(err);
        if (ptr.equals(MemorySegment.NULL)) {
            return "unknown error (code " + err + ")";
        }
        return MemorySegmentAdapter.getString(ptr.reinterpret(256), 0);
    }

    @Override
    public void close() {
        if (ctx != null) {
            LIB.destroy(ctx);
            ctx = null;
        }
    }
}
