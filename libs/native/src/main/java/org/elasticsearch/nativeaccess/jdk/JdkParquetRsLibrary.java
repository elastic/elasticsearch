/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.lib.LoaderHelper;
import org.elasticsearch.nativeaccess.lib.ParquetRsLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

/**
 * Panama FFI bindings to the Rust es_parquet_rs shared library for Parquet operations.
 */
public final class JdkParquetRsLibrary implements ParquetRsLibrary {

    private static final Logger logger = LogManager.getLogger(JdkParquetRsLibrary.class);

    private static final int ERROR_BUF_SIZE = 4096;

    private static final MethodHandle lastError$mh;
    private static final MethodHandle getStatistics$mh;
    private static final MethodHandle getSchemaFFI$mh;

    static {
        LoaderHelper.loadLibrary("es_parquet_rs");
        logger.info("Loaded es_parquet_rs native library");

        lastError$mh = downcallHandle("pqrs_last_error", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT));
        getStatistics$mh = downcallHandle("pqrs_get_statistics", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS));
        getSchemaFFI$mh = downcallHandle("pqrs_get_schema_ffi", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG));
    }

    @Override
    public String lastError() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(ERROR_BUF_SIZE);
            int len = (int) lastError$mh.invokeExact(buf, ERROR_BUF_SIZE);
            if (len <= 0) {
                return null;
            }
            return MemorySegmentUtil.getString(buf, 0);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long[] getStatistics(String path, String configJson) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathSeg = MemorySegmentUtil.allocateString(arena, path);
            MemorySegment configSeg = configJson != null ? MemorySegmentUtil.allocateString(arena, configJson) : MemorySegment.NULL;
            MemorySegment outRows = arena.allocate(JAVA_LONG);
            MemorySegment outBytes = arena.allocate(JAVA_LONG);
            int rc = (int) getStatistics$mh.invokeExact(pathSeg, configSeg, outRows, outBytes);
            if (rc != 0) {
                return null;
            }
            return new long[] { outRows.get(JAVA_LONG, 0), outBytes.get(JAVA_LONG, 0) };
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int getSchemaFFI(String path, String configJson, long schemaAddr) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathSeg = MemorySegmentUtil.allocateString(arena, path);
            MemorySegment configSeg = configJson != null ? MemorySegmentUtil.allocateString(arena, configJson) : MemorySegment.NULL;
            return (int) getSchemaFFI$mh.invokeExact(pathSeg, configSeg, schemaAddr);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
