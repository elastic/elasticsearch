/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.reflect;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.VectorEncoding;
import org.elasticsearch.core.SuppressForbidden;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

import static java.lang.invoke.MethodType.methodType;

/**
 * Reflective access to non-accessible members of Lucene's KnnVectorsReader implementations.
 * Remove once KnnVectorsReaders::getOffHeapByteSize is available.
 */
public class OffHeapReflectionUtils {

    private OffHeapReflectionUtils() {}

    static final String FLAT_VECTOR_DATA_EXTENSION = "vec";
    static final String SQ_VECTOR_INDEX_EXTENSION = "veq";
    static final String HNSW_VECTOR_INDEX_EXTENSION = "vex";

    private static final MethodHandle GET_FIELD_ENTRY_HNDL_SQ;
    private static final MethodHandle GET_VECTOR_DATA_LENGTH_HANDLE_SQ;
    private static final VarHandle RAW_VECTORS_READER_HNDL_SQ;
    private static final MethodHandle GET_FIELD_ENTRY_HANDLE_L99FLT;
    private static final MethodHandle VECTOR_DATA_LENGTH_HANDLE_L99FLT;
    private static final MethodHandle GET_FIELD_ENTRY_HANDLE_L99HNSW;
    private static final MethodHandle GET_VECTOR_INDEX_LENGTH_HANDLE_L99HNSW;
    private static final VarHandle FLAT_VECTORS_READER_HNDL_L99HNSW;

    static final Class<?> L99_SQ_VR_CLS = Lucene99ScalarQuantizedVectorsReader.class;
    static final Class<?> L99_FLT_VR_CLS = Lucene99FlatVectorsReader.class;
    static final Class<?> L99_HNSW_VR_CLS = Lucene99HnswVectorsReader.class;

    static {
        try {
            // Lucene99ScalarQuantizedVectorsReader
            var cls = Class.forName("org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader$FieldEntry");
            var lookup = privilegedPrivateLookupIn(L99_SQ_VR_CLS, MethodHandles.lookup());
            var mt = methodType(cls, String.class);
            GET_FIELD_ENTRY_HNDL_SQ = lookup.findVirtual(L99_SQ_VR_CLS, "getFieldEntry", mt);
            GET_VECTOR_DATA_LENGTH_HANDLE_SQ = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
            RAW_VECTORS_READER_HNDL_SQ = lookup.findVarHandle(L99_SQ_VR_CLS, "rawVectorsReader", FlatVectorsReader.class);
            // Lucene99FlatVectorsReader
            cls = Class.forName("org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader$FieldEntry");
            lookup = privilegedPrivateLookupIn(L99_FLT_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class, VectorEncoding.class);
            GET_FIELD_ENTRY_HANDLE_L99FLT = lookup.findVirtual(L99_FLT_VR_CLS, "getFieldEntry", mt);
            VECTOR_DATA_LENGTH_HANDLE_L99FLT = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
            // Lucene99HnswVectorsReader
            cls = Class.forName("org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader$FieldEntry");
            lookup = privilegedPrivateLookupIn(L99_HNSW_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class, VectorEncoding.class);
            GET_FIELD_ENTRY_HANDLE_L99HNSW = lookup.findVirtual(L99_HNSW_VR_CLS, "getFieldEntry", mt);
            GET_VECTOR_INDEX_LENGTH_HANDLE_L99HNSW = lookup.findVirtual(cls, "vectorIndexLength", methodType(long.class));
            lookup = privilegedPrivateLookupIn(L99_HNSW_VR_CLS, MethodHandles.lookup());
            FLAT_VECTORS_READER_HNDL_L99HNSW = lookup.findVarHandle(L99_HNSW_VR_CLS, "flatVectorsReader", FlatVectorsReader.class);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressForbidden(reason = "static type is not accessible")
    static Map<String, Long> getOffHeapByteSizeSQ(Lucene99ScalarQuantizedVectorsReader reader, FieldInfo fieldInfo) {
        try {
            var entry = GET_FIELD_ENTRY_HNDL_SQ.invoke(reader, fieldInfo.name);
            long len = (long) GET_VECTOR_DATA_LENGTH_HANDLE_SQ.invoke(entry);
            return Map.of(SQ_VECTOR_INDEX_EXTENSION, len);
        } catch (Throwable t) {
            handleThrowable(t);
        }
        throw new AssertionError("should not reach here");
    }

    static FlatVectorsReader getFlatVectorsReaderSQ(Lucene99ScalarQuantizedVectorsReader reader) {
        return (FlatVectorsReader) RAW_VECTORS_READER_HNDL_SQ.get(reader);
    }

    @SuppressForbidden(reason = "static type is not accessible")
    static Map<String, Long> getOffHeapByteSizeF99FLT(Lucene99FlatVectorsReader reader, FieldInfo fieldInfo) {
        try {
            var entry = GET_FIELD_ENTRY_HANDLE_L99FLT.invoke(reader, fieldInfo.name, fieldInfo.getVectorEncoding());
            long len = (long) VECTOR_DATA_LENGTH_HANDLE_L99FLT.invoke(entry);
            return Map.of(FLAT_VECTOR_DATA_EXTENSION, len);
        } catch (Throwable t) {
            handleThrowable(t);
        }
        throw new AssertionError("should not reach here");
    }

    @SuppressForbidden(reason = "static type is not accessible")
    static Map<String, Long> getOffHeapByteSizeL99HNSW(Lucene99HnswVectorsReader reader, FieldInfo fieldInfo) {
        try {
            var entry = GET_FIELD_ENTRY_HANDLE_L99HNSW.invoke(reader, fieldInfo.name, fieldInfo.getVectorEncoding());
            long len = (long) GET_VECTOR_INDEX_LENGTH_HANDLE_L99HNSW.invoke(entry);
            return Map.of(HNSW_VECTOR_INDEX_EXTENSION, len);
        } catch (Throwable t) {
            handleThrowable(t);
        }
        throw new AssertionError("should not reach here");
    }

    static FlatVectorsReader getFlatVectorsReaderL99HNSW(Lucene99HnswVectorsReader reader) {
        return (FlatVectorsReader) FLAT_VECTORS_READER_HNDL_L99HNSW.get(reader);
    }

    @SuppressWarnings("removal")
    private static MethodHandles.Lookup privilegedPrivateLookupIn(Class<?> cls, MethodHandles.Lookup lookup) {
        PrivilegedAction<MethodHandles.Lookup> pa = () -> {
            try {
                return MethodHandles.privateLookupIn(cls, lookup);
            } catch (IllegalAccessException e) {
                throw new AssertionError("should not happen, check opens", e);
            }
        };
        return AccessController.doPrivileged(pa);
    }

    private static void handleThrowable(Throwable t) {
        if (t instanceof Error error) {
            throw error;
        } else if (t instanceof RuntimeException runtimeException) {
            throw runtimeException;
        } else {
            throw new AssertionError(t);
        }
    }
}
