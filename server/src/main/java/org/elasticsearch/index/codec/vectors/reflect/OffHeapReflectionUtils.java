/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.reflect;

import org.apache.lucene.backward_codecs.lucene90.Lucene90HnswVectorsReader;
import org.apache.lucene.backward_codecs.lucene91.Lucene91HnswVectorsReader;
import org.apache.lucene.backward_codecs.lucene92.Lucene92HnswVectorsReader;
import org.apache.lucene.backward_codecs.lucene94.Lucene94HnswVectorsReader;
import org.apache.lucene.backward_codecs.lucene95.Lucene95HnswVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.VectorEncoding;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.vectors.es818.DirectIOLucene99FlatVectorsReader;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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
    private static final MethodHandle GET_FIELD_ENTRY_HANDLE_DIOL99FLT;
    private static final MethodHandle VECTOR_DATA_LENGTH_HANDLE_DIOL99FLT;
    private static final MethodHandle GET_FIELD_ENTRY_HANDLE_L99HNSW;
    private static final MethodHandle GET_VECTOR_INDEX_LENGTH_HANDLE_L99HNSW;
    private static final VarHandle FLAT_VECTORS_READER_HNDL_L99HNSW;

    static final Class<?> L99_SQ_VR_CLS = Lucene99ScalarQuantizedVectorsReader.class;
    static final Class<?> L99_FLT_VR_CLS = Lucene99FlatVectorsReader.class;
    static final Class<?> DIOL99_FLT_VR_CLS = DirectIOLucene99FlatVectorsReader.class;
    static final Class<?> L99_HNSW_VR_CLS = Lucene99HnswVectorsReader.class;

    // old codecs
    private static final MethodHandle GET_FIELD_ENTRY_HANDLE_L90HNSW;
    private static final MethodHandle GET_VECTOR_INDEX_LENGTH_HANDLE_L90HNSW;
    private static final MethodHandle GET_VECTOR_DATA_LENGTH_HANDLE_L90HNSW;

    private static final MethodHandle GET_FIELD_ENTRY_HANDLE_L91HNSW;
    private static final MethodHandle GET_VECTOR_INDEX_LENGTH_HANDLE_L91HNSW;
    private static final MethodHandle GET_VECTOR_DATA_LENGTH_HANDLE_L91HNSW;

    private static final MethodHandle GET_FIELD_ENTRY_HANDLE_L92HNSW;
    private static final MethodHandle GET_VECTOR_INDEX_LENGTH_HANDLE_L92HNSW;
    private static final MethodHandle GET_VECTOR_DATA_LENGTH_HANDLE_L92HNSW;

    private static final MethodHandle GET_FIELD_ENTRY_HANDLE_L94HNSW;
    private static final MethodHandle GET_VECTOR_INDEX_LENGTH_HANDLE_L94HNSW;
    private static final MethodHandle GET_VECTOR_DATA_LENGTH_HANDLE_L94HNSW;

    private static final MethodHandle GET_FIELD_ENTRY_HANDLE_L95HNSW;
    private static final MethodHandle GET_VECTOR_INDEX_LENGTH_HANDLE_L95HNSW;
    private static final MethodHandle GET_VECTOR_DATA_LENGTH_HANDLE_L95HNSW;

    static final Class<?> L90_HNSW_VR_CLS = Lucene90HnswVectorsReader.class;
    static final Class<?> L91_HNSW_VR_CLS = Lucene91HnswVectorsReader.class;
    static final Class<?> L92_HNSW_VR_CLS = Lucene92HnswVectorsReader.class;
    static final Class<?> L94_HNSW_VR_CLS = Lucene94HnswVectorsReader.class;
    static final Class<?> L95_HNSW_VR_CLS = Lucene95HnswVectorsReader.class;

    static {
        try {
            // Lucene99ScalarQuantizedVectorsReader
            var cls = Class.forName("org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader$FieldEntry");
            var lookup = MethodHandles.privateLookupIn(L99_SQ_VR_CLS, MethodHandles.lookup());
            var mt = methodType(cls, String.class);
            GET_FIELD_ENTRY_HNDL_SQ = lookup.findVirtual(L99_SQ_VR_CLS, "getFieldEntry", mt);
            GET_VECTOR_DATA_LENGTH_HANDLE_SQ = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
            RAW_VECTORS_READER_HNDL_SQ = lookup.findVarHandle(L99_SQ_VR_CLS, "rawVectorsReader", FlatVectorsReader.class);
            // Lucene99FlatVectorsReader
            cls = Class.forName("org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader$FieldEntry");
            lookup = MethodHandles.privateLookupIn(L99_FLT_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class, VectorEncoding.class);
            GET_FIELD_ENTRY_HANDLE_L99FLT = lookup.findVirtual(L99_FLT_VR_CLS, "getFieldEntry", mt);
            VECTOR_DATA_LENGTH_HANDLE_L99FLT = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
            // DirectIOLucene99FlatVectorsReader
            cls = Class.forName("org.elasticsearch.index.codec.vectors.es818.DirectIOLucene99FlatVectorsReader$FieldEntry");
            lookup = MethodHandles.privateLookupIn(DIOL99_FLT_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class, VectorEncoding.class);
            GET_FIELD_ENTRY_HANDLE_DIOL99FLT = lookup.findVirtual(DIOL99_FLT_VR_CLS, "getFieldEntry", mt);
            VECTOR_DATA_LENGTH_HANDLE_DIOL99FLT = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
            // Lucene99HnswVectorsReader
            cls = Class.forName("org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader$FieldEntry");
            lookup = MethodHandles.privateLookupIn(L99_HNSW_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class, VectorEncoding.class);
            GET_FIELD_ENTRY_HANDLE_L99HNSW = lookup.findVirtual(L99_HNSW_VR_CLS, "getFieldEntry", mt);
            GET_VECTOR_INDEX_LENGTH_HANDLE_L99HNSW = lookup.findVirtual(cls, "vectorIndexLength", methodType(long.class));
            lookup = MethodHandles.privateLookupIn(L99_HNSW_VR_CLS, MethodHandles.lookup());
            FLAT_VECTORS_READER_HNDL_L99HNSW = lookup.findVarHandle(L99_HNSW_VR_CLS, "flatVectorsReader", FlatVectorsReader.class);
            // Lucene90HnswVectorsReader
            cls = Class.forName("org.apache.lucene.backward_codecs.lucene90.Lucene90HnswVectorsReader$FieldEntry");
            lookup = MethodHandles.privateLookupIn(L90_HNSW_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class);
            GET_FIELD_ENTRY_HANDLE_L90HNSW = lookup.findVirtual(L90_HNSW_VR_CLS, "getFieldEntry", mt);
            GET_VECTOR_INDEX_LENGTH_HANDLE_L90HNSW = lookup.findVirtual(cls, "indexDataLength", methodType(long.class));
            GET_VECTOR_DATA_LENGTH_HANDLE_L90HNSW = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
            // Lucene91HnswVectorsReader
            cls = Class.forName("org.apache.lucene.backward_codecs.lucene91.Lucene91HnswVectorsReader$FieldEntry");
            lookup = MethodHandles.privateLookupIn(L91_HNSW_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class);
            GET_FIELD_ENTRY_HANDLE_L91HNSW = lookup.findVirtual(L91_HNSW_VR_CLS, "getFieldEntry", mt);
            GET_VECTOR_INDEX_LENGTH_HANDLE_L91HNSW = lookup.findVirtual(cls, "vectorIndexLength", methodType(long.class));
            GET_VECTOR_DATA_LENGTH_HANDLE_L91HNSW = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
            // Lucene92HnswVectorsReader
            cls = Class.forName("org.apache.lucene.backward_codecs.lucene92.Lucene92HnswVectorsReader$FieldEntry");
            lookup = MethodHandles.privateLookupIn(L92_HNSW_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class);
            GET_FIELD_ENTRY_HANDLE_L92HNSW = lookup.findVirtual(L92_HNSW_VR_CLS, "getFieldEntry", mt);
            GET_VECTOR_INDEX_LENGTH_HANDLE_L92HNSW = lookup.findVirtual(cls, "vectorIndexLength", methodType(long.class));
            GET_VECTOR_DATA_LENGTH_HANDLE_L92HNSW = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
            // Lucene94HnswVectorsReader
            cls = Class.forName("org.apache.lucene.backward_codecs.lucene94.Lucene94HnswVectorsReader$FieldEntry");
            lookup = MethodHandles.privateLookupIn(L94_HNSW_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class, VectorEncoding.class);
            GET_FIELD_ENTRY_HANDLE_L94HNSW = lookup.findVirtual(L94_HNSW_VR_CLS, "getFieldEntry", mt);
            GET_VECTOR_INDEX_LENGTH_HANDLE_L94HNSW = lookup.findVirtual(cls, "vectorIndexLength", methodType(long.class));
            GET_VECTOR_DATA_LENGTH_HANDLE_L94HNSW = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
            // Lucene95HnswVectorsReader
            cls = Class.forName("org.apache.lucene.backward_codecs.lucene95.Lucene95HnswVectorsReader$FieldEntry");
            lookup = MethodHandles.privateLookupIn(L95_HNSW_VR_CLS, MethodHandles.lookup());
            mt = methodType(cls, String.class, VectorEncoding.class);
            GET_FIELD_ENTRY_HANDLE_L95HNSW = lookup.findVirtual(L95_HNSW_VR_CLS, "getFieldEntry", mt);
            GET_VECTOR_INDEX_LENGTH_HANDLE_L95HNSW = lookup.findVirtual(cls, "vectorIndexLength", methodType(long.class));
            GET_VECTOR_DATA_LENGTH_HANDLE_L95HNSW = lookup.findVirtual(cls, "vectorDataLength", methodType(long.class));
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
    static Map<String, Long> getOffHeapByteSizeF99FLT(DirectIOLucene99FlatVectorsReader reader, FieldInfo fieldInfo) {
        try {
            var entry = GET_FIELD_ENTRY_HANDLE_DIOL99FLT.invoke(reader, fieldInfo.name, fieldInfo.getVectorEncoding());
            long len = (long) VECTOR_DATA_LENGTH_HANDLE_DIOL99FLT.invoke(entry);
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

    // old codecs
    @SuppressForbidden(reason = "static type is not accessible")
    static Map<String, Long> getOffHeapByteSizeL90HNSW(Lucene90HnswVectorsReader reader, FieldInfo fieldInfo) {
        try {
            var entry = GET_FIELD_ENTRY_HANDLE_L90HNSW.invoke(reader, fieldInfo.name);
            long graph = (long) GET_VECTOR_INDEX_LENGTH_HANDLE_L90HNSW.invoke(entry);
            long raw = (long) GET_VECTOR_DATA_LENGTH_HANDLE_L90HNSW.invoke(entry);
            return Map.of(HNSW_VECTOR_INDEX_EXTENSION, graph, FLAT_VECTOR_DATA_EXTENSION, raw);
        } catch (Throwable t) {
            handleThrowable(t);
        }
        throw new AssertionError("should not reach here");
    }

    @SuppressForbidden(reason = "static type is not accessible")
    static Map<String, Long> getOffHeapByteSizeL91HNSW(Lucene91HnswVectorsReader reader, FieldInfo fieldInfo) {
        try {
            var entry = GET_FIELD_ENTRY_HANDLE_L91HNSW.invoke(reader, fieldInfo.name);
            long graph = (long) GET_VECTOR_INDEX_LENGTH_HANDLE_L91HNSW.invoke(entry);
            long raw = (long) GET_VECTOR_DATA_LENGTH_HANDLE_L91HNSW.invoke(entry);
            return Map.of(HNSW_VECTOR_INDEX_EXTENSION, graph, FLAT_VECTOR_DATA_EXTENSION, raw);
        } catch (Throwable t) {
            handleThrowable(t);
        }
        throw new AssertionError("should not reach here");
    }

    @SuppressForbidden(reason = "static type is not accessible")
    static Map<String, Long> getOffHeapByteSizeL92HNSW(Lucene92HnswVectorsReader reader, FieldInfo fieldInfo) {
        try {
            var entry = GET_FIELD_ENTRY_HANDLE_L92HNSW.invoke(reader, fieldInfo.name);
            long graph = (long) GET_VECTOR_INDEX_LENGTH_HANDLE_L92HNSW.invoke(entry);
            long raw = (long) GET_VECTOR_DATA_LENGTH_HANDLE_L92HNSW.invoke(entry);
            return Map.of(HNSW_VECTOR_INDEX_EXTENSION, graph, FLAT_VECTOR_DATA_EXTENSION, raw);
        } catch (Throwable t) {
            handleThrowable(t);
        }
        throw new AssertionError("should not reach here");
    }

    @SuppressForbidden(reason = "static type is not accessible")
    static Map<String, Long> getOffHeapByteSizeL94HNSW(Lucene94HnswVectorsReader reader, FieldInfo fieldInfo) {
        try {
            var entry = GET_FIELD_ENTRY_HANDLE_L94HNSW.invoke(reader, fieldInfo.name, fieldInfo.getVectorEncoding());
            long graph = (long) GET_VECTOR_INDEX_LENGTH_HANDLE_L94HNSW.invoke(entry);
            long raw = (long) GET_VECTOR_DATA_LENGTH_HANDLE_L94HNSW.invoke(entry);
            return Map.of(HNSW_VECTOR_INDEX_EXTENSION, graph, FLAT_VECTOR_DATA_EXTENSION, raw);
        } catch (Throwable t) {
            handleThrowable(t);
        }
        throw new AssertionError("should not reach here");
    }

    @SuppressForbidden(reason = "static type is not accessible")
    static Map<String, Long> getOffHeapByteSizeL95HNSW(Lucene95HnswVectorsReader reader, FieldInfo fieldInfo) {
        try {
            var entry = GET_FIELD_ENTRY_HANDLE_L95HNSW.invoke(reader, fieldInfo.name, fieldInfo.getVectorEncoding());
            long graph = (long) GET_VECTOR_INDEX_LENGTH_HANDLE_L95HNSW.invoke(entry);
            long raw = (long) GET_VECTOR_DATA_LENGTH_HANDLE_L95HNSW.invoke(entry);
            return Map.of(HNSW_VECTOR_INDEX_EXTENSION, graph, FLAT_VECTOR_DATA_EXTENSION, raw);
        } catch (Throwable t) {
            handleThrowable(t);
        }
        throw new AssertionError("should not reach here");
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
