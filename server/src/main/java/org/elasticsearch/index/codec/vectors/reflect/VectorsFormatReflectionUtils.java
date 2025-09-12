/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.reflect;

import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.index.codec.vectors.ES814ScalarQuantizedVectorsFormat;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;

public class VectorsFormatReflectionUtils {

    private static final VarHandle FLAT_VECTOR_DATA_HANDLE;
    private static final VarHandle QUANTIZED_VECTOR_DATA_HANDLE;
    private static final VarHandle RAW_DELEGATE_WRITER_HANDLE;
    private static final VarHandle RAW_FIELD_DELEGATE_WRITER_HANDLE;

    private static final MethodHandle lucene99FlatVectorsWriter_writeField$mh;
    private static final MethodHandle lucene99FlatVectorsWriter_writeSortingField$mh;

    private static final MethodHandle lucene99ScalarQuantizedVectorsWriter_writeField$mh;
    private static final MethodHandle lucene99ScalarQuantizedVectorsWriter_writeSortingField$mh;
    private static final MethodHandle lucene99ScalarQuantizedVectorsWriter_FieldWriter_createQuantizer$mh;

    static final Class<?> L99_SQ_VW_CLS = Lucene99ScalarQuantizedVectorsWriter.class;
    static final Class<?> L99_SQ_VW_FIELD_WRITER_CLS;
    static final Class<?> L99_F_VW_CLS = Lucene99FlatVectorsWriter.class;
    static final Class<?> L99_F_VW_FIELD_WRITER_CLS;

    static {
        try {
            L99_F_VW_FIELD_WRITER_CLS = Class.forName("org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter$FieldWriter");
            L99_SQ_VW_FIELD_WRITER_CLS = Class.forName(
                "org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter$FieldWriter"
            );
            var lookup = MethodHandles.privateLookupIn(L99_F_VW_CLS, MethodHandles.lookup());
            FLAT_VECTOR_DATA_HANDLE = lookup.findVarHandle(L99_F_VW_CLS, "vectorData", IndexOutput.class);
            lucene99FlatVectorsWriter_writeField$mh = lookup.findVirtual(
                L99_F_VW_CLS,
                "writeField",
                MethodType.methodType(void.class, L99_F_VW_FIELD_WRITER_CLS, int.class)
            );

            lucene99FlatVectorsWriter_writeSortingField$mh = lookup.findVirtual(
                L99_F_VW_CLS,
                "writeSortingField",
                MethodType.methodType(void.class, L99_F_VW_FIELD_WRITER_CLS, int.class, Sorter.DocMap.class)
            );

            lookup = MethodHandles.privateLookupIn(L99_SQ_VW_CLS, MethodHandles.lookup());
            QUANTIZED_VECTOR_DATA_HANDLE = lookup.findVarHandle(L99_SQ_VW_CLS, "quantizedVectorData", IndexOutput.class);
            RAW_DELEGATE_WRITER_HANDLE = lookup.findVarHandle(L99_SQ_VW_CLS, "rawVectorDelegate", FlatVectorsWriter.class);
            lucene99ScalarQuantizedVectorsWriter_writeField$mh = lookup.findVirtual(
                L99_SQ_VW_CLS,
                "writeField",
                MethodType.methodType(void.class, L99_SQ_VW_FIELD_WRITER_CLS, int.class, ScalarQuantizer.class)
            );
            lucene99ScalarQuantizedVectorsWriter_writeSortingField$mh = lookup.findVirtual(
                L99_SQ_VW_CLS,
                "writeSortingField",
                MethodType.methodType(void.class, L99_SQ_VW_FIELD_WRITER_CLS, int.class, Sorter.DocMap.class, ScalarQuantizer.class)
            );
            RAW_FIELD_DELEGATE_WRITER_HANDLE = lookup.findVarHandle(
                L99_SQ_VW_FIELD_WRITER_CLS,
                "flatFieldVectorsWriter",
                FlatFieldVectorsWriter.class
            );
            lucene99ScalarQuantizedVectorsWriter_FieldWriter_createQuantizer$mh = lookup.findVirtual(
                L99_SQ_VW_FIELD_WRITER_CLS,
                "createQuantizer",
                MethodType.methodType(ScalarQuantizer.class)
            );
        } catch (IllegalAccessException e) {
            throw new AssertionError("should not happen, check opens", e);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    public static IndexOutput getQuantizedVectorDataIndexOutput(FlatVectorsWriter flatVectorWriter) {
        assert flatVectorWriter instanceof ES814ScalarQuantizedVectorsFormat.ES814ScalarQuantizedVectorsWriter;
        var rawVectorDelegate = getRawVectorDelegate(
            (ES814ScalarQuantizedVectorsFormat.ES814ScalarQuantizedVectorsWriter) flatVectorWriter
        );
        return (IndexOutput) FLAT_VECTOR_DATA_HANDLE.get(rawVectorDelegate);
    }

    public static IndexOutput getVectorDataIndexOutput(FlatVectorsWriter flatVectorWriter) {
        assert flatVectorWriter instanceof Lucene99FlatVectorsWriter;
        return (IndexOutput) FLAT_VECTOR_DATA_HANDLE.get(flatVectorWriter);
    }

    // private void Lucene99FlatVectorsWriter#writeField(FieldWriter<?> fieldData, int maxDoc)
    public static void lucene99FlatVectorsWriter_writeField(
        Lucene99FlatVectorsWriter that,
        FlatFieldVectorsWriter<?> fieldData,
        int maxDoc
    ) {
        try {
            lucene99FlatVectorsWriter_writeField$mh.invoke(that, fieldData, maxDoc);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // private void Lucene99FlatVectorsWriter#writeSortingField(FieldWriter<?> fieldData, int maxDoc)
    public static void lucene99FlatVectorsWriter_writeSortingField(
        Lucene99FlatVectorsWriter that,
        FlatFieldVectorsWriter<?> fieldData,
        int maxDoc,
        Sorter.DocMap sortMap
    ) {
        try {
            lucene99FlatVectorsWriter_writeSortingField$mh.invoke(that, fieldData, maxDoc, sortMap);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // private void Lucene99FlatVectorsWriter#writeField(FieldWriter<?> fieldData, int maxDoc)
    public static void lucene99ScalarQuantizedVectorsWriter_writeField(
        Lucene99ScalarQuantizedVectorsWriter that,
        FlatFieldVectorsWriter<?> fieldData,
        int maxDoc,
        ScalarQuantizer scalarQuantizer
    ) {
        try {
            lucene99ScalarQuantizedVectorsWriter_writeField$mh.invoke(that, fieldData, maxDoc, scalarQuantizer);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // private void Lucene99FlatVectorsWriter#writeSortingField(FieldWriter<?> fieldData, int maxDoc)
    public static void lucene99ScalarQuantizedVectorsWriter_writeSortingField(
        Lucene99ScalarQuantizedVectorsWriter that,
        FlatFieldVectorsWriter<?> fieldData,
        int maxDoc,
        Sorter.DocMap sortMap,
        ScalarQuantizer scalarQuantizer
    ) {
        try {
            lucene99ScalarQuantizedVectorsWriter_writeSortingField$mh.invoke(that, fieldData, maxDoc, sortMap, scalarQuantizer);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static ScalarQuantizer lucene99ScalarQuantizedVectorsWriter_FieldWriter_createQuantizer(FlatFieldVectorsWriter<?> fieldData) {
        try {
            return (ScalarQuantizer) lucene99ScalarQuantizedVectorsWriter_FieldWriter_createQuantizer$mh.invoke(fieldData);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static Lucene99FlatVectorsWriter getRawVectorDelegate(
        ES814ScalarQuantizedVectorsFormat.ES814ScalarQuantizedVectorsWriter quantizedVectorsWriter
    ) {
        return (Lucene99FlatVectorsWriter) RAW_DELEGATE_WRITER_HANDLE.get(quantizedVectorsWriter.delegate);
    }

    public static FlatFieldVectorsWriter<float[]> getRawFieldVectorDelegate(FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
        if (L99_F_VW_FIELD_WRITER_CLS.isAssignableFrom(flatFieldVectorsWriter.getClass())) {
            return flatFieldVectorsWriter;
        } else {
            assert L99_SQ_VW_FIELD_WRITER_CLS.isAssignableFrom(flatFieldVectorsWriter.getClass());
            @SuppressWarnings("unchecked")
            var rawFieldVectorDelegate = (FlatFieldVectorsWriter<float[]>) RAW_FIELD_DELEGATE_WRITER_HANDLE.get(flatFieldVectorsWriter);
            return rawFieldVectorDelegate;
        }
    }
}
