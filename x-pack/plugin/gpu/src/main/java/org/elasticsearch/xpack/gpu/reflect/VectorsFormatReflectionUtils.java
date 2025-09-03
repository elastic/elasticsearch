/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.reflect;

import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.vectors.ES814ScalarQuantizedVectorsFormat;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class VectorsFormatReflectionUtils {

    private static final VarHandle FLAT_VECTOR_DATA_HANDLE;
    private static final VarHandle QUANTIZED_VECTOR_DATA_HANDLE;
    private static final VarHandle DELEGATE_WRITER_HANDLE;

    static final Class<?> L99_SQ_VW_CLS = Lucene99ScalarQuantizedVectorsWriter.class;
    static final Class<?> L99_F_VW_CLS = Lucene99FlatVectorsWriter.class;
    static final Class<?> ES814_SQ_VW_CLS = ES814ScalarQuantizedVectorsFormat.ES814ScalarQuantizedVectorsWriter.class;

    static {
        try {
            var lookup = MethodHandles.privateLookupIn(L99_F_VW_CLS, MethodHandles.lookup());
            FLAT_VECTOR_DATA_HANDLE = lookup.findVarHandle(L99_F_VW_CLS, "vectorData", IndexOutput.class);

            lookup = MethodHandles.privateLookupIn(L99_SQ_VW_CLS, MethodHandles.lookup());
            QUANTIZED_VECTOR_DATA_HANDLE = lookup.findVarHandle(L99_SQ_VW_CLS, "quantizedVectorData", IndexOutput.class);

            lookup = MethodHandles.privateLookupIn(ES814_SQ_VW_CLS, MethodHandles.lookup());
            DELEGATE_WRITER_HANDLE = lookup.findVarHandle(ES814_SQ_VW_CLS, "delegate", L99_SQ_VW_CLS);

        } catch (IllegalAccessException e) {
            throw new AssertionError("should not happen, check opens", e);
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    public static IndexOutput getVectorDataIndexOutput(FlatVectorsWriter flatVectorWriter) {
        assert flatVectorWriter instanceof ES814ScalarQuantizedVectorsFormat.ES814ScalarQuantizedVectorsWriter;
        var delegate = (Lucene99ScalarQuantizedVectorsWriter)DELEGATE_WRITER_HANDLE.get(flatVectorWriter);
        return (IndexOutput) QUANTIZED_VECTOR_DATA_HANDLE.get(delegate);
    }

    public static IndexOutput getQuantizedVectorDataIndexOutput(FlatVectorsWriter flatVectorWriter) {
        assert flatVectorWriter instanceof Lucene99FlatVectorsWriter;
        return (IndexOutput) FLAT_VECTOR_DATA_HANDLE.get(flatVectorWriter);
    }
}
