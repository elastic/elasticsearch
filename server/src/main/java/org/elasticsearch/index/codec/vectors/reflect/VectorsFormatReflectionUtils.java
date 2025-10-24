/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.reflect;

import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.elasticsearch.simdvec.QuantizedByteVectorValuesAccess;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class VectorsFormatReflectionUtils {
    private static final VarHandle FLOAT_SUPPLIER_HANDLE;
    private static final VarHandle BYTE_SUPPLIER_HANDLE;
    private static final VarHandle FLOAT_VECTORS_HANDLE;

    private static final Class<?> FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS;
    private static final Class<?> SCALAR_QUANTIZED_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS;
    private static final Class<?> FLOAT_SCORING_SUPPLIER_CLASS;
    static {
        try {
            FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS = Class.forName(
                "org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter$FlatCloseableRandomVectorScorerSupplier"
            );
            var lookup = MethodHandles.privateLookupIn(FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS, MethodHandles.lookup());
            FLOAT_SUPPLIER_HANDLE = lookup.findVarHandle(
                FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS,
                "supplier",
                RandomVectorScorerSupplier.class
            );

            FLOAT_SCORING_SUPPLIER_CLASS = Class.forName(
                "org.apache.lucene.internal.vectorization.Lucene99MemorySegmentFloatVectorScorerSupplier"
            );
            lookup = MethodHandles.privateLookupIn(FLOAT_SCORING_SUPPLIER_CLASS, MethodHandles.lookup());
            FLOAT_VECTORS_HANDLE = lookup.findVarHandle(FLOAT_SCORING_SUPPLIER_CLASS, "values", FloatVectorValues.class);

            SCALAR_QUANTIZED_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS = Class.forName(
                Lucene99ScalarQuantizedVectorsWriter.class.getCanonicalName() + "$ScalarQuantizedCloseableRandomVectorScorerSupplier"
            );
            lookup = MethodHandles.privateLookupIn(SCALAR_QUANTIZED_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS, MethodHandles.lookup());
            BYTE_SUPPLIER_HANDLE = lookup.findVarHandle(
                SCALAR_QUANTIZED_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS,
                "supplier",
                RandomVectorScorerSupplier.class
            );

        } catch (IllegalAccessException e) {
            throw new AssertionError("should not happen, check opens", e);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    public static RandomVectorScorerSupplier getFlatRandomVectorScorerInnerSupplier(CloseableRandomVectorScorerSupplier scorerSupplier) {
        if (scorerSupplier.getClass().equals(FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS)) {
            return (RandomVectorScorerSupplier) FLOAT_SUPPLIER_HANDLE.get(scorerSupplier);
        }
        return null;
    }

    public static RandomVectorScorerSupplier getScalarQuantizedRandomVectorScorerInnerSupplier(
        CloseableRandomVectorScorerSupplier scorerSupplier
    ) {
        if (scorerSupplier.getClass().equals(SCALAR_QUANTIZED_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS)) {
            return (RandomVectorScorerSupplier) BYTE_SUPPLIER_HANDLE.get(scorerSupplier);
        }
        return null;
    }

    public static HasIndexSlice getFloatScoringSupplierVectorOrNull(RandomVectorScorerSupplier scorerSupplier) {
        if (FLOAT_SCORING_SUPPLIER_CLASS.isAssignableFrom(scorerSupplier.getClass())) {
            var vectorValues = FLOAT_VECTORS_HANDLE.get(scorerSupplier);
            if (vectorValues instanceof HasIndexSlice indexSlice) {
                return indexSlice;
            }
        }
        return null;
    }

    public static HasIndexSlice getByteScoringSupplierVectorOrNull(RandomVectorScorerSupplier scorerSupplier) {
        if (scorerSupplier instanceof QuantizedByteVectorValuesAccess quantizedByteVectorValuesAccess) {
            return quantizedByteVectorValuesAccess.get();
        }
        return null;
    }
}
