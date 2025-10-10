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
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class VectorsFormatReflectionUtils {
    private static final VarHandle SUPPLIER_HANDLE;
    private static final VarHandle FLOAT_VECTORS_HANDLE;
    private static final VarHandle BYTE_VECTORS_HANDLE;

    private static final Class<?> FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS;
    private static final Class<?> FLOAT_SCORING_SUPPLIER_CLASS;
    private static final Class<?> BYTE_SCORING_SUPPLIER_CLASS;
    static {
        try {
            FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS = Class.forName(
                "org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter$FlatCloseableRandomVectorScorerSupplier"
            );
            var lookup = MethodHandles.privateLookupIn(FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS, MethodHandles.lookup());
            SUPPLIER_HANDLE = lookup.findVarHandle(
                FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS,
                "supplier",
                RandomVectorScorerSupplier.class
            );

            FLOAT_SCORING_SUPPLIER_CLASS = Class.forName(
                "org.apache.lucene.internal.vectorization.Lucene99MemorySegmentFloatVectorScorerSupplier"
            );
            lookup = MethodHandles.privateLookupIn(FLOAT_SCORING_SUPPLIER_CLASS, MethodHandles.lookup());
            FLOAT_VECTORS_HANDLE = lookup.findVarHandle(
                FLOAT_SCORING_SUPPLIER_CLASS,
                "values",
                FloatVectorValues.class
            );

            BYTE_SCORING_SUPPLIER_CLASS = Class.forName(
                "org.apache.lucene.internal.vectorization.Lucene99MemorySegmentByteVectorScorerSupplier"
            );
            lookup = MethodHandles.privateLookupIn(BYTE_SCORING_SUPPLIER_CLASS, MethodHandles.lookup());
            BYTE_VECTORS_HANDLE = lookup.findVarHandle(
                BYTE_SCORING_SUPPLIER_CLASS,
                "values",
                KnnVectorValues.class
            );

        } catch (IllegalAccessException e) {
            throw new AssertionError("should not happen, check opens", e);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    public static RandomVectorScorerSupplier getFlatCloseableRandomVectorScorerInnerSupplier(
        CloseableRandomVectorScorerSupplier scorerSupplier
    ) {
        assert scorerSupplier.getClass().equals(FLAT_CLOSEABLE_RANDOM_VECTOR_SCORER_SUPPLIER_CLASS);
        return (RandomVectorScorerSupplier) SUPPLIER_HANDLE.get(scorerSupplier);
    }

    public static HasIndexSlice getFloatScoringSupplierVectorOrNull(RandomVectorScorerSupplier supplier) {
        if (supplier.getClass().equals(FLOAT_SCORING_SUPPLIER_CLASS)) {
            var vectorValues = FLOAT_VECTORS_HANDLE.get(supplier);
            if (vectorValues instanceof HasIndexSlice indexSlice) {
                return indexSlice;
            }
        }
        return null;
    }

    public static HasIndexSlice getByteScoringSupplierVectorOrNull(RandomVectorScorerSupplier supplier) {
        if (supplier.getClass().equals(BYTE_SCORING_SUPPLIER_CLASS)) {
            var vectorValues = BYTE_VECTORS_HANDLE.get(supplier);
            if (vectorValues instanceof HasIndexSlice indexSlice) {
                return indexSlice;
            }
        }
        return null;
    }
}
