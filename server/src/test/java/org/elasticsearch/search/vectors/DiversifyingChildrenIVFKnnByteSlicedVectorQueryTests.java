/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.index.mapper.RoutingFieldMapper;

/** Tests for {@link DiversifyingChildrenIVFKnnByteSlicedVectorQuery}. */
public class DiversifyingChildrenIVFKnnByteSlicedVectorQueryTests extends AbstractDiversifyingChildrenIVFKnnSlicedVectorQueryTestCase<
    byte[]> {

    @Override
    byte[] vector(int... components) {
        byte[] v = new byte[components.length];
        for (int i = 0; i < components.length; i++) {
            v[i] = (byte) components[i];
        }
        return v;
    }

    @Override
    byte[][] createVectorArray(int size) {
        return new byte[size][];
    }

    @Override
    byte[] randomVector(int dim) {
        byte[] v = new byte[dim];
        random().nextBytes(v);
        return v;
    }

    @Override
    byte[] randomDenseQueryVector(int dim) {
        return randomVector(dim);
    }

    @Override
    Query getDiversifyingChildrenKnnQuery(String fieldName, byte[] queryVector, Query childFilter, int k, BitSetProducer parentBitSet) {
        return new DiversifyingChildrenIVFKnnByteSlicedVectorQuery(
            fieldName,
            queryVector,
            k,
            k,
            childFilter,
            parentBitSet,
            0,
            testResolver(),
            RoutingFieldMapper.NAME,
            SLICE_ZERO
        );
    }

    @Override
    Query createSlicedDiversifyingQuery(
        String field,
        byte[] vector,
        int k,
        int numCands,
        Query filter,
        BitSetProducer parents,
        float visitRatio,
        TestIvfQueryConfigResolver resolver,
        String routingField,
        BytesRef... slices
    ) {
        return new DiversifyingChildrenIVFKnnByteSlicedVectorQuery(
            field,
            vector,
            k,
            numCands,
            filter,
            parents,
            visitRatio,
            resolver,
            routingField,
            slices
        );
    }

    @Override
    Field getKnnVectorField(String name, byte[] vector) {
        return new KnnByteVectorField(name, vector, VectorSimilarityFunction.EUCLIDEAN);
    }

    public void testToString() {
        DiversifyingChildrenIVFKnnByteSlicedVectorQuery q = new DiversifyingChildrenIVFKnnByteSlicedVectorQuery(
            "vector",
            new byte[] { 0, 1 },
            4,
            4,
            null,
            parent -> null,
            0.1f,
            testResolver(),
            RoutingFieldMapper.NAME,
            SLICE_ZERO
        );
        assertEquals(
            "DiversifyingChildrenIVFKnnByteSlicedVectorQuery:vector[0,...][4][" + RoutingFieldMapper.NAME + "=[0]]",
            q.toString("ignored")
        );
    }
}
