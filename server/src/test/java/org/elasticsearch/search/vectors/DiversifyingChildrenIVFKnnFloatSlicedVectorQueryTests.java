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
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.index.mapper.RoutingFieldMapper;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;

/** Tests for {@link DiversifyingChildrenIVFKnnFloatSlicedVectorQuery}. */
public class DiversifyingChildrenIVFKnnFloatSlicedVectorQueryTests extends AbstractDiversifyingChildrenIVFKnnSlicedVectorQueryTestCase<
    float[]> {

    @Override
    float[] vector(int... components) {
        float[] v = new float[components.length];
        for (int i = 0; i < components.length; i++) {
            v[i] = components[i];
        }
        return v;
    }

    @Override
    float[][] createVectorArray(int size) {
        return new float[size][];
    }

    @Override
    float[] randomVector(int dim) {
        return VectorTestUtils.randomNormalizedFloatVector(dim);
    }

    @Override
    float[] randomDenseQueryVector(int dim) {
        float[] vec = new float[dim];
        for (int i = 0; i < dim; i++) {
            vec[i] = randomFloat();
        }
        VectorUtil.l2normalize(vec);
        return vec;
    }

    @Override
    Query getDiversifyingChildrenKnnQuery(String fieldName, float[] queryVector, Query childFilter, int k, BitSetProducer parentBitSet) {
        return new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
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
        float[] vector,
        int k,
        int numCands,
        Query filter,
        BitSetProducer parents,
        float visitRatio,
        TestIvfQueryConfigResolver resolver,
        String routingField,
        BytesRef... slices
    ) {
        return new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
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
    Field getKnnVectorField(String name, float[] vector) {
        return new KnnFloatVectorField(name, vector);
    }

    public void testToString() {
        DiversifyingChildrenIVFKnnFloatSlicedVectorQuery q = new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
            "vec",
            new float[] { 0.5f, 0.5f },
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
            "DiversifyingChildrenIVFKnnFloatSlicedVectorQuery:vec[0.5,...][4][" + RoutingFieldMapper.NAME + "=[0]]",
            q.toString("ignored")
        );
    }
}
