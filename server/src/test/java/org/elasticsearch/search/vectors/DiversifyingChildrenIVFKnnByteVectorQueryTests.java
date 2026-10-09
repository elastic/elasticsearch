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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.junit.Before;

public class DiversifyingChildrenIVFKnnByteVectorQueryTests extends AbstractDiversifyingChildrenIVFKnnVectorQueryTestCase<byte[]> {

    @Before
    public void setUpByteFormat() {
        format = new ESNextDiskBBQVectorsFormat(128, 4, null);
    }

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
    Query getDiversifyingChildrenKnnQuery(String fieldName, byte[] queryVector, Query childFilter, int k, BitSetProducer parentBitSet) {
        return new DiversifyingChildrenIVFKnnByteVectorQuery(fieldName, queryVector, k, k, childFilter, parentBitSet, 0, testResolver());
    }

    @Override
    Field getKnnVectorField(String name, byte[] vector) {
        return new KnnByteVectorField(name, vector);
    }
}
