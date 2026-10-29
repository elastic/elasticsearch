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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.junit.Before;

import java.io.IOException;

public class IVFKnnByteVectorQueryTests extends AbstractIVFKnnVectorQueryTestCase<byte[]> {

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
    IVFKnnByteVectorQuery getKnnVectorQuery(String field, byte[] query, int k, Query queryFilter, float visitRatio) {
        return new IVFKnnByteVectorQuery(field, query, k, k, queryFilter, visitRatio, testResolver());
    }

    @Override
    byte[] randomVector(int dim) {
        byte[] v = new byte[dim];
        random().nextBytes(v);
        return v;
    }

    @Override
    Field getKnnVectorField(String name, byte[] vector, VectorSimilarityFunction similarityFunction) {
        return new KnnByteVectorField(name, vector, similarityFunction);
    }

    @Override
    Field getKnnVectorField(String name, byte[] vector) {
        return new KnnByteVectorField(name, vector);
    }

    @Override
    boolean supportsCosine() {
        return false;
    }

    public void testToString() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", vector(0, 1), vector(1, 2), vector(0, 0));
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new byte[] { 0, 1 }, 10);
            assertEquals("IVFKnnByteVectorQuery:field[0,...][10]", query.toString("ignored"));

            assertDocScoreQueryToString(query.rewrite(newSearcher(reader)));

            // test with filter
            Query filter = new TermQuery(new Term("id", "text"));
            query = getKnnVectorQuery("field", new byte[] { 0, 1 }, 10, filter);
            assertEquals("IVFKnnByteVectorQuery:field[0,...][10][id:text]", query.toString("ignored"));
        }
    }
}
