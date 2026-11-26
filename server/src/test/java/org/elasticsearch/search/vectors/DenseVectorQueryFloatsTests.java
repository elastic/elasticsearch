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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.VectorUtil.l2normalize;

public class DenseVectorQueryFloatsTests extends AbstractDenseVectorQueryTestCase {
    @Override
    DenseVectorQuery getDenseVectorQuery(String field, float[] query) {
        return DenseVectorQuery.Floats.codecScored(query, field);
    }

    @Override
    Query getDenseVectorQuery(String field, float[] query, Query filter) {
        return DenseVectorQuery.Floats.codecScored(query, field).filteredBy(filter);
    }

    @Override
    float[] randomVector(int dim) {
        return VectorTestUtils.randomFloatVector(dim);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
        return new KnnFloatVectorField(name, vector, similarityFunction);
    }

    @Override
    public void testScoreCosine() throws IOException {
        // cosine is normalized to use dot_product comparison for floats
        float[][] vectors = new float[5][];
        for (int j = 1; j <= 5; j++) {
            vectors[j - 1] = l2normalize(new float[] { j, j * j });
        }
        try (Directory d = getStableIndexStore("field", DOT_PRODUCT, vectors)) {
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                float[] queryVector = l2normalize(new float[] { 2, 3 });
                DenseVectorQuery query = getDenseVectorQuery("field", queryVector);
                Query rewritten = query.rewrite(searcher);
                Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
                Scorer scorer = weight.scorer(reader.leaves().get(0));

                // prior to advancing, score is undefined
                assertEquals(-1, scorer.docID());
                DocIdSetIterator it = scorer.iterator();
                assertEquals(5, it.cost());
                it.nextDoc();
                int curDoc = 0;
                // iterate the docs and assert the scores are what we expect
                while (it.docID() != NO_MORE_DOCS) {
                    assertEquals(DOT_PRODUCT.compare(vectors[curDoc], queryVector), scorer.score(), 0.0001);
                    curDoc++;
                    it.nextDoc();
                }
            }
        }
    }
}
