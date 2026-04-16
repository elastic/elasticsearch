/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public abstract class BaseQuantizedHnswVectorsFormatTestCase extends BaseHnswVectorsFormatTestCase {

    public void testRescoreUsesRawVectorSlice() throws IOException {
        int dimension = random().nextInt(12, 64);
        float[] vector = randomVector(dimension);
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("f", vector, DOT_PRODUCT));
                w.addDocument(doc);
                w.commit();
            }
            DirectoryReader reader = DirectoryReader.open(dir);
            ContextIndexSearcher searcher = new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(),
                random().nextBoolean()
            );
            searcher.addQueryCancellation(() -> {});
            DirectoryReader wrapped = searcher.getDirectoryReader();
            try {
                LeafReader leaf = getOnlyLeafReader(wrapped);
                FloatVectorValues vectorValues = leaf.getFloatVectorValues("f");
                assertThat(vectorValues, instanceOf(HasIndexSlice.class));
                assertThat(vectorValues.getVectorByteLength(), equalTo(dimension * Float.BYTES));

                float[] expected = Arrays.copyOf(vector, vector.length);
                assertArrayEquals(expected, vectorValues.vectorValue(0), 0f);

                HasIndexSlice sliceable = (HasIndexSlice) vectorValues;
                float[] sliceValues = new float[dimension];
                assertNotNull(sliceable.getSlice());
                var slice = sliceable.getSlice().clone();
                slice.seek(0L);
                slice.readFloats(sliceValues, 0, dimension);
                assertArrayEquals(expected, sliceValues, 0f);
            } finally {
                wrapped.close();
                searcher.close();
            }
        }
    }
}
