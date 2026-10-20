/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.SequentialVectorValues;
import org.elasticsearch.index.codec.vectors.SequentialVectorValuesTests;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/** Exitable vector values read sequentially when the format supports it, and still check for cancellation. */
public class ExitableFloatVectorValuesTests extends ESTestCase {

    public void testSequentialKeepsCheckingCancellation() throws IOException {
        AtomicBoolean cancelled = new AtomicBoolean();
        ExitableDirectoryReader.QueryCancellation cancellation = new ExitableDirectoryReader.QueryCancellation() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public void checkCancelled() {
                if (cancelled.get()) {
                    throw new TaskCancelledException("cancelled");
                }
            }
        };
        VectorReadHintsTests.Case quantized = VectorReadHintsTests.cases()
            .stream()
            .filter(c -> c.name().equals("ES93HnswScalarQuantizedVectorsFormat"))
            .findFirst()
            .orElseThrow();
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(quantized.codec(null)))) {
                for (int i = 0; i < 256; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("field", VectorReadHintsTests.randomVector(), VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = new ExitableDirectoryReader(DirectoryReader.open(dir), cancellation)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                FloatVectorValues values = leaf.getFloatVectorValues("field");
                FloatVectorValues sequential = SequentialVectorValues.sequential(values);
                assertThat(sequential, not(sameInstance(values)));
                assertThat("still exitable", sequential.getClass(), equalTo(values.getClass()));
                SequentialVectorValuesTests.assertSameVectors(leaf.getFloatVectorValues("field"), sequential);

                cancelled.set(true);
                KnnVectorValues.DocIndexIterator iterator = sequential.iterator();
                expectThrows(TaskCancelledException.class, () -> {
                    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        sequential.vectorValue(iterator.index());
                    }
                });
            }
        }
    }
}
