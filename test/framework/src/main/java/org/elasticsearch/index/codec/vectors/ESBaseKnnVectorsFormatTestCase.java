/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Common superclass for every Elasticsearch {@code KnnVectorsFormat} test, sitting between our
 * {@code getCodec()}-implementing test classes and Lucene's {@link BaseKnnVectorsFormatTestCase}.
 *
 * <p>Anything Elasticsearch-specific that every vector format test should get "for free" belongs here,
 * rather than being duplicated (or, worse, forgotten) in individual {@code *FormatTests} classes: it
 * runs for every format under test simply because the class extends this one, with no per-format
 * opt-in required.
 */
public abstract class ESBaseKnnVectorsFormatTestCase extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    private static final String FIELD = "vector";

    /**
     * {@code KnnVectorsReader#getVectorCount} has a default implementation that opens the field's
     * vector values and calls {@code size()}. That's correct but, for a sparse field, constructing
     * those values builds an {@code IndexedDISI} over a jump table that gets prefetched -- exactly
     * the cost this API exists to let callers avoid (see its javadoc). Every Elasticsearch reader
     * that wraps a Lucene format must override {@code getVectorCount} to read the count from
     * segment metadata instead.
     *
     * <p>Lucene's own inherited {@code testVectorCount()}/{@code testGetVectorCountInvalidField()}
     * only check the *value* {@code getVectorCount} returns, and pass whether or not the override
     * exists, since the default fallback is still correct -- just expensive. This test instead
     * observes whether counting actually touched the directory, so it fails for a reader (at any
     * nesting depth) that forgot the override.
     */
    public void testGetVectorCountDoesNotOpenVectorValues() throws Exception {
        VectorEncoding encoding = randomVectorEncoding();
        VectorSimilarityFunction similarity = randomSimilarity();
        int dim = TestUtil.nextInt(random(), 1, 20);
        if (dim % 2 != 0) {
            dim++; // some formats (e.g. int4 quantization) require even dimensions
        }
        int numDocs = atLeast(200);

        try (Directory rawDir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(rawDir, newIndexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    if (i % 10 == 0) {
                        // sparse: only one in ten documents has the field, forcing the vector
                        // values (when opened) through an IndexedDISI rather than a dense path
                        addVectorField(doc, encoding, dim, similarity);
                    }
                    w.addDocument(doc);
                }
                w.commit();
            }

            AtomicInteger prefetches = new AtomicInteger();
            Directory dir = new PrefetchCountingDirectory(rawDir, prefetches);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                boolean checkedAtLeastOneLeaf = false;
                for (LeafReaderContext ctx : reader.leaves()) {
                    LeafReader leafReader = ctx.reader();
                    FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(FIELD);
                    if (fieldInfo == null || fieldInfo.getVectorDimension() <= 0 || (leafReader instanceof CodecReader) == false) {
                        continue;
                    }
                    KnnVectorsReader knnVectorsReader = ((CodecReader) leafReader).getVectorReader();
                    if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                        knnVectorsReader = fieldsReader.getFieldReader(FIELD);
                    }

                    int before = prefetches.get();
                    int count = knnVectorsReader.getVectorCount(fieldInfo);
                    assertThat(
                        "getVectorCount() opened the vector values instead of reading the count from metadata; reader="
                            + knnVectorsReader.getClass(),
                        prefetches.get(),
                        equalTo(before)
                    );
                    assertThat(count, greaterThan(0));
                    checkedAtLeastOneLeaf = true;
                }
                assertTrue("expected at least one leaf with the vector field", checkedAtLeastOneLeaf);
            }
        }
    }

    private void addVectorField(Document doc, VectorEncoding encoding, int dim, VectorSimilarityFunction similarity) {
        switch (encoding) {
            case BYTE -> doc.add(new KnnByteVectorField(FIELD, randomVector8(dim), similarity));
            case FLOAT32 -> doc.add(new KnnFloatVectorField(FIELD, randomNormalizedVector(dim), similarity));
            default -> throw new AssertionError("unexpected vector encoding: " + encoding);
        }
    }

    /** Counts {@link IndexInput#prefetch} calls, including on slices and clones. */
    private static class PrefetchCountingDirectory extends FilterDirectory {

        private final AtomicInteger prefetches;

        PrefetchCountingDirectory(Directory in, AtomicInteger prefetches) {
            super(in);
            this.prefetches = prefetches;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new CountingIndexInput(super.openInput(name, context), prefetches);
        }
    }

    private static class CountingIndexInput extends FilterIndexInput {

        private final AtomicInteger prefetches;

        CountingIndexInput(IndexInput in, AtomicInteger prefetches) {
            super("CountingIndexInput(" + in + ")", in);
            this.prefetches = prefetches;
        }

        @Override
        public void prefetch(long offset, long length) throws IOException {
            prefetches.incrementAndGet();
            in.prefetch(offset, length);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new CountingIndexInput(in.slice(sliceDescription, offset, length), prefetches);
        }

        @Override
        public IndexInput clone() {
            return new CountingIndexInput(in.clone(), prefetches);
        }
    }
}
