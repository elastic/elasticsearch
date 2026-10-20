/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NoReuseHint;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests.Case;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests.Open;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests.RecordingDirectory;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.search.internal.FilterFloatVectorValues;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/** Raw vectors kept only to rescore are read sequentially; closing the directory checks the sequential reader was closed. */
public class SequentialVectorValuesTests extends MapperServiceTestCase {

    private static final int NUM_DOCS = 256;

    private final Case each;

    public SequentialVectorValuesTests(Case each) {
        this.each = each;
    }

    @ParametersFactory(argumentFormatting = "%s")
    public static Iterable<Object[]> parameters() {
        return VectorReadHintsTests.cases().stream().map(c -> new Object[] { c }).toList();
    }

    /** Whether {@code each} reads the raw vectors in doc order through a mapping of their own: every format that rescores from them. */
    public static boolean readsSequentially(Case each) {
        return each.rescoresFromRaw();
    }

    private boolean readsSequentially() {
        return readsSequentially(each);
    }

    public void testSequentialReadsTheSameVectors() throws IOException {
        List<Open> opens = new CopyOnWriteArrayList<>();
        boolean compound = randomBoolean();
        try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
            index(dir, compound);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                opens.clear();
                FloatVectorValues sequential = SequentialVectorValues.sequential(leaf.getFloatVectorValues("field"));
                if (readsSequentially() == false) {
                    assertThat("nothing is mapped again, " + opens, opens, empty());
                }
                if (readsSequentially() && compound == false) {
                    assertThat(opens, hasItem(sequentialRawVectors()));
                }
                if (readsSequentially() && compound) {
                    // the compound file is mapped again, for merges and reads in doc order, apart from the one searches use
                    assertThat(opens.stream().filter(open -> open.name().endsWith(".cfs")).toList(), hasSize(1));
                }

                FloatVectorValues values = leaf.getFloatVectorValues("field");
                assertSameVectors(values, sequential);
                float[] target = VectorReadHintsTests.randomVector();
                assertSameScores(values.rescorer(target), sequential.rescorer(target));

                opens.clear();
                SequentialVectorValues.sequential(leaf.getFloatVectorValues("field"));
                assertThat("reads in doc order share one mapping, " + opens, opens, empty());
            }
        }
    }

    public void testConcurrentReadsInDocOrder() throws Exception {
        try (Directory dir = newDirectory()) {
            index(dir, randomBoolean());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                Thread[] threads = new Thread[randomIntBetween(2, 8)];
                CyclicBarrier start = new CyclicBarrier(threads.length);
                List<Throwable> failures = new CopyOnWriteArrayList<>();
                for (int t = 0; t < threads.length; t++) {
                    threads[t] = new Thread(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < 5; i++) {
                                FloatVectorValues sequential = SequentialVectorValues.sequential(leaf.getFloatVectorValues("field"));
                                assertSameVectors(leaf.getFloatVectorValues("field"), sequential);
                            }
                        } catch (Throwable e) {
                            failures.add(e);
                        }
                    });
                    threads[t].start();
                }
                for (Thread thread : threads) {
                    thread.join();
                }
                assertThat(failures, empty());
            }
        }
    }

    public void testReadsAtRandomWhenTheFilesAreGone() throws IOException {
        try (NoMergeOpensDirectory dir = new NoMergeOpensDirectory(newDirectory())) {
            index(dir, false);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                dir.filesGone = true;
                LeafReader leaf = getOnlyLeafReader(reader);
                FloatVectorValues values = leaf.getFloatVectorValues("field");
                assertThat(SequentialVectorValues.sequential(values), sameInstance(values));
                assertSameVectors(leaf.getFloatVectorValues("field"), values);
            }
        }
    }

    /** A wrapper that does not support reading sequentially reads as it did. */
    public void testOtherWrappersReadAsTheyDid() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, false);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                FloatVectorValues wrapper = new Wrapper(getOnlyLeafReader(reader).getFloatVectorValues("field"));
                assertThat(SequentialVectorValues.sequential(wrapper), sameInstance(wrapper));
            }
        }
    }

    /** The open of raw vectors read in doc order: as a merge opens them, sequentially and not reused. */
    public static Matcher<Open> sequentialRawVectors() {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Open open) {
                var hints = open.context().hints();
                return open.isRawVectors()
                    && open.context().context() == IOContext.Context.MERGE
                    && hints.contains(DataAccessHint.SEQUENTIAL)
                    && hints.contains(NoReuseHint.INSTANCE);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("raw vectors mapped for a merge, read in order and not reused");
            }
        };
    }

    private void index(Directory dir, boolean compound) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(each.codec(this)).setUseCompoundFile(compound);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < NUM_DOCS; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("field", VectorReadHintsTests.randomVector(), VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    /** {@code actual} holds the vectors of {@code expected}, for the same documents. */
    public static void assertSameVectors(FloatVectorValues expected, FloatVectorValues actual) throws IOException {
        assertThat(actual.size(), equalTo(expected.size()));
        KnnVectorValues.DocIndexIterator expectedIt = expected.iterator();
        KnnVectorValues.DocIndexIterator actualIt = actual.iterator();
        for (int doc = expectedIt.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = expectedIt.nextDoc()) {
            assertThat(actualIt.nextDoc(), equalTo(doc));
            assertArrayEquals(expected.vectorValue(expectedIt.index()), actual.vectorValue(actualIt.index()), 0f);
        }
        assertThat(actualIt.nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
    }

    private static void assertSameScores(VectorScorer expected, VectorScorer actual) throws IOException {
        DocIdSetIterator expectedIt = expected.iterator();
        DocIdSetIterator actualIt = actual.iterator();
        int count = 0;
        for (int doc = expectedIt.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = expectedIt.nextDoc()) {
            assertThat(actualIt.nextDoc(), equalTo(doc));
            assertEquals(expected.score(), actual.score(), 1e-5f);
            count++;
        }
        assertThat(count, equalTo(NUM_DOCS));
        assertThat(actualIt.nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
    }

    /** Once {@link #filesGone}, behaves as if the files were deleted for anything opened as a merge opens it. */
    private static class NoMergeOpensDirectory extends FilterDirectory {
        volatile boolean filesGone;

        NoMergeOpensDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (filesGone && context.context() == IOContext.Context.MERGE) {
                throw new NoSuchFileException(name);
            }
            return super.openInput(name, context);
        }
    }

    /** A wrapper that does not support reading sequentially. */
    private static class Wrapper extends FilterFloatVectorValues {
        Wrapper(FloatVectorValues in) {
            super(in);
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return new Wrapper(in.copy());
        }
    }
}
