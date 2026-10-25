/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NoReuseHint;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/** bfloat16 vectors that searches read at random without reuse are opened again once for merges and sequential reads. */
public class ES93BFloat16FlatVectorsReaderMergeTests extends ESTestCase {

    // larger than a page, which mmap can advise
    private static final int DIMS = 256;
    private static final int DOCS = 256;

    @Before
    public void assumeAdvice() {
        assumeTrue("the vectors are opened again only when their mapping is advised", MMapDirectory.supportsMadvise());
    }

    public void testMergesAndSequentialReadsOpenTheVectorsOnce() throws IOException {
        boolean mergeFirst = randomBoolean();
        try (Directory base = shardMmap()) {
            TrackingDirectory dir = new TrackingDirectory(base);
            float[][] vectors = writeSegment(base);
            try (FlatVectorsReader reader = openReader(dir, randomAccess().union(CallerHint.INSTANCE, NoReuseHint.INSTANCE))) {
                dir.tracking = true;
                FlatVectorsReader merge = mergeFirst ? reader.getMergeInstance() : null;
                assertVectors(vectors, reader.getFloatVectorValues("field", DataAccessHint.SEQUENTIAL));
                if (merge == null) {
                    merge = reader.getMergeInstance();
                }
                assertThat(merge, not(sameInstance(reader)));
                assertVectors(vectors, merge.getFloatVectorValues("field"));
                reader.getMergeInstance();

                assertThat(dir.opens, hasSize(1));
                IOContext context = dir.opens.get(0);
                assertThat(context.context(), equalTo(mergeFirst ? IOContext.Context.MERGE : IOContext.Context.DEFAULT));
                assertThat(context.hints(), hasItem(DataAccessHint.SEQUENTIAL));
                assertThat(context.hints(), not(hasItem(DataAccessHint.RANDOM)));
                assertThat(context.hints(), hasItem(NoReuseHint.INSTANCE));
                assertThat(context.hints(), hasItem(CallerHint.INSTANCE));
            }
            assertThat("closed with the reader", dir.closes.get(), equalTo(1));
        }
    }

    public void testNothingOpenedWhenTheSearchInputServes() throws IOException {
        try (Directory base = shardMmap()) {
            TrackingDirectory dir = new TrackingDirectory(base);
            writeSegment(base);
            for (IOContext context : List.of(IOContext.DEFAULT, randomAccess(), IOContext.merge().withHints(DataAccessHint.SEQUENTIAL))) {
                dir.tracking = false;
                try (FlatVectorsReader reader = openReader(dir, context)) {
                    dir.tracking = true;
                    assertThat(context.toString(), reader.getMergeInstance(), sameInstance(reader));
                    reader.getFloatVectorValues("field", DataAccessHint.SEQUENTIAL);
                    assertThat(context.toString(), dir.opens, hasSize(0));
                }
            }
        }
    }

    public void testFallsBackToTheSearchInputWhenTheFileIsGone() throws IOException {
        try (Directory base = shardMmap()) {
            TrackingDirectory dir = new TrackingDirectory(base);
            float[][] vectors = writeSegment(base);
            try (FlatVectorsReader reader = openReader(dir, randomAccess().union(NoReuseHint.INSTANCE))) {
                dir.tracking = true;
                dir.fileGone = true;
                assertVectors(vectors, reader.getMergeInstance().getFloatVectorValues("field"));
                assertVectors(vectors, reader.getFloatVectorValues("field", DataAccessHint.SEQUENTIAL));
                assertVectors(vectors, reader.getFloatVectorValues("field"));
            }
            assertThat(dir.closes.get(), equalTo(0));
        }
    }

    private static Directory shardMmap() throws IOException {
        MMapDirectory mmap = new MMapDirectory(createTempDir());
        mmap.setReadAdvice(FsDirectoryFactory.getReadAdviceFunc());
        return mmap;
    }

    private static IOContext randomAccess() {
        return IOContext.DEFAULT.withHints(DataAccessHint.RANDOM);
    }

    private static float[][] writeSegment(Directory dir) throws IOException {
        float[][] vectors = new float[DOCS][];
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(new BFloat16Codec()).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < DOCS; i++) {
                vectors[i] = new float[DIMS];
                for (int d = 0; d < DIMS; d++) {
                    // small integers are exact in bfloat16
                    vectors[i][d] = randomIntBetween(-8, 8);
                }
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("field", vectors[i], VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        return vectors;
    }

    private static FlatVectorsReader openReader(Directory dir, IOContext context) throws IOException {
        SegmentInfo info = SegmentInfos.readLatestCommit(dir).info(0).info;
        FieldInfos fieldInfos = info.getCodec().fieldInfosFormat().read(dir, info, "", IOContext.DEFAULT);
        return new ES93BFloat16FlatVectorsReader(
            new SegmentReadState(dir, info, fieldInfos, context),
            ES93GenericFlatVectorScorer.INSTANCE
        );
    }

    private static void assertVectors(float[][] expected, FloatVectorValues values) throws IOException {
        assertThat(values.size(), equalTo(expected.length));
        for (int ord = 0; ord < expected.length; ord++) {
            assertArrayEquals(expected[values.ordToDoc(ord)], values.vectorValue(ord), 0f);
        }
    }

    /** A hint the caller adds, which a merge mapping keeps. */
    private enum CallerHint implements IOContext.FileOpenHint {
        INSTANCE
    }

    /** Writes vectors with the bfloat16 format, so a reader can be opened over the segment directly. */
    private static class BFloat16Codec extends FilterCodec {
        BFloat16Codec() {
            super(Codec.getDefault().getName(), Codec.getDefault());
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return new ES93BFloat16FlatVectorsFormat(ES93GenericFlatVectorScorer.INSTANCE);
        }
    }

    /** Once tracking, records the opens of the vectors and counts their closes; can pretend the file is gone. */
    private static class TrackingDirectory extends FilterDirectory {
        final List<IOContext> opens = new ArrayList<>();
        final AtomicInteger closes = new AtomicInteger();
        volatile boolean tracking;
        volatile boolean fileGone;

        TrackingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (tracking == false || name.endsWith("." + ES93BFloat16FlatVectorsFormat.VECTOR_DATA_EXTENSION) == false) {
                return super.openInput(name, context);
            }
            if (fileGone) {
                throw new NoSuchFileException(name);
            }
            opens.add(context);
            return new FilterIndexInput(name, super.openInput(name, context)) {
                @Override
                public void close() throws IOException {
                    closes.incrementAndGet();
                    super.close();
                }

                @Override
                public IndexInput clone() {
                    return in.clone();
                }

                @Override
                public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                    return in.slice(sliceDescription, offset, length);
                }
            };
        }
    }
}
