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
import org.apache.lucene.store.NoReuseHint;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * How bfloat16 vectors are read by merges while searches read them at random: through one mapping of their own, opened by
 * the first merge, shared by the merges that follow, and closed once the last one is done or with the reader.
 */
public class ES93BFloat16FlatVectorsReaderMergeTests extends ESTestCase {

    private static final int DIMS = 16;
    private static final int DOCS = 32;

    public void testMergesShareOneMappingOfTheirOwn() throws IOException {
        try (Directory base = newDirectory()) {
            TrackingDirectory dir = new TrackingDirectory(base);
            float[][] vectors = writeSegment(base);
            boolean noReuse = randomBoolean();
            IOContext searchContext = noReuse
                ? randomAccess().union(CallerHint.INSTANCE, NoReuseHint.INSTANCE)
                : randomAccess().union(CallerHint.INSTANCE);
            try (FlatVectorsReader reader = openReader(dir, searchContext)) {
                FlatVectorsReader first = reader.getMergeInstance();
                FlatVectorsReader second = reader.getMergeInstance();
                assertNotSame(reader, first);
                assertNotSame(first, second);
                assertEquals("merges share one mapping", 1, dir.mergeOpens.size());
                IOContext mergeContext = dir.mergeOpens.get(0);
                assertSame(IOContext.Context.MERGE, mergeContext.context());
                assertTrue(mergeContext.hints().contains(DataAccessHint.SEQUENTIAL));
                assertFalse(mergeContext.hints().contains(DataAccessHint.RANDOM));
                assertTrue("keeps what the caller said about the file", mergeContext.hints().contains(CallerHint.INSTANCE));
                assertEquals("keeps what the caller said about reuse", noReuse, mergeContext.hints().contains(NoReuseHint.INSTANCE));
                assertVectors(vectors, first);
                assertVectors(vectors, second);

                first.finishMerge();
                first.finishMerge();
                reader.finishMerge();
                assertEquals("a merge gives the mapping back once, and the reader holds none", 0, dir.mergeCloses.get());
                assertVectors(vectors, second);

                second.finishMerge();
                assertEquals("closed after the last merge", 1, dir.mergeCloses.get());

                FlatVectorsReader third = reader.getMergeInstance();
                assertEquals("a later merge maps the file again", 2, dir.mergeOpens.size());
                assertVectors(vectors, third);
                reader.close();
                assertEquals("closing the reader closes the mapping", 2, dir.mergeCloses.get());
                third.finishMerge();
                assertEquals("and a merge finishing later does not close it again", 2, dir.mergeCloses.get());
            }
        }
    }

    public void testNoMappingOfTheirOwnUnlessSearchesReadAtRandom() throws IOException {
        try (Directory base = newDirectory()) {
            TrackingDirectory dir = new TrackingDirectory(base);
            writeSegment(base);
            for (IOContext context : List.of(
                IOContext.DEFAULT,
                IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL),
                IOContext.merge().withHints(DataAccessHint.RANDOM)
            )) {
                try (FlatVectorsReader reader = openReader(dir, context)) {
                    int opened = dir.mergeOpens.size();
                    FlatVectorsReader mergeInstance = reader.getMergeInstance();
                    assertSame(context.toString(), reader, mergeInstance);
                    mergeInstance.finishMerge();
                    assertEquals(context.toString(), opened, dir.mergeOpens.size());
                }
            }
        }
    }

    public void testFallsBackToTheSearchMappingWhenTheFileIsGone() throws IOException {
        try (Directory base = newDirectory()) {
            TrackingDirectory dir = new TrackingDirectory(base);
            float[][] vectors = writeSegment(base);
            try (FlatVectorsReader reader = openReader(dir, randomAccess())) {
                dir.fileGone = true;
                FlatVectorsReader mergeInstance = reader.getMergeInstance();
                assertVectors(vectors, mergeInstance);
                mergeInstance.finishMerge();
                assertEquals(0, dir.mergeCloses.get());
                // the search mapping is still open
                assertVectors(vectors, reader);
            }
        }
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

    private static void assertVectors(float[][] expected, FlatVectorsReader reader) throws IOException {
        FloatVectorValues values = reader.getFloatVectorValues("field");
        assertEquals(expected.length, values.size());
        for (int ord = 0; ord < expected.length; ord++) {
            assertArrayEquals(expected[values.ordToDoc(ord)], values.vectorValue(ord), 0f);
        }
    }

    /** A hint the caller adds, which a merge mapping keeps. */
    private enum CallerHint implements IOContext.FileOpenHint {
        INSTANCE
    }

    /** Writes vectors with the bfloat16 format directly, so that a reader can be opened over the segment it wrote. */
    private static class BFloat16Codec extends FilterCodec {
        BFloat16Codec() {
            super(Codec.getDefault().getName(), Codec.getDefault());
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return new ES93BFloat16FlatVectorsFormat(ES93GenericFlatVectorScorer.INSTANCE);
        }
    }

    /** Records the vectors opened under a merge context and counts their closes; can pretend the file is gone. */
    private static class TrackingDirectory extends FilterDirectory {
        final List<IOContext> mergeOpens = new ArrayList<>();
        final AtomicInteger mergeCloses = new AtomicInteger();
        volatile boolean fileGone;

        TrackingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            boolean mergeVectors = name.endsWith("." + ES93BFloat16FlatVectorsFormat.VECTOR_DATA_EXTENSION)
                && context.context() == IOContext.Context.MERGE;
            if (mergeVectors == false) {
                return super.openInput(name, context);
            }
            if (fileGone) {
                throw new NoSuchFileException(name);
            }
            mergeOpens.add(context);
            return new FilterIndexInput(name, super.openInput(name, context)) {
                @Override
                public void close() throws IOException {
                    mergeCloses.incrementAndGet();
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
