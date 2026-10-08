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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/** A merge of bfloat16 vectors that searches read at random reads them through a mapping of its own. */
public class ES93BFloat16FlatVectorsReaderMergeTests extends ESTestCase {

    private static final int DIMS = 16;
    private static final int DOCS = 32;

    public void testAMergeReadsAMappingOfItsOwn() throws IOException {
        try (Directory base = newDirectory()) {
            TrackingDirectory dir = new TrackingDirectory(base);
            float[][] vectors = writeSegment(base);
            boolean noReuse = randomBoolean();
            IOContext searchContext = noReuse
                ? randomAccess().union(CallerHint.INSTANCE, NoReuseHint.INSTANCE)
                : randomAccess().union(CallerHint.INSTANCE);
            try (FlatVectorsReader reader = openReader(dir, searchContext)) {
                FlatVectorsReader merge = reader.getMergeInstance();
                assertThat(merge, not(sameInstance(reader)));
                assertThat(dir.mergeOpens, hasSize(1));
                IOContext mergeContext = dir.mergeOpens.get(0);
                assertThat(mergeContext.context(), equalTo(IOContext.Context.MERGE));
                assertThat(mergeContext.hints(), hasItem(DataAccessHint.SEQUENTIAL));
                assertThat(mergeContext.hints(), not(hasItem(DataAccessHint.RANDOM)));
                assertThat("keeps what the caller said about the file", mergeContext.hints(), hasItem(CallerHint.INSTANCE));
                assertThat(
                    "keeps what the caller said about reuse",
                    mergeContext.hints(),
                    noReuse ? hasItem(NoReuseHint.INSTANCE) : not(hasItem(NoReuseHint.INSTANCE))
                );
                assertVectors(vectors, merge);
                merge.finishMerge();
                assertThat("closed when the merge finishes", dir.mergeCloses.get(), equalTo(1));
                assertVectors(vectors, reader);

                FlatVectorsReader nextMerge = reader.getMergeInstance();
                try {
                    assertThat("the next merge maps the file again", dir.mergeOpens, hasSize(2));
                    assertVectors(vectors, nextMerge);
                } finally {
                    nextMerge.finishMerge();
                }
                assertThat(dir.mergeCloses.get(), equalTo(2));
            }
        }
    }

    /** A merge takes a merge instance per field, so fields sharing a reader each open the file and close their own. */
    public void testMergeInstancesOfOneMergeEachOpenTheirOwn() throws IOException {
        try (Directory base = newDirectory()) {
            TrackingDirectory dir = new TrackingDirectory(base);
            float[][] vectors = writeSegment(base);
            try (FlatVectorsReader reader = openReader(dir, randomAccess())) {
                FlatVectorsReader first = reader.getMergeInstance();
                FlatVectorsReader second = reader.getMergeInstance();
                assertThat(dir.mergeOpens, hasSize(2));
                first.finishMerge();
                assertThat(dir.mergeCloses.get(), equalTo(1));
                assertVectors(vectors, second);
                second.finishMerge();
                assertThat(dir.mergeCloses.get(), equalTo(2));
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
                    assertThat(context.toString(), mergeInstance, sameInstance(reader));
                    mergeInstance.finishMerge();
                    assertThat(context.toString(), dir.mergeOpens, hasSize(opened));
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
                assertThat(dir.mergeCloses.get(), equalTo(0));
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

    /** Records merge opens of the vectors and counts their closes; can pretend the file is gone. */
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
