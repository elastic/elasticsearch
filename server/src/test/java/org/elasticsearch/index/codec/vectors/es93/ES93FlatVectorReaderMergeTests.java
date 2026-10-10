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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.codec.vectors.FieldKnnVectorsFormat;
import org.elasticsearch.index.store.VectorFieldHint;
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

/** A merge of a flat field streams its raw vectors through an open of its own, when the directory can act on it. */
public class ES93FlatVectorReaderMergeTests extends ESTestCase {

    private static final int DOCS = 32;

    public void testAMergeOpensTheVectorsForItself() throws IOException {
        try (TrackingDirectory dir = new TrackingDirectory(newDirectory())) {
            writeSegment(dir, true, "field");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                KnnVectorsReader vectors = fieldReader(reader, "field");
                KnnVectorsReader merge = vectors.getMergeInstance();
                assertThat(merge, not(sameInstance(vectors)));
                assertThat(dir.mergeOpens, hasSize(1));
                IOContext context = dir.mergeOpens.get(0);
                assertThat(context.hints(), hasItem(DataAccessHint.SEQUENTIAL));
                assertThat(context.hints(), hasItem(new VectorFieldHint("field")));
                assertVectors(vectors, merge, "field");
                merge.finishMerge();
                assertThat(dir.mergeCloses.get(), equalTo(1));
            }
        }
    }

    public void testAMergeReadsTheSearchReaderOnceTheFilesAreGone() throws IOException {
        try (TrackingDirectory dir = new TrackingDirectory(newDirectory())) {
            writeSegment(dir, true, "field");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                KnnVectorsReader vectors = fieldReader(reader, "field");
                dir.gone = true;
                KnnVectorsReader merge = vectors.getMergeInstance();
                assertThat(merge, sameInstance(vectors));
                merge.finishMerge();
                assertThat(fieldReader(reader, "field").getFloatVectorValues("field").size(), equalTo(DOCS));
            }
        }
    }

    public void testFilesHoldingSeveralFieldsAreReadThroughTheSearchReader() throws IOException {
        try (TrackingDirectory dir = new TrackingDirectory(newDirectory())) {
            writeSegment(dir, false, "a", "b");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                KnnVectorsReader vectors = fieldReader(reader, "a");
                assertThat(vectors.getMergeInstance(), sameInstance(vectors));
                assertThat(dir.mergeOpens, hasSize(0));
            }
        }
    }

    /** One segment of flat vectors, with a format per field or one format shared by every field. */
    private static void writeSegment(Directory dir, boolean formatPerField, String... fields) throws IOException {
        KnnVectorsFormat shared = new ES93FlatVectorFormat();
        Codec codec = new FilterCodec(Codec.getDefault().getName(), Codec.getDefault()) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new PerFieldKnnVectorsFormat() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return formatPerField ? new FieldKnnVectorsFormat(field, new ES93FlatVectorFormat()) : shared;
                    }
                };
            }
        };
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false))) {
            for (int i = 0; i < DOCS; i++) {
                Document doc = new Document();
                for (String field : fields) {
                    doc.add(new KnnFloatVectorField(field, new float[] { i, i + 1, i + 2, i + 3 }, VectorSimilarityFunction.EUCLIDEAN));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static KnnVectorsReader fieldReader(DirectoryReader reader, String field) {
        KnnVectorsReader vectors = ((CodecReader) reader.leaves().get(0).reader()).getVectorReader();
        return ((PerFieldKnnVectorsFormat.FieldsReader) vectors).getFieldReader(field);
    }

    private static void assertVectors(KnnVectorsReader expected, KnnVectorsReader actual, String field) throws IOException {
        FloatVectorValues expectedValues = expected.getFloatVectorValues(field);
        FloatVectorValues actualValues = actual.getFloatVectorValues(field);
        assertThat(actualValues.size(), equalTo(expectedValues.size()));
        for (int ord = 0; ord < expectedValues.size(); ord++) {
            assertArrayEquals(expectedValues.vectorValue(ord), actualValues.vectorValue(ord), 0f);
        }
    }

    /** Records the raw vectors opened under a merge context and counts their closes; can pretend the files are gone. */
    private static class TrackingDirectory extends FilterDirectory {
        final List<IOContext> mergeOpens = new ArrayList<>();
        final AtomicInteger mergeCloses = new AtomicInteger();
        volatile boolean gone;

        TrackingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (context.context() != IOContext.Context.MERGE) {
                return super.openInput(name, context);
            }
            if (gone) {
                throw new NoSuchFileException(name);
            }
            if (name.endsWith(".vec") == false) {
                return super.openInput(name, context);
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
