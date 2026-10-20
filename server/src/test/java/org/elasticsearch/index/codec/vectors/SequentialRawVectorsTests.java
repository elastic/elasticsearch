/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.store.VectorFieldHint;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/** Which raw vectors {@link SequentialRawVectors} opens again, and how. */
public class SequentialRawVectorsTests extends ESTestCase {

    private static final VectorFieldHint FIELD = new VectorFieldHint("field");

    /** Vectors that searches read in order, or reuse, are already advised for reads in doc order: nothing is opened again. */
    public void testOnlyVectorsReadAtRandomAndNotReusedAreOpenedAgain() throws IOException {
        List<IOContext> opens = new ArrayList<>();
        FlatVectorsFormat format = recording(opens);
        try (Directory dir = newDirectory()) {
            for (IOContext.FileOpenHint[] hints : List.of(
                new IOContext.FileOpenHint[] { FileTypeHint.DATA, FileDataHint.KNN_VECTORS },
                new IOContext.FileOpenHint[] { DataAccessHint.RANDOM, FIELD },
                new IOContext.FileOpenHint[] { NoReuseHint.INSTANCE, FIELD },
                new IOContext.FileOpenHint[] { DataAccessHint.SEQUENTIAL, NoReuseHint.INSTANCE, FIELD }
            )) {
                try (SequentialRawVectors sequential = new SequentialRawVectors(format, state(dir, hints))) {
                    assertNull(sequential.getFloatVectorValues("field"));
                }
            }
        }
        assertThat(opens, empty());
    }

    /**
     * Vectors searches read at random and do not reuse are opened again as a merge opens them, reading them sequentially and
     * keeping what else searches say about them; once their files are gone, they are read through the searches' reader.
     */
    public void testVectorsReadAtRandomAndNotReusedAreOpenedAsAMergeOpensThem() throws IOException {
        List<IOContext> opens = new ArrayList<>();
        try (Directory dir = newDirectory()) {
            SegmentReadState state = state(
                dir,
                FileTypeHint.DATA,
                FileDataHint.KNN_VECTORS,
                DataAccessHint.RANDOM,
                NoReuseHint.INSTANCE,
                FIELD
            );
            try (SequentialRawVectors sequential = new SequentialRawVectors(recording(opens), state)) {
                assertNull("the files are gone", sequential.getFloatVectorValues("field"));
                assertNull(sequential.getFloatVectorValues("field"));
            }
        }
        assertThat("opened once", opens, hasSize(1));
        IOContext context = opens.get(0);
        assertThat(context.context(), equalTo(IOContext.Context.MERGE));
        assertThat(
            context.hints(),
            hasItems(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.SEQUENTIAL, NoReuseHint.INSTANCE, FIELD)
        );
        assertThat(context.hints(), not(hasItem(DataAccessHint.RANDOM)));
    }

    public void testClosed() throws IOException {
        try (Directory dir = newDirectory()) {
            SequentialRawVectors sequential = new SequentialRawVectors(
                recording(new ArrayList<>()),
                state(dir, DataAccessHint.RANDOM, NoReuseHint.INSTANCE, FIELD)
            );
            sequential.close();
            expectThrows(AlreadyClosedException.class, () -> sequential.getFloatVectorValues("field"));
        }
    }

    private static SegmentReadState state(Directory dir, IOContext.FileOpenHint... hints) {
        SegmentInfo info = new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_0",
            1,
            false,
            false,
            Codec.getDefault(),
            Map.of(),
            StringHelper.randomId(),
            Map.of(),
            null
        );
        return new SegmentReadState(dir, info, FieldInfos.EMPTY, IOContext.DEFAULT.withHints(hints));
    }

    /** Records the context each reader is opened with, then fails as if the segment's files were gone. */
    private static FlatVectorsFormat recording(List<IOContext> opens) {
        return new FlatVectorsFormat("recording") {
            @Override
            public FlatVectorsWriter fieldsWriter(SegmentWriteState state) {
                throw new UnsupportedOperationException();
            }

            @Override
            public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
                opens.add(state.context);
                throw new NoSuchFileException("_0.vec");
            }

            @Override
            public int getMaxDimensions(String fieldName) {
                return 4096;
            }
        };
    }
}
