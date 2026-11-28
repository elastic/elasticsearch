/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.codec.bwc.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * At flush the consumer reads a single-valued numeric field once into arrays ({@link MaterializedNumericValues}) and runs its
 * stats, skip index, block and DISI passes over them; in any other context it walks the doc values iterator for each pass.
 * The two ways of reading must write the same bytes, so these tests write the same field in a flush context and in a merge
 * context and compare every file.
 */
public class FlushMaterializedNumericValuesTests extends ESTestCase {

    private static final String FIELD = "field";

    public void testFlushWritesSameBytesAsIteratorWalk() throws IOException {
        final int iterations = atLeast(20);
        for (int i = 0; i < iterations; i++) {
            final int maxDoc = randomMaxDoc();
            final int[] docs = randomDocs(maxDoc);
            final long[] values = randomValues(docs.length);
            final DocValuesType type = randomBoolean() ? DocValuesType.NUMERIC : DocValuesType.SORTED_NUMERIC;
            final boolean skipIndex = randomBoolean();
            final DocValuesFormat format = randomFormat();
            final byte[] segmentId = StringHelper.randomId();

            try (Directory flushDir = new ByteBuffersDirectory(); Directory mergeDir = new ByteBuffersDirectory()) {
                final CountingProducer flushProducer = write(
                    flushDir,
                    format,
                    IOContext.flush(new FlushInfo(maxDoc, 0L)),
                    segmentId,
                    maxDoc,
                    type,
                    skipIndex,
                    docs,
                    values
                );
                final CountingProducer mergeProducer = write(
                    mergeDir,
                    format,
                    IOContext.merge(new MergeInfo(maxDoc, 0L, false, 1)),
                    segmentId,
                    maxDoc,
                    type,
                    skipIndex,
                    docs,
                    values
                );

                assertSameFiles(flushDir, mergeDir);
                assertThat("flush reads the field once", flushProducer.iteratorsCreated, equalTo(1));
                if (docs.length > 0) {
                    assertThat("merge walks the field once per pass", mergeProducer.iteratorsCreated, greaterThan(1));
                }
            }
        }
    }

    public void testLoadDenseField() throws IOException {
        final int maxDoc = randomIntBetween(1, 3000);
        final long[] values = randomValues(maxDoc);
        final MaterializedNumericValues materialized = MaterializedNumericValues.load(iterator(identity(maxDoc), values), maxDoc);
        assertThat(materialized.size(), equalTo(maxDoc));
        for (int i = 0; i < maxDoc; i++) {
            assertThat(materialized.doc(i), equalTo(i));
            assertThat(materialized.values()[i], equalTo(values[i]));
        }
        assertDocIds(materialized, identity(maxDoc));
    }

    public void testLoadSparseField() throws IOException {
        final int maxDoc = randomIntBetween(2, 3000);
        final int[] docs = randomDocs(maxDoc);
        final long[] values = randomValues(docs.length);
        final MaterializedNumericValues materialized = MaterializedNumericValues.load(iterator(docs, values), maxDoc);
        assertThat(materialized.size(), equalTo(docs.length));
        for (int i = 0; i < docs.length; i++) {
            assertThat(materialized.doc(i), equalTo(docs[i]));
            assertThat(materialized.values()[i], equalTo(values[i]));
        }
        assertDocIds(materialized, docs);
    }

    /** A {@code cost()} that underestimates must only cost a regrow, and a field that turns sparse late must keep its early doc ids. */
    public void testLoadWhenCostUnderestimates() throws IOException {
        final int count = randomIntBetween(100, 2000);
        final int[] docs = new int[count];
        final long[] values = new long[count];
        final int firstGap = randomIntBetween(1, count - 1);
        for (int i = 0, doc = 0; i < count; i++, doc++) {
            if (i == firstGap) {
                doc += randomIntBetween(1, 5);
            }
            docs[i] = doc;
            values[i] = randomLong();
        }
        final int maxDoc = docs[count - 1] + 1;
        final NumericDocValues in = new ArrayNumericDocValues(docs, values) {
            @Override
            public long cost() {
                return 1;
            }
        };
        final MaterializedNumericValues materialized = MaterializedNumericValues.load(in, maxDoc);
        assertThat(materialized.size(), equalTo(count));
        for (int i = 0; i < count; i++) {
            assertThat(materialized.doc(i), equalTo(docs[i]));
            assertThat(materialized.values()[i], equalTo(values[i]));
        }
    }

    public void testLoadEmptyField() throws IOException {
        final MaterializedNumericValues materialized = MaterializedNumericValues.load(iterator(new int[0], new long[0]), 10);
        assertThat(materialized.size(), equalTo(0));
        assertThat(materialized.docIds().nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
    }

    private static void assertDocIds(final MaterializedNumericValues materialized, final int[] expectedDocs) throws IOException {
        final DocIdSetIterator it = materialized.docIds();
        assertThat(it.docID(), equalTo(-1));
        for (int expected : expectedDocs) {
            assertThat(it.nextDoc(), equalTo(expected));
        }
        assertThat(it.nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
    }

    /** Writes the field through {@code format}'s consumer in {@code ioContext} and returns the producer it read from. */
    private static CountingProducer write(
        final Directory dir,
        final DocValuesFormat format,
        final IOContext ioContext,
        final byte[] segmentId,
        final int maxDoc,
        final DocValuesType type,
        final boolean skipIndex,
        final int[] docs,
        final long[] values
    ) throws IOException {
        final FieldInfo field = new FieldInfo(
            FIELD,
            0,
            false,
            false,
            false,
            IndexOptions.NONE,
            type,
            skipIndex ? DocValuesSkipIndexType.RANGE : DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
        final SegmentInfo segmentInfo = new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_0",
            maxDoc,
            false,
            false,
            new Elasticsearch93Lucene104Codec(),
            Collections.emptyMap(),
            segmentId,
            new HashMap<>(),
            null
        );
        final SegmentWriteState state = new SegmentWriteState(
            InfoStream.getDefault(),
            dir,
            segmentInfo,
            new FieldInfos(new FieldInfo[] { field }),
            null,
            ioContext
        );
        final CountingProducer producer = new CountingProducer(docs, values);
        try (DocValuesConsumer consumer = format.fieldsConsumer(state)) {
            if (type == DocValuesType.NUMERIC) {
                consumer.addNumericField(field, producer);
            } else {
                consumer.addSortedNumericField(field, producer);
            }
        }
        return producer;
    }

    private static void assertSameFiles(final Directory expectedDir, final Directory actualDir) throws IOException {
        final String[] expectedFiles = expectedDir.listAll();
        final String[] actualFiles = actualDir.listAll();
        Arrays.sort(expectedFiles);
        Arrays.sort(actualFiles);
        assertArrayEquals(expectedFiles, actualFiles);
        assertThat("the consumer wrote data, meta and skip files", expectedFiles.length, greaterThan(2));
        for (String file : expectedFiles) {
            assertArrayEquals("file [" + file + "] differs", read(expectedDir, file), read(actualDir, file));
        }
    }

    private static byte[] read(final Directory dir, final String file) throws IOException {
        try (IndexInput in = dir.openInput(file, IOContext.READONCE)) {
            final byte[] bytes = new byte[(int) in.length()];
            in.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }

    private static DocValuesFormat randomFormat() {
        return randomBoolean()
            ? new ES819TSDBDocValuesFormat()
            : ES95TSDBDocValuesFormatFactory.create(randomBoolean(), false, false, (f, bs) -> new FieldContext(bs, f, null, null));
    }

    private static int randomMaxDoc() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> randomIntBetween(1, 300);
            case 1 -> randomIntBetween(300, 5000);
            case 2 -> randomIntBetween(5000, 40_000);
            default -> throw new AssertionError();
        };
    }

    /** Dense, sparse at a random density, or empty. */
    private static int[] randomDocs(final int maxDoc) {
        final double density = switch (randomIntBetween(0, 4)) {
            case 0 -> 1.0;
            case 1 -> 0.999;
            case 2 -> randomDoubleBetween(0.01, 0.99, true);
            case 3 -> 0.0005;
            case 4 -> 0.0;
            default -> throw new AssertionError();
        };
        final int[] docs = new int[maxDoc];
        int n = 0;
        for (int doc = 0; doc < maxDoc; doc++) {
            if (density >= 1.0 || random().nextDouble() < density) {
                docs[n++] = doc;
            }
        }
        return Arrays.copyOf(docs, n);
    }

    private static long[] randomValues(final int count) {
        final long[] values = new long[count];
        switch (randomIntBetween(0, 4)) {
            case 0 -> Arrays.fill(values, randomLong());
            case 1 -> {
                long v = randomLongBetween(-1000, 1000);
                for (int i = 0; i < count; i++) {
                    values[i] = v;
                    v += randomLongBetween(0, 5);
                }
            }
            case 2 -> {
                for (int i = 0; i < count; i++) {
                    values[i] = randomLongBetween(0, 99);
                }
            }
            case 3 -> {
                for (int i = 0; i < count; i++) {
                    values[i] = randomLong();
                }
            }
            case 4 -> {
                // Runs of one value, so that skip index intervals can close on a value change or run on while the value stays constant.
                for (int i = 0; i < count;) {
                    final long v = randomLongBetween(-50, 50);
                    for (int end = Math.min(count, i + randomIntBetween(1, 9000)); i < end; i++) {
                        values[i] = v;
                    }
                }
            }
            default -> throw new AssertionError();
        }
        return values;
    }

    private static int[] identity(final int n) {
        final int[] docs = new int[n];
        for (int i = 0; i < n; i++) {
            docs[i] = i;
        }
        return docs;
    }

    private static NumericDocValues iterator(final int[] docs, final long[] values) {
        return new ArrayNumericDocValues(docs, values);
    }

    private static class ArrayNumericDocValues extends NumericDocValues {
        private final int[] docs;
        private final long[] values;
        private int index = -1;

        ArrayNumericDocValues(final int[] docs, final long[] values) {
            this.docs = docs;
            this.values = values;
        }

        @Override
        public long longValue() {
            return values[index];
        }

        @Override
        public boolean advanceExact(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID() {
            return index < 0 ? -1 : index < docs.length ? docs[index] : NO_MORE_DOCS;
        }

        @Override
        public int nextDoc() {
            index++;
            return docID();
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return docs.length;
        }
    }

    /** Hands out a fresh iterator on every call and counts how many it handed out. */
    private static final class CountingProducer extends EmptyDocValuesProducer {
        private final int[] docs;
        private final long[] values;
        int iteratorsCreated;

        CountingProducer(final int[] docs, final long[] values) {
            this.docs = docs;
            this.values = values;
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) {
            iteratorsCreated++;
            return iterator(docs, values);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
            iteratorsCreated++;
            return DocValues.singleton(iterator(docs, values));
        }
    }
}
