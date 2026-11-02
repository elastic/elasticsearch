/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil;
import org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.Written;
import org.elasticsearch.index.codec.tsdb.OrdinalReadContext;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalReader;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.MISSING;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.randomBlockSize;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.readContext;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.sortedValuesSource;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests the layout choice of {@link RunTableSortedCodec}: run-shaped dimension fields are written as a run
 * table that reads back to the original ordinals, and every other field is written by the fallback codec
 * byte for byte, behind the discriminator.
 */
public class RunTableSortedCodecTests extends ESTestCase {

    private static final FieldContextResolver DIMENSIONS = (name, blockSize) -> new FieldContext(blockSize, name, null, null, true);
    private static final FieldContextResolver NO_DIMENSIONS = (name, blockSize) -> new FieldContext(blockSize, name, null, null, false);
    private static final int NO_PRIMARY_SORT_FIELD = -1;

    private static final SortedOrdinalCodec FALLBACK = ES95TSDBDocValuesFormat.SORTED_CODEC;

    public void testRunShapedDimensionIsWrittenAsRunTable() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(16, 5000);
        final long maxOrd = randomLongBetween(2, maxDoc / 8);
        final long[] ordinals = runShapedOrdinals(maxDoc, maxOrd, randomBoolean());

        final Written written = write(new RunTableSortedCodec(FALLBACK, DIMENSIONS), blockSize, ordinals, maxOrd, NO_PRIMARY_SORT_FIELD);

        assertThat(written.meta()[0], equalTo(RunTableSortedCodec.LAYOUT_RUN_TABLE));
        final int numDocsWithField = countDocsWithValue(ordinals);
        assertThat(written.stats().numDocsWithField(), equalTo(numDocsWithField));
        assertThat(written.stats().numValues(), equalTo((long) numDocsWithField));

        final NumericEntry entry = new NumericEntry();
        final SortedOrdinalReader reader = readOrdinalMeta(new RunTableSortedCodec(FALLBACK, null), written, entry, blockSize);
        assertThat(reader, instanceOf(RunTableSortedCodec.RunTableReader.class));
        assertThat(entry.numDocsWithField, equalTo(numDocsWithField));
        assertThat(entry.numValues, equalTo((long) numDocsWithField));
        assertThat(entry.blockSize, equalTo(blockSize));
        assertThat("must not look like an empty field", entry.docsWithFieldOffset, equalTo(-1L));

        // the reader is shared by every opening of the field, so two open doc values must not interfere
        try (IndexInput data = new ByteArrayIndexInput("test-data", written.data())) {
            final OrdinalReadContext ctx = new OrdinalReadContext(data, maxDoc, null);
            final NumericDocValues first = reader.ordinals(ctx, maxOrd);
            final NumericDocValues second = reader.ordinals(ctx, maxOrd);
            int expected = nextDocWithValue(ordinals, 0);
            while (expected != NO_MORE_DOCS) {
                assertThat(first.nextDoc(), equalTo(expected));
                assertThat(first.longValue(), equalTo(ordinals[expected]));
                assertThat(second.advanceExact(expected), equalTo(true));
                assertThat(second.longValue(), equalTo(ordinals[expected]));
                expected = nextDocWithValue(ordinals, expected + 1);
            }
            assertThat(first.nextDoc(), equalTo(NO_MORE_DOCS));
        }
    }

    public void testRoutingHashIsWrittenAsRunTableWithoutBeingADimension() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(16, 2000);
        final long maxOrd = randomLongBetween(2, maxDoc / 8);
        final long[] ordinals = runShapedOrdinals(maxDoc, maxOrd, false);
        final FieldInfo field = OrdinalCodecTestUtil.fieldInfo(TimeSeriesRoutingHashFieldMapper.NAME, DocValuesType.SORTED);

        final Written written = OrdinalCodecTestUtil.write(
            blockSize,
            maxDoc,
            ctx -> new RunTableSortedCodec(FALLBACK, NO_DIMENSIONS).createWriter(ctx)
                .writeOrdinals(field, sortedValuesSource(ordinals), maxOrd, SortedFieldObserver.NOOP)
        );
        assertThat(written.meta()[0], equalTo(RunTableSortedCodec.LAYOUT_RUN_TABLE));
    }

    public void testNonDimensionFallsBack() throws IOException {
        final int maxDoc = randomIntBetween(16, 2000);
        final long maxOrd = randomLongBetween(2, maxDoc / 8);
        assertFallsBack(NO_DIMENSIONS, runShapedOrdinals(maxDoc, maxOrd, randomBoolean()), maxOrd, NO_PRIMARY_SORT_FIELD);
    }

    public void testNoResolverFallsBack() throws IOException {
        final int maxDoc = randomIntBetween(16, 2000);
        final long maxOrd = randomLongBetween(2, maxDoc / 8);
        assertFallsBack(null, runShapedOrdinals(maxDoc, maxOrd, randomBoolean()), maxOrd, NO_PRIMARY_SORT_FIELD);
    }

    public void testPrimarySortFieldFallsBack() throws IOException {
        final int maxDoc = randomIntBetween(16, 2000);
        final long maxOrd = randomLongBetween(2, maxDoc / 8);
        // the test field is field number 0
        assertFallsBack(DIMENSIONS, runShapedOrdinals(maxDoc, maxOrd, false), maxOrd, 0);
    }

    public void testTooManyValuesFallsBack() throws IOException {
        final int maxDoc = randomIntBetween(16, 2000);
        final long maxOrd = randomLongBetween(maxDoc / 2 + 1, maxDoc);
        final long[] ordinals = new long[maxDoc];
        for (int doc = 0; doc < maxDoc; doc++) {
            ordinals[doc] = doc % maxOrd;
        }
        assertFallsBack(DIMENSIONS, ordinals, maxOrd, NO_PRIMARY_SORT_FIELD);
    }

    public void testTooManyRunsFallsBack() throws IOException {
        // few values, but they change on almost every doc, so the gate gives up during the walk
        final int maxDoc = randomIntBetween(16, 2000);
        final long[] ordinals = new long[maxDoc];
        for (int doc = 0; doc < maxDoc; doc++) {
            ordinals[doc] = doc % 2;
        }
        assertFallsBack(DIMENSIONS, ordinals, 2, NO_PRIMARY_SORT_FIELD);
    }

    public void testSingleValueFallsBack() throws IOException {
        final int maxDoc = randomIntBetween(16, 2000);
        final long[] ordinals = new long[maxDoc];
        for (int doc = 0; doc < maxDoc; doc++) {
            ordinals[doc] = randomBoolean() ? 0 : MISSING;
        }
        assertFallsBack(DIMENSIONS, ordinals, 1, NO_PRIMARY_SORT_FIELD);
    }

    public void testUnknownLayoutIsCorruption() throws IOException {
        final byte layout = randomValueOtherThanMany(
            b -> b == RunTableSortedCodec.LAYOUT_DEFAULT || b == RunTableSortedCodec.LAYOUT_RUN_TABLE,
            ESTestCase::randomByte
        );
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", new byte[] { layout })) {
            expectThrows(
                CorruptIndexException.class,
                () -> new RunTableSortedCodec(FALLBACK, null).readOrdinalMeta(readContext(128), meta, new NumericEntry(), 7)
            );
        }
    }

    /**
     * Writes {@code ordinals} with the run-table codec and with the fallback codec alone, and checks that the
     * run-table codec chose the fallback: its output is the fallback's behind the discriminator, and reads back
     * into the same entry.
     */
    private static void assertFallsBack(final FieldContextResolver resolver, final long[] ordinals, long maxOrd, int primarySortFieldNumber)
        throws IOException {
        final int blockSize = randomBlockSize();
        final Written viaRunTableCodec = write(
            new RunTableSortedCodec(FALLBACK, resolver),
            blockSize,
            ordinals,
            maxOrd,
            primarySortFieldNumber
        );
        final Written viaFallback = write(FALLBACK, blockSize, ordinals, maxOrd, primarySortFieldNumber);

        assertThat(viaRunTableCodec.meta()[0], equalTo(RunTableSortedCodec.LAYOUT_DEFAULT));
        assertThat(Arrays.copyOfRange(viaRunTableCodec.meta(), 1, viaRunTableCodec.meta().length), equalTo(viaFallback.meta()));
        assertThat(viaRunTableCodec.data(), equalTo(viaFallback.data()));
        assertThat(viaRunTableCodec.stats(), equalTo(viaFallback.stats()));

        final NumericEntry entry = new NumericEntry();
        readOrdinalMeta(new RunTableSortedCodec(FALLBACK, null), viaRunTableCodec, entry, blockSize);
        final NumericEntry expected = new NumericEntry();
        readOrdinalMeta(FALLBACK, viaFallback, expected, blockSize);
        assertThat(entry.numDocsWithField, equalTo(expected.numDocsWithField));
        assertThat(entry.numValues, equalTo(expected.numValues));
        assertThat(entry.docsWithFieldOffset, equalTo(expected.docsWithFieldOffset));
        assertThat(entry.valuesOffset, equalTo(expected.valuesOffset));
    }

    private static Written write(
        final SortedOrdinalCodec codec,
        int blockSize,
        final long[] ordinals,
        long maxOrd,
        int primarySortFieldNumber
    ) throws IOException {
        return OrdinalCodecTestUtil.write(
            blockSize,
            ordinals.length,
            primarySortFieldNumber,
            ctx -> codec.createWriter(ctx)
                .writeOrdinals(
                    OrdinalCodecTestUtil.fieldInfo(DocValuesType.SORTED),
                    sortedValuesSource(ordinals),
                    maxOrd,
                    SortedFieldObserver.NOOP
                )
        );
    }

    /** Parses {@code written}'s metadata into {@code entry}, checking it is consumed exactly. */
    private static SortedOrdinalReader readOrdinalMeta(
        final SortedOrdinalCodec codec,
        final Written written,
        final NumericEntry entry,
        int blockSize
    ) throws IOException {
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", written.meta())) {
            final SortedOrdinalReader reader = codec.readOrdinalMeta(
                readContext(blockSize),
                meta,
                entry,
                Integer.numberOfTrailingZeros(blockSize)
            );
            assertThat("metadata stream not fully consumed", meta.getFilePointer(), equalTo(meta.length()));
            return reader;
        }
    }

    /**
     * Random runs of at least four docs, so that the run table passes the gate on every prefix of the walk;
     * {@code sparse} adds runs of docs without a value.
     */
    private static long[] runShapedOrdinals(int maxDoc, long maxOrd, boolean sparse) {
        final long[] ordinals = new long[maxDoc];
        int doc = 0;
        while (doc < maxDoc) {
            final int remaining = maxDoc - doc;
            final int runLength = remaining < 8 ? remaining : randomIntBetween(4, Math.min(remaining - 4, 200));
            final long ord = sparse && randomBoolean() ? MISSING : randomLongBetween(0, maxOrd - 1);
            Arrays.fill(ordinals, doc, doc + runLength, ord);
            doc += runLength;
        }
        return ordinals;
    }

    private static int countDocsWithValue(final long[] ordinals) {
        return (int) Arrays.stream(ordinals).filter(ord -> ord != MISSING).count();
    }

    private static int nextDocWithValue(final long[] ordinals, int from) {
        for (int doc = from; doc < ordinals.length; doc++) {
            if (ordinals[doc] != MISSING) {
                return doc;
            }
        }
        return NO_MORE_DOCS;
    }
}
