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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;
import org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil;
import org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.Written;
import org.elasticsearch.index.codec.tsdb.OrdinalReadContext;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalReader;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.LongStream;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.randomBlockSize;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.readContext;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.sortedSetValuesSource;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the layout choice of {@link RunTableSortedSetCodec}: run-shaped multi-valued dimension fields are
 * written as a run table, without an addresses table, that reads back to the original ordinal sets; every
 * other field is written by the fallback codec byte for byte, behind the discriminator.
 */
public class RunTableSortedSetCodecTests extends ESTestCase {

    private static final FieldContextResolver DIMENSIONS = (name, blockSize) -> new FieldContext(blockSize, name, null, null, true);
    private static final FieldContextResolver NO_DIMENSIONS = (name, blockSize) -> new FieldContext(blockSize, name, null, null, false);
    private static final int NO_PRIMARY_SORT_FIELD = -1;
    private static final long[] EMPTY = new long[0];

    private static final SortedSetOrdinalCodec FALLBACK = ES95TSDBDocValuesFormat.SORTED_SET_CODEC;

    public void testRunShapedDimensionIsWrittenAsRunTable() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(16, 5000);
        final long maxOrd = randomLongBetween(2, maxDoc / 8);
        final long[][] ordinals = runShapedOrdinals(maxDoc, maxOrd, randomBoolean());

        final Written written;
        try (Directory dir = newDirectory()) {
            written = write(dir, new RunTableSortedSetCodec(FALLBACK, DIMENSIONS), blockSize, ordinals, maxOrd, NO_PRIMARY_SORT_FIELD);
        }

        assertThat(written.meta()[0], equalTo(RunTableLayout.LAYOUT_RUN_TABLE));
        final int numDocsWithField = countDocsWithValue(ordinals);
        final long numValues = countValues(ordinals);
        assertThat(written.stats().numDocsWithField(), equalTo(numDocsWithField));
        assertThat(written.stats().numValues(), equalTo(numValues));

        final SortedNumericEntry entry = new SortedNumericEntry();
        final SortedSetOrdinalReader reader = readOrdinalMeta(new RunTableSortedSetCodec(FALLBACK, null), written, entry, blockSize);
        assertThat(reader, instanceOf(RunTableSortedSetCodec.RunTableReader.class));
        assertThat(entry.numDocsWithField, equalTo(numDocsWithField));
        assertThat(entry.numValues, equalTo(numValues));
        assertThat(entry.blockSize, equalTo(blockSize));
        assertThat("no addresses table", entry.addressesMeta, nullValue());

        // the reader is shared by every opening of the field, so two open doc values must not interfere
        try (IndexInput data = new ByteArrayIndexInput("test-data", written.data())) {
            final OrdinalReadContext ctx = new OrdinalReadContext(data, maxDoc, null);
            final SortedNumericDocValues first = reader.ordinals(ctx, maxOrd);
            final SortedNumericDocValues second = reader.ordinals(ctx, maxOrd);
            int expected = nextDocWithValue(ordinals, 0);
            while (expected != NO_MORE_DOCS) {
                assertThat(first.nextDoc(), equalTo(expected));
                assertThat(second.advanceExact(expected), equalTo(true));
                assertSet(first, ordinals[expected]);
                assertSet(second, ordinals[expected]);
                expected = nextDocWithValue(ordinals, expected + 1);
            }
            assertThat(first.nextDoc(), equalTo(NO_MORE_DOCS));
        }
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
        final long[][] ordinals = new long[maxDoc][];
        for (int doc = 0; doc < maxDoc; doc++) {
            ordinals[doc] = randomSet(maxOrd);
        }
        assertFallsBack(DIMENSIONS, ordinals, maxOrd, NO_PRIMARY_SORT_FIELD);
    }

    public void testTooManyRunsFallsBack() throws IOException {
        // few values, but the set changes on almost every doc, so the gate gives up during the walk
        final int maxDoc = randomIntBetween(16, 2000);
        final long[][] ordinals = new long[maxDoc][];
        for (int doc = 0; doc < maxDoc; doc++) {
            ordinals[doc] = doc % 2 == 0 ? new long[] { 0, 1 } : new long[] { 1, 2 };
        }
        assertFallsBack(DIMENSIONS, ordinals, 3, NO_PRIMARY_SORT_FIELD);
    }

    public void testUnknownLayoutIsCorruption() throws IOException {
        final byte layout = randomValueOtherThanMany(
            b -> b == RunTableLayout.LAYOUT_DEFAULT || b == RunTableLayout.LAYOUT_RUN_TABLE,
            ESTestCase::randomByte
        );
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", new byte[] { layout })) {
            expectThrows(
                CorruptIndexException.class,
                () -> new RunTableSortedSetCodec(FALLBACK, null).readOrdinalMeta(readContext(128), meta, new SortedNumericEntry(), 7)
            );
        }
    }

    /**
     * Writes {@code ordinals} with the run-table codec and with the fallback codec alone, and checks that the
     * run-table codec chose the fallback: its output is the fallback's behind the discriminator, and reads back
     * into the same entry. Merge statistics are random, so both of the fallback's addresses-table paths run.
     */
    private static void assertFallsBack(
        final FieldContextResolver resolver,
        final long[][] ordinals,
        long maxOrd,
        int primarySortFieldNumber
    ) throws IOException {
        final int blockSize = randomBlockSize();
        final boolean mergeStats = randomBoolean();
        final Written viaRunTableCodec;
        final Written viaFallback;
        try (Directory dir = newDirectory()) {
            viaRunTableCodec = write(
                dir,
                new RunTableSortedSetCodec(FALLBACK, resolver),
                blockSize,
                ordinals,
                maxOrd,
                primarySortFieldNumber,
                mergeStats
            );
            viaFallback = write(dir, FALLBACK, blockSize, ordinals, maxOrd, primarySortFieldNumber, mergeStats);
        }

        assertThat(viaRunTableCodec.meta()[0], equalTo(RunTableLayout.LAYOUT_DEFAULT));
        assertThat(Arrays.copyOfRange(viaRunTableCodec.meta(), 1, viaRunTableCodec.meta().length), equalTo(viaFallback.meta()));
        assertThat(viaRunTableCodec.data(), equalTo(viaFallback.data()));
        assertThat(viaRunTableCodec.stats(), equalTo(viaFallback.stats()));

        final SortedNumericEntry entry = new SortedNumericEntry();
        readOrdinalMeta(new RunTableSortedSetCodec(FALLBACK, null), viaRunTableCodec, entry, blockSize);
        final SortedNumericEntry expected = new SortedNumericEntry();
        readOrdinalMeta(FALLBACK, viaFallback, expected, blockSize);
        assertThat(entry.numDocsWithField, equalTo(expected.numDocsWithField));
        assertThat(entry.numValues, equalTo(expected.numValues));
        assertThat(entry.docsWithFieldOffset, equalTo(expected.docsWithFieldOffset));
        assertThat(entry.valuesOffset, equalTo(expected.valuesOffset));
        assertThat(entry.addressesOffset, equalTo(expected.addressesOffset));
        assertThat(entry.addressesLength, equalTo(expected.addressesLength));
    }

    private static Written write(
        final Directory dir,
        final SortedSetOrdinalCodec codec,
        int blockSize,
        final long[][] ordinals,
        long maxOrd,
        int primarySortFieldNumber
    ) throws IOException {
        return write(dir, codec, blockSize, ordinals, maxOrd, primarySortFieldNumber, randomBoolean());
    }

    private static Written write(
        final Directory dir,
        final SortedSetOrdinalCodec codec,
        int blockSize,
        final long[][] ordinals,
        long maxOrd,
        int primarySortFieldNumber,
        boolean mergeStats
    ) throws IOException {
        return OrdinalCodecTestUtil.write(
            dir,
            blockSize,
            ordinals.length,
            primarySortFieldNumber,
            ctx -> codec.createWriter(ctx)
                .writeOrdinals(
                    OrdinalCodecTestUtil.fieldInfo(DocValuesType.SORTED_SET),
                    sortedSetValuesSource(ordinals, mergeStats),
                    maxOrd
                )
        );
    }

    /** Parses {@code written}'s metadata into {@code entry}, checking it is consumed exactly. */
    private static SortedSetOrdinalReader readOrdinalMeta(
        final SortedSetOrdinalCodec codec,
        final Written written,
        final SortedNumericEntry entry,
        int blockSize
    ) throws IOException {
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", written.meta())) {
            final SortedSetOrdinalReader reader = codec.readOrdinalMeta(
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
     * {@code sparse} adds runs of docs without a value. The first run always has two values, so the field is
     * multi-valued, as it is whenever the consumer hands it to a sorted-set codec.
     */
    private static long[][] runShapedOrdinals(int maxDoc, long maxOrd, boolean sparse) {
        final long[][] ordinals = new long[maxDoc][];
        int doc = 0;
        while (doc < maxDoc) {
            final int remaining = maxDoc - doc;
            final int runLength = remaining < 8 ? remaining : randomIntBetween(4, Math.min(remaining - 4, 200));
            final long[] set = doc == 0 ? new long[] { 0, maxOrd - 1 } : sparse && randomBoolean() ? EMPTY : randomSet(maxOrd);
            Arrays.fill(ordinals, doc, doc + runLength, set);
            doc += runLength;
        }
        return ordinals;
    }

    private static long[] randomSet(long maxOrd) {
        return randomSubsetOf(randomIntBetween(1, (int) Math.min(4, maxOrd)), LongStream.range(0, maxOrd).boxed().toList()).stream()
            .mapToLong(Long::longValue)
            .sorted()
            .toArray();
    }

    private static void assertSet(final SortedNumericDocValues values, final long[] expected) throws IOException {
        assertThat(values.docValueCount(), equalTo(expected.length));
        for (long ord : expected) {
            assertThat(values.nextValue(), equalTo(ord));
        }
    }

    private static int countDocsWithValue(final long[][] ordinals) {
        return (int) Arrays.stream(ordinals).filter(set -> set.length > 0).count();
    }

    private static long countValues(final long[][] ordinals) {
        return Arrays.stream(ordinals).mapToLong(set -> set.length).sum();
    }

    private static int nextDocWithValue(final long[][] ordinals, int from) {
        for (int doc = from; doc < ordinals.length; doc++) {
            if (ordinals[doc].length > 0) {
                return doc;
            }
        }
        return NO_MORE_DOCS;
    }
}
