/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;
import org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.RecordingOrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.Written;
import org.elasticsearch.index.codec.tsdb.es95.ES95DefaultSortedSetCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.assertDecodesBlockCodecOutput;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.randomBlockSize;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.readContext;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.sortedSetValuesSource;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.write;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.writeContext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests {@link ES95DefaultSortedSetCodec} as a thin adapter over an {@link OrdinalBlockCodec}: every call
 * must reach the wrapped block codec with the same context and arguments, and the bytes it writes must be
 * the block codec's own output followed by the per-doc addresses table of {@link MultiValueAddresses}.
 * The {@link SortedSetOrdinalReader#ordinals} path is checked against a recording
 * {@link BlockOrdinalsFactory}; the full read path runs in the format-level tests.
 */
public class ES95DefaultSortedSetCodecTests extends ESTestCase {

    private static final int MAX_VALUES_PER_DOC = 3;

    /**
     * {@link ES95DefaultSortedSetCodec#createWriter} must return a different instance on every call so that
     * concurrent merges own independent writers with independent encoder state.
     */
    public void testCreateWriterReturnsFreshInstances() {
        final NumericWriteContext ctx = writeContext(null, null, 0, randomBlockSize());
        final ES95DefaultSortedSetCodec codec = new ES95DefaultSortedSetCodec(new TSDBOrdinalBlockCodec());

        final SortedSetOrdinalWriter first = codec.createWriter(ctx);
        final SortedSetOrdinalWriter second = codec.createWriter(ctx);
        assertThat(second, not(sameInstance(first)));
    }

    /**
     * The block codec's writer is created from the very same {@link NumericWriteContext} the sorted-set
     * codec received. The block codec derives its block size and output streams from that context, so
     * any copying or re-wrapping here would silently change the on-disk layout.
     */
    public void testCreateWriterForwardsContextToBlockCodec() {
        final RecordingOrdinalBlockCodec blockCodec = new RecordingOrdinalBlockCodec();
        final NumericWriteContext ctx = writeContext(null, null, 0, randomBlockSize());

        new ES95DefaultSortedSetCodec(blockCodec).createWriter(ctx);

        assertThat(blockCodec.writeContexts, equalTo(List.of(ctx)));
        assertThat(blockCodec.writeContexts.get(0), sameInstance(ctx));
    }

    /**
     * {@link SortedSetOrdinalWriter#writeOrdinals} must produce the same statistics as
     * {@link OrdinalFieldWriter#writeFieldEntry} on the wrapped block codec, and metadata and data that start
     * with the block codec's own bytes and continue with the per-doc addresses table exactly when the field
     * has more values than documents. This is the backwards-compatibility guarantee of the default codec:
     * before the sorted/sorted-set split the shared consumer wrote that table around the block codec, so
     * segments written after the split are indistinguishable from those written before it.
     */
    public void testWriteOrdinalsMatchesBlockCodecOutput() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(1, 3 * blockSize);
        final long maxOrd = randomLongBetween(MAX_VALUES_PER_DOC, 255);
        final long[][] ordinals = randomOrdinals(maxDoc, maxOrd, randomBoolean());
        final FieldInfo field = sortedSetFieldInfo();

        final Written viaDefaultCodec = write(
            blockSize,
            maxDoc,
            ctx -> new ES95DefaultSortedSetCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                .writeOrdinals(field, sortedSetValuesSource(ordinals, false), maxOrd)
        );
        final Written viaBlockCodec = write(
            blockSize,
            maxDoc,
            ctx -> new TSDBOrdinalBlockCodec().createWriter(ctx)
                .writeFieldEntry(field, sortedSetValuesSource(ordinals, false), maxOrd, null, null)
        );

        assertThat(viaDefaultCodec.stats(), equalTo(viaBlockCodec.stats()));
        assertThat(viaDefaultCodec.stats().numDocsWithField(), equalTo(countDocsWithValue(ordinals)));
        assertThat(viaDefaultCodec.stats().numValues(), equalTo(countValues(ordinals)));
        assertThat(Arrays.copyOf(viaDefaultCodec.meta(), viaBlockCodec.meta().length), equalTo(viaBlockCodec.meta()));
        assertThat(Arrays.copyOf(viaDefaultCodec.data(), viaBlockCodec.data().length), equalTo(viaBlockCodec.data()));
        final boolean multiValued = countValues(ordinals) > countDocsWithValue(ordinals);
        assertThat(
            "addresses table follows the stream only when multi-valued",
            viaDefaultCodec.meta().length > viaBlockCodec.meta().length,
            equalTo(multiValued)
        );
        assertThat(
            "addresses table follows the stream only when multi-valued",
            viaDefaultCodec.data().length > viaBlockCodec.data().length,
            equalTo(multiValued)
        );
    }

    /**
     * A multi-valued stream is followed by the per-doc addresses table that the shared consumer used to write
     * around the ordinal codec and that the sorted-set codec now owns. Both ways of building the table are
     * covered: from merge statistics, where the counts are accumulated during the single doc pass, and
     * without them, where the values are iterated a second time. The table is read back through
     * {@link SortedSetOrdinalCodec#readOrdinalMeta} and must give the slice start of every document with a
     * value, plus the final fencepost.
     */
    public void testWriteOrdinalsWritesAddressesTable() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(1, 3 * blockSize);
        final long maxOrd = randomLongBetween(MAX_VALUES_PER_DOC, 255);
        final long[][] ordinals = randomOrdinals(maxDoc, maxOrd, randomBoolean());
        // at least one multi-valued document, so the table exists
        ordinals[randomIntBetween(0, maxDoc - 1)] = new long[] { 0, 1 };
        final boolean mergeStats = randomBoolean();

        final Written written;
        // the merge-statistics path accumulates the table in a temp file of the directory
        try (Directory dir = newDirectory()) {
            written = write(
                dir,
                blockSize,
                maxDoc,
                ctx -> new ES95DefaultSortedSetCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                    .writeOrdinals(sortedSetFieldInfo(), sortedSetValuesSource(ordinals, mergeStats), maxOrd)
            );
        }

        final ES95DefaultSortedSetCodec codec = new ES95DefaultSortedSetCodec(new TSDBOrdinalBlockCodec());
        final SortedNumericEntry entry = new SortedNumericEntry();
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", written.meta())) {
            codec.readOrdinalMeta(readContext(blockSize), meta, entry, Integer.numberOfTrailingZeros(blockSize));
            assertThat("metadata stream not fully consumed", meta.getFilePointer(), equalTo(meta.length()));
        }
        assertThat(entry.addressesMeta, notNullValue());
        assertThat(entry.addressesLength, greaterThan(0L));

        try (IndexInput data = new ByteArrayIndexInput("test-data", written.data())) {
            final LongValues addresses = DirectMonotonicReader.getInstance(
                entry.addressesMeta,
                data.randomAccessSlice(entry.addressesOffset, entry.addressesLength)
            );
            long expectedStart = 0;
            int index = 0;
            assertThat(addresses.get(index), equalTo(expectedStart));
            for (long[] docOrdinals : ordinals) {
                if (docOrdinals.length > 0) {
                    expectedStart += docOrdinals.length;
                    assertThat(addresses.get(++index), equalTo(expectedStart));
                }
            }
            assertThat(index, equalTo(countDocsWithValue(ordinals)));
        }
    }

    /**
     * Metadata written through {@link SortedSetOrdinalWriter#writeOrdinals} is parsed back by
     * {@link SortedSetOrdinalCodec#readOrdinalMeta} into the {@link SortedNumericEntry} the producer
     * uses for sorted-set fields: the value and document counts and the sparse/dense marker match the
     * input, {@code entry.blockSize} is seeded from the format-level shift the reader was given, the
     * addresses table is present exactly when the field is multi-valued, and the metadata stream is
     * consumed exactly to its end.
     */
    public void testReadOrdinalMetaParsesWriterOutput() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(1, 3 * blockSize);
        final long maxOrd = randomLongBetween(MAX_VALUES_PER_DOC, 255);
        final boolean dense = randomBoolean();
        final long[][] ordinals = randomOrdinals(maxDoc, maxOrd, dense);
        final int numDocsWithValue = countDocsWithValue(ordinals);
        final boolean mergeStats = randomBoolean();

        final Written written;
        // the merge-statistics path accumulates the table in a temp file of the directory
        try (Directory dir = newDirectory()) {
            written = write(
                dir,
                blockSize,
                maxDoc,
                ctx -> new ES95DefaultSortedSetCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                    .writeOrdinals(sortedSetFieldInfo(), sortedSetValuesSource(ordinals, mergeStats), maxOrd)
            );
        }

        final ES95DefaultSortedSetCodec codec = new ES95DefaultSortedSetCodec(new TSDBOrdinalBlockCodec());
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", written.meta())) {
            final SortedNumericEntry entry = new SortedNumericEntry();
            codec.readOrdinalMeta(readContext(blockSize), meta, entry, Integer.numberOfTrailingZeros(blockSize));

            assertThat(entry.numValues, equalTo(countValues(ordinals)));
            assertThat(entry.numDocsWithField, equalTo(numDocsWithValue));
            assertThat(entry.blockSize, equalTo(blockSize));
            assertThat(entry.valuesLength, greaterThan(0L));
            if (numDocsWithValue == maxDoc) {
                assertThat("dense field has no DISI", entry.docsWithFieldOffset, equalTo(-1L));
            } else {
                assertThat("sparse field points at its DISI", entry.docsWithFieldOffset, greaterThanOrEqualTo(0L));
                assertThat(entry.docsWithFieldLength, greaterThan(0L));
            }
            if (countValues(ordinals) > numDocsWithValue) {
                assertThat("multi-valued field has an addresses table", entry.addressesMeta, notNullValue());
                assertThat(entry.addressesLength, greaterThan(0L));
            } else {
                assertThat("single-valued field has no addresses table", entry.addressesMeta, nullValue());
            }
            assertThat("metadata stream not fully consumed", meta.getFilePointer(), equalTo(meta.length()));
        }
    }

    /**
     * The block codec's reader is created from the very same {@link NumericReadContext} the codec received.
     * Version-aware block codecs (e.g. ES95) decide how to parse field metadata purely from
     * {@code ctx.segmentVersion()}, so the context must be threaded through untouched.
     */
    public void testReadOrdinalMetaForwardsContextToBlockCodec() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(1, 3 * blockSize);
        final long maxOrd = randomLongBetween(MAX_VALUES_PER_DOC, 255);
        final Written written = write(
            blockSize,
            maxDoc,
            ctx -> new ES95DefaultSortedSetCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                .writeOrdinals(sortedSetFieldInfo(), sortedSetValuesSource(randomOrdinals(maxDoc, maxOrd, randomBoolean()), false), maxOrd)
        );

        final RecordingOrdinalBlockCodec blockCodec = new RecordingOrdinalBlockCodec();
        final NumericReadContext readCtx = readContext(blockSize);
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", written.meta())) {
            new ES95DefaultSortedSetCodec(blockCodec).readOrdinalMeta(
                readCtx,
                meta,
                new SortedNumericEntry(),
                Integer.numberOfTrailingZeros(blockSize)
            );
        }

        assertThat(blockCodec.readContexts, equalTo(List.of(readCtx)));
        assertThat(blockCodec.readContexts.get(0), sameInstance(readCtx));
    }

    /**
     * The reader returned by {@link ES95DefaultSortedSetCodec#readOrdinalMeta} serves the blocked layout through the
     * {@link BlockOrdinalsFactory} of the read context instead of reaching into the producer: the factory
     * receives the entry the reader was bound to, the term count and the wrapped block codec's decoder for
     * the entry's block size, and its result is returned unchanged.
     */
    public void testOrdinalsDelegatesToBlockOrdinalsFactory() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(1, 3 * blockSize);
        final long maxOrd = randomLongBetween(MAX_VALUES_PER_DOC, 255);
        final ES95DefaultSortedSetCodec codec = new ES95DefaultSortedSetCodec(new TSDBOrdinalBlockCodec());
        final Written written = write(
            blockSize,
            maxDoc,
            ctx -> codec.createWriter(ctx)
                .writeOrdinals(sortedSetFieldInfo(), sortedSetValuesSource(randomOrdinals(maxDoc, maxOrd, randomBoolean()), false), maxOrd)
        );

        final SortedNumericEntry entry = new SortedNumericEntry();
        final SortedSetOrdinalReader reader;
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", written.meta())) {
            reader = codec.readOrdinalMeta(readContext(blockSize), meta, entry, Integer.numberOfTrailingZeros(blockSize));
        }

        final SortedNumericDocValues expected = DocValues.emptySortedNumeric();
        final RecordingBlockOrdinalsFactory factory = new RecordingBlockOrdinalsFactory(expected);
        // the default reader never touches the data input, so an empty one is enough
        try (IndexInput data = new ByteArrayIndexInput("test-data", new byte[0])) {
            assertThat(reader.ordinals(new OrdinalReadContext(data, maxDoc, factory), maxOrd), sameInstance(expected));
        }

        assertThat(factory.entry, sameInstance(entry));
        assertThat(factory.maxOrd, equalTo(maxOrd));
        assertDecodesBlockCodecOutput(factory.decoder, blockSize, maxOrd);
    }

    /**
     * Verifies that {@link SortedSetOrdinalWriter#writeOrdinals} does not throw when maxOrd is 0
     * (a field with no values, hence no ordinals).
     */
    public void testMaxOrdinalGuard() throws IOException {
        final TsdbDocValuesProducer valuesSource = new TsdbDocValuesProducer(new DocValuesConsumerUtil.MergeStats(true, 0, 0, 0, 0));
        write(
            randomBlockSize(),
            0,
            ctx -> new ES95DefaultSortedSetCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                .writeOrdinals(sortedSetFieldInfo(), valuesSource, 0)
        );
    }

    private static FieldInfo sortedSetFieldInfo() {
        return OrdinalCodecTestUtil.fieldInfo(DocValuesType.SORTED_SET);
    }

    /**
     * Between one and {@link #MAX_VALUES_PER_DOC} distinct, ascending ordinals in {@code [0, maxOrd)} per
     * document. When {@code dense} is false a random subset of documents (never all of them) is left
     * without a value, represented by an empty array.
     */
    private static long[][] randomOrdinals(final int maxDoc, final long maxOrd, final boolean dense) {
        final long[][] ordinals = new long[maxDoc][];
        for (int doc = 0; doc < maxDoc; doc++) {
            ordinals[doc] = dense || randomBoolean() ? randomDocOrdinals(maxOrd) : new long[0];
        }
        // guarantee at least one value so that the block layout is exercised
        ordinals[randomIntBetween(0, maxDoc - 1)] = randomDocOrdinals(maxOrd);
        return ordinals;
    }

    private static long[] randomDocOrdinals(final long maxOrd) {
        final TreeSet<Long> distinct = new TreeSet<>();
        final int count = randomIntBetween(1, MAX_VALUES_PER_DOC);
        while (distinct.size() < count) {
            distinct.add(randomLongBetween(0, maxOrd - 1));
        }
        return distinct.stream().mapToLong(Long::longValue).toArray();
    }

    private static int countDocsWithValue(final long[][] ordinals) {
        return (int) Arrays.stream(ordinals).filter(docOrdinals -> docOrdinals.length > 0).count();
    }

    private static long countValues(final long[][] ordinals) {
        return Arrays.stream(ordinals).mapToLong(docOrdinals -> docOrdinals.length).sum();
    }

    /**
     * Stands in for the producer's factory: records the arguments of the multi-valued call and returns a
     * fixed result, and rejects the single-valued call the sorted-set reader must never make.
     */
    private static final class RecordingBlockOrdinalsFactory implements BlockOrdinalsFactory {

        private final SortedNumericDocValues result;
        SortedNumericEntry entry;
        long maxOrd;
        OrdinalFieldReader.Decoder decoder;

        RecordingBlockOrdinalsFactory(final SortedNumericDocValues result) {
            this.result = result;
        }

        @Override
        public NumericDocValues sorted(final NumericEntry entry, long maxOrd, final OrdinalFieldReader.Decoder decoder) {
            throw new AssertionError("sorted-set reader must not build single-valued ordinals");
        }

        @Override
        public SortedNumericDocValues sortedSet(final SortedNumericEntry entry, long maxOrd, final OrdinalFieldReader.Decoder decoder) {
            this.entry = entry;
            this.maxOrd = maxOrd;
            this.decoder = decoder;
            return result;
        }
    }
}
