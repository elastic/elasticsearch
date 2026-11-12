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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;
import org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.RecordingOrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.Written;
import org.elasticsearch.index.codec.tsdb.es95.ES95DefaultSortedCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.MISSING;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.assertDecodesBlockCodecOutput;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.randomBlockSize;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.readContext;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.sortedValuesSource;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.write;
import static org.elasticsearch.index.codec.tsdb.OrdinalCodecTestUtil.writeContext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests {@link ES95DefaultSortedCodec} as a thin adapter over an {@link OrdinalBlockCodec}: every call must
 * reach the wrapped block codec with the same context and arguments, and the bytes it writes must be
 * indistinguishable from the block codec's own output. The {@link SortedOrdinalReader#ordinals} path
 * is checked against a recording {@link BlockOrdinalsFactory}; the full read path runs in the format-level
 * tests.
 */
public class ES95DefaultSortedCodecTests extends ESTestCase {

    /**
     * {@link ES95DefaultSortedCodec#createWriter} must return a different instance on every call so that
     * concurrent merges own independent writers with independent encoder state.
     */
    public void testCreateWriterReturnsFreshInstances() {
        final NumericWriteContext ctx = writeContext(null, null, 0, randomBlockSize());
        final ES95DefaultSortedCodec codec = new ES95DefaultSortedCodec(new TSDBOrdinalBlockCodec());

        final SortedOrdinalWriter first = codec.createWriter(ctx);
        final SortedOrdinalWriter second = codec.createWriter(ctx);
        assertThat(second, not(sameInstance(first)));
    }

    /**
     * The block codec's writer is created from the very same {@link NumericWriteContext} the sorted codec
     * received. The block codec derives its block size and output streams from that context, so any copying
     * or re-wrapping here would silently change the on-disk layout.
     */
    public void testCreateWriterForwardsContextToBlockCodec() {
        final RecordingOrdinalBlockCodec blockCodec = new RecordingOrdinalBlockCodec();
        final NumericWriteContext ctx = writeContext(null, null, 0, randomBlockSize());

        new ES95DefaultSortedCodec(blockCodec).createWriter(ctx);

        assertThat(blockCodec.writeContexts, equalTo(List.of(ctx)));
        assertThat(blockCodec.writeContexts.get(0), sameInstance(ctx));
    }

    /**
     * {@link SortedOrdinalWriter#writeOrdinals} must produce byte-identical metadata and data, and the same
     * statistics, as {@link OrdinalFieldWriter#writeFieldEntry} on the wrapped block codec. This is the
     * backwards-compatibility guarantee of the default codec: segments written after the sorted/sorted-set
     * split are indistinguishable from those written before it.
     */
    public void testWriteOrdinalsMatchesBlockCodecOutput() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(1, 3 * blockSize);
        final long maxOrd = randomLongBetween(2, 255);
        final long[] ordinals = randomOrdinals(maxDoc, maxOrd, randomBoolean());
        final FieldInfo field = sortedFieldInfo();

        final Written viaDefaultCodec = write(
            blockSize,
            maxDoc,
            ctx -> new ES95DefaultSortedCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                .writeOrdinals(field, sortedValuesSource(ordinals), maxOrd, null)
        );
        final Written viaBlockCodec = write(
            blockSize,
            maxDoc,
            ctx -> new TSDBOrdinalBlockCodec().createWriter(ctx).writeFieldEntry(field, sortedValuesSource(ordinals), maxOrd, null, null)
        );

        assertThat(viaDefaultCodec.stats(), equalTo(viaBlockCodec.stats()));
        assertThat(viaDefaultCodec.stats().numValues(), equalTo((long) countPresent(ordinals)));
        assertThat(viaDefaultCodec.meta(), equalTo(viaBlockCodec.meta()));
        assertThat(viaDefaultCodec.data(), equalTo(viaBlockCodec.data()));
    }

    /**
     * The {@link SortedFieldObserver} handed to {@link SortedOrdinalWriter#writeOrdinals} is forwarded to the
     * block codec and sees every document with a value, with its ordinal, in document order. The primary
     * sort field's prefix partitions are built from these callbacks, so dropping or reordering them would
     * corrupt that index.
     */
    public void testWriteOrdinalsNotifiesSortedFieldObserver() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(1, 3 * blockSize);
        final long maxOrd = randomLongBetween(2, 255);
        final long[] ordinals = randomOrdinals(maxDoc, maxOrd, randomBoolean());

        final List<DocOrd> expected = new ArrayList<>();
        for (int doc = 0; doc < maxDoc; doc++) {
            if (ordinals[doc] != MISSING) {
                expected.add(new DocOrd(doc, ordinals[doc]));
            }
        }

        final RecordingSortedFieldObserver observer = new RecordingSortedFieldObserver();
        write(
            blockSize,
            maxDoc,
            ctx -> new ES95DefaultSortedCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                .writeOrdinals(sortedFieldInfo(), sortedValuesSource(ordinals), maxOrd, observer)
        );

        assertThat(observer.seen, equalTo(expected));
    }

    /**
     * Metadata written through {@link SortedOrdinalWriter#writeOrdinals} is parsed back by
     * {@link SortedOrdinalCodec#readOrdinalMeta} into the {@link NumericEntry} the producer uses for sorted
     * fields: the value and document counts and the sparse/dense marker match the input,
     * {@code entry.blockSize} is seeded from the format-level shift the reader was given, and the metadata
     * stream is consumed exactly to its end.
     */
    public void testReadOrdinalMetaParsesWriterOutput() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(1, 3 * blockSize);
        final long maxOrd = randomLongBetween(2, 255);
        final boolean dense = randomBoolean();
        final long[] ordinals = randomOrdinals(maxDoc, maxOrd, dense);
        final int numDocsWithValue = countPresent(ordinals);

        final Written written = write(
            blockSize,
            maxDoc,
            ctx -> new ES95DefaultSortedCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                .writeOrdinals(sortedFieldInfo(), sortedValuesSource(ordinals), maxOrd, null)
        );

        final ES95DefaultSortedCodec codec = new ES95DefaultSortedCodec(new TSDBOrdinalBlockCodec());
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", written.meta())) {
            final NumericEntry entry = new NumericEntry();
            codec.readOrdinalMeta(readContext(blockSize), meta, entry, Integer.numberOfTrailingZeros(blockSize));

            assertThat(entry.numValues, equalTo((long) numDocsWithValue));
            assertThat(entry.numDocsWithField, equalTo(numDocsWithValue));
            assertThat(entry.blockSize, equalTo(blockSize));
            assertThat(entry.valuesLength, greaterThan(0L));
            if (numDocsWithValue == maxDoc) {
                assertThat("dense field has no DISI", entry.docsWithFieldOffset, equalTo(-1L));
            } else {
                assertThat("sparse field points at its DISI", entry.docsWithFieldOffset, greaterThanOrEqualTo(0L));
                assertThat(entry.docsWithFieldLength, greaterThan(0L));
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
        final long maxOrd = randomLongBetween(2, 255);
        final Written written = write(
            blockSize,
            maxDoc,
            ctx -> new ES95DefaultSortedCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                .writeOrdinals(sortedFieldInfo(), sortedValuesSource(randomOrdinals(maxDoc, maxOrd, randomBoolean())), maxOrd, null)
        );

        final RecordingOrdinalBlockCodec blockCodec = new RecordingOrdinalBlockCodec();
        final NumericReadContext readCtx = readContext(blockSize);
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", written.meta())) {
            new ES95DefaultSortedCodec(blockCodec).readOrdinalMeta(
                readCtx,
                meta,
                new NumericEntry(),
                Integer.numberOfTrailingZeros(blockSize)
            );
        }

        assertThat(blockCodec.readContexts, equalTo(List.of(readCtx)));
        assertThat(blockCodec.readContexts.get(0), sameInstance(readCtx));
    }

    /**
     * The reader returned by {@link ES95DefaultSortedCodec#readOrdinalMeta} serves the blocked layout through the
     * {@link BlockOrdinalsFactory} of the read context instead of reaching into the producer: the factory
     * receives the entry the reader was bound to, the term count and the wrapped block codec's decoder for
     * the entry's block size, and its result is returned unchanged.
     */
    public void testOrdinalsDelegatesToBlockOrdinalsFactory() throws IOException {
        final int blockSize = randomBlockSize();
        final int maxDoc = randomIntBetween(1, 3 * blockSize);
        final long maxOrd = randomLongBetween(2, 255);
        final ES95DefaultSortedCodec codec = new ES95DefaultSortedCodec(new TSDBOrdinalBlockCodec());
        final Written written = write(
            blockSize,
            maxDoc,
            ctx -> codec.createWriter(ctx)
                .writeOrdinals(sortedFieldInfo(), sortedValuesSource(randomOrdinals(maxDoc, maxOrd, randomBoolean())), maxOrd, null)
        );

        final NumericEntry entry = new NumericEntry();
        final SortedOrdinalReader reader;
        try (IndexInput meta = new ByteArrayIndexInput("test-meta", written.meta())) {
            reader = codec.readOrdinalMeta(readContext(blockSize), meta, entry, Integer.numberOfTrailingZeros(blockSize));
        }

        final NumericDocValues expected = DocValues.emptyNumeric();
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
     * Verifies that {@link SortedOrdinalWriter#writeOrdinals} does not throw when maxOrd is 0
     * (a field with no values, hence no ordinals).
     */
    public void testMaxOrdinalGuard() throws IOException {
        final TsdbDocValuesProducer valuesSource = new TsdbDocValuesProducer(new DocValuesConsumerUtil.MergeStats(true, 0, 0, 0, 0));
        write(
            randomBlockSize(),
            0,
            ctx -> new ES95DefaultSortedCodec(new TSDBOrdinalBlockCodec()).createWriter(ctx)
                .writeOrdinals(sortedFieldInfo(), valuesSource, 0, null)
        );
    }

    private record DocOrd(int docId, long ord) {}

    private static FieldInfo sortedFieldInfo() {
        return OrdinalCodecTestUtil.fieldInfo(DocValuesType.SORTED);
    }

    /**
     * One ordinal per document in {@code [0, maxOrd)}. When {@code dense} is false a random subset of
     * documents (never all of them) is left without a value.
     */
    private static long[] randomOrdinals(final int maxDoc, final long maxOrd, final boolean dense) {
        final long[] ordinals = new long[maxDoc];
        for (int doc = 0; doc < maxDoc; doc++) {
            ordinals[doc] = dense || randomBoolean() ? randomLongBetween(0, maxOrd - 1) : MISSING;
        }
        // guarantee at least one value so that the block layout is exercised
        ordinals[randomIntBetween(0, maxDoc - 1)] = randomLongBetween(0, maxOrd - 1);
        return ordinals;
    }

    private static int countPresent(final long[] ordinals) {
        int count = 0;
        for (long ordinal : ordinals) {
            if (ordinal != MISSING) {
                count++;
            }
        }
        return count;
    }

    /**
     * Records the doc pass so the test can assert what the wrapped block codec reported.
     */
    private static final class RecordingSortedFieldObserver implements SortedFieldObserver {

        final List<DocOrd> seen = new ArrayList<>();

        @Override
        public void onTerm(final BytesRef term, final long ord) {}

        @Override
        public void prepareForDocs() {}

        @Override
        public void onDoc(final int docId, final long ord) {
            seen.add(new DocOrd(docId, ord));
        }

        @Override
        public void flush(final IndexOutput data, final IndexOutput meta) {}
    }

    /**
     * Stands in for the producer's factory: records the arguments of the single-valued call and returns a
     * fixed result, and rejects the multi-valued call the sorted reader must never make.
     */
    private static final class RecordingBlockOrdinalsFactory implements BlockOrdinalsFactory {

        private final NumericDocValues result;
        NumericEntry entry;
        long maxOrd;
        OrdinalFieldReader.Decoder decoder;

        RecordingBlockOrdinalsFactory(final NumericDocValues result) {
            this.result = result;
        }

        @Override
        public NumericDocValues sorted(final NumericEntry entry, long maxOrd, final OrdinalFieldReader.Decoder decoder) {
            this.entry = entry;
            this.maxOrd = maxOrd;
            this.decoder = decoder;
            return result;
        }

        @Override
        public SortedNumericDocValues sortedSet(final SortedNumericEntry entry, long maxOrd, final OrdinalFieldReader.Decoder decoder) {
            throw new AssertionError("sorted reader must not build multi-valued ordinals");
        }
    }
}
