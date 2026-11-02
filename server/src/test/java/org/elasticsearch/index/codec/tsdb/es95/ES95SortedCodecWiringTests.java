/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalReader;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalReader;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

/**
 * Verifies that the ES95 format routes each ordinal stream to the right codec slot: SORTED fields and
 * single-valued SORTED_SET fields to the {@link SortedOrdinalCodec}, multi-valued SORTED_SET fields to the
 * {@link SortedSetOrdinalCodec}, on flush, on merge and on read.
 *
 * <p>The shipped codecs write byte-identical ordinal streams, so a mis-wired slot would pass every
 * round-trip test. The only way to observe the dispatch is to inject codecs that delegate to the real
 * ES95 ones and record what flows through them. The three fields are sized so that the value count and
 * the term count identify the field on the read side, where the codec interfaces do not see the field name.
 */
public class ES95SortedCodecWiringTests extends ESTestCase {

    private static final String SORTED_FIELD = "sorted";
    private static final String SINGLE_VALUED_SET_FIELD = "single_valued_set";
    private static final String MULTI_VALUED_SET_FIELD = "multi_valued_set";

    /** Two flushes of {@code NUM_DOCS / 2} docs each, then a force merge to one segment. */
    private static final int NUM_DOCS = 24;
    private static final int SEGMENT_WRITES = 3;

    /**
     * The read side identifies a field by its term count and its value count, and the merge opens the two
     * flushed segments as well as the merged one, so every segment must contain every term of every field
     * and no per-segment or merged count may be shared between fields. Term counts: 3, 4 and 7. Value
     * counts: sorted 24 and 12, single-valued set 8 and 4, multi-valued set 72 and 36.
     */
    private static final int SORTED_TERMS = 3;
    /** Present on every third doc only; the four docs per segment hit all four terms since 3 and 4 are coprime. */
    private static final int SINGLE_VALUED_SET_TERMS = 4;
    private static final int SINGLE_VALUED_SET_DOC_STRIDE = 3;
    private static final int MULTI_VALUED_SET_TERMS = 7;
    private static final int VALUES_PER_MULTI_DOC = 3;

    public void testSortedAndSortedSetFieldsAreRoutedToTheirCodecs() throws IOException {
        final RecordingSortedCodec sortedCodec = new RecordingSortedCodec();
        final RecordingSortedSetCodec sortedSetCodec = new RecordingSortedSetCodec();

        // A plain directory rather than newDirectory(): the mock wrapper runs CheckIndex on close, which reopens
        // the index from disk and resolves the codec by name, so it would read the segment with the default
        // Lucene doc values format instead of the injected one.
        try (Directory dir = new ByteBuffersDirectory()) {
            final IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(codecWith(sortedCodec, sortedSetCodec));
            config.setMergePolicy(new LogByteSizeMergePolicy());
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (int doc = 0; doc < NUM_DOCS; doc++) {
                    writer.addDocument(document(doc));
                    if (doc == NUM_DOCS / 2 - 1) {
                        writer.flush();
                    }
                }
                writer.flush();
                writer.forceMerge(1);

                // Write side: every segment write (two flushes and the merge) dispatched the sorted field
                // and the single-valued sorted-set field to the sorted codec and only the multi-valued
                // sorted-set field to the sorted-set codec.
                assertThat(sortedCodec.writtenFields, hasSize(2 * SEGMENT_WRITES));
                assertThat(Collections.frequency(sortedCodec.writtenFields, SORTED_FIELD), equalTo(SEGMENT_WRITES));
                assertThat(Collections.frequency(sortedCodec.writtenFields, SINGLE_VALUED_SET_FIELD), equalTo(SEGMENT_WRITES));
                assertThat(sortedSetCodec.writtenFields, equalTo(Collections.nCopies(SEGMENT_WRITES, MULTI_VALUED_SET_FIELD)));

                // The reader must come from the writer: reopening from disk resolves the codec by name and
                // would hand the producer the default ES95 codecs instead of the recording ones.
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves(), hasSize(1));
                    final LeafReader leaf = reader.leaves().get(0).reader();

                    final SortedDocValues sorted = leaf.getSortedDocValues(SORTED_FIELD);
                    final SortedSetDocValues singleValuedSet = leaf.getSortedSetDocValues(SINGLE_VALUED_SET_FIELD);
                    final SortedSetDocValues multiValuedSet = leaf.getSortedSetDocValues(MULTI_VALUED_SET_FIELD);

                    // Read side, metadata: the merge also opened the source segments, so the recorded value
                    // counts include per-segment sizes. Every count must belong to a field of the right kind,
                    // and the merged segment's counts must be present.
                    final long sortedMergedValues = NUM_DOCS;
                    final long sortedSegmentValues = NUM_DOCS / 2;
                    final long singleValuedSetMergedValues = NUM_DOCS / SINGLE_VALUED_SET_DOC_STRIDE;
                    final long singleValuedSetSegmentValues = singleValuedSetMergedValues / 2;
                    final long multiValuedSetMergedValues = (long) NUM_DOCS * VALUES_PER_MULTI_DOC;
                    final long multiValuedSetSegmentValues = multiValuedSetMergedValues / 2;
                    assertThat(
                        "value counts must identify the fields unambiguously",
                        Set.of(
                            sortedMergedValues,
                            sortedSegmentValues,
                            singleValuedSetMergedValues,
                            singleValuedSetSegmentValues,
                            multiValuedSetMergedValues,
                            multiValuedSetSegmentValues
                        ),
                        hasSize(6)
                    );
                    assertThat(
                        sortedCodec.readNumValues,
                        everyItem(
                            is(
                                in(
                                    Set.of(
                                        sortedMergedValues,
                                        sortedSegmentValues,
                                        singleValuedSetMergedValues,
                                        singleValuedSetSegmentValues
                                    )
                                )
                            )
                        )
                    );
                    assertThat(sortedCodec.readNumValues, hasItems(sortedMergedValues, singleValuedSetMergedValues));
                    assertThat(
                        sortedSetCodec.readNumValues,
                        everyItem(is(in(Set.of(multiValuedSetMergedValues, multiValuedSetSegmentValues))))
                    );
                    assertThat(sortedSetCodec.readNumValues, hasItems(multiValuedSetMergedValues));

                    // Read side, doc values: each lookup asked exactly one codec for the ordinals, identified
                    // by the field's term count.
                    assertThat(sortedCodec.ordinalsMaxOrds, everyItem(is(in(Set.of((long) SORTED_TERMS, (long) SINGLE_VALUED_SET_TERMS)))));
                    assertThat(sortedCodec.ordinalsMaxOrds, hasItems((long) SORTED_TERMS, (long) SINGLE_VALUED_SET_TERMS));
                    assertThat(sortedSetCodec.ordinalsMaxOrds, everyItem(is(in(Set.of((long) MULTI_VALUED_SET_TERMS)))));
                    assertThat(sortedSetCodec.ordinalsMaxOrds, hasItems((long) MULTI_VALUED_SET_TERMS));

                    // The recording codecs delegate to the real ones, so the values still round-trip.
                    assertValues(sorted, singleValuedSet, multiValuedSet);
                }
            }
        }
    }

    private static Document document(int doc) {
        final Document document = new Document();
        document.add(new SortedDocValuesField(SORTED_FIELD, new BytesRef(sortedTerm(doc))));
        if (doc % SINGLE_VALUED_SET_DOC_STRIDE == 0) {
            document.add(new SortedSetDocValuesField(SINGLE_VALUED_SET_FIELD, new BytesRef(singleValuedSetTerm(doc))));
        }
        for (String term : multiValuedSetTerms(doc)) {
            document.add(new SortedSetDocValuesField(MULTI_VALUED_SET_FIELD, new BytesRef(term)));
        }
        return document;
    }

    private static String sortedTerm(int doc) {
        return "s" + (doc % SORTED_TERMS);
    }

    private static String singleValuedSetTerm(int doc) {
        return "ss" + (doc % SINGLE_VALUED_SET_TERMS);
    }

    private static List<String> multiValuedSetTerms(int doc) {
        final List<String> terms = new ArrayList<>(VALUES_PER_MULTI_DOC);
        for (int value = 0; value < VALUES_PER_MULTI_DOC; value++) {
            terms.add("m" + ((doc + value) % MULTI_VALUED_SET_TERMS));
        }
        return terms;
    }

    /** Documents keep their insertion order through the merge because there is no index sort. */
    private static void assertValues(
        final SortedDocValues sorted,
        final SortedSetDocValues singleValuedSet,
        final SortedSetDocValues multiValuedSet
    ) throws IOException {
        for (int doc = 0; doc < NUM_DOCS; doc++) {
            assertTrue(sorted.advanceExact(doc));
            assertThat(sorted.lookupOrd(sorted.ordValue()).utf8ToString(), equalTo(sortedTerm(doc)));

            final boolean hasSingleValuedSet = doc % SINGLE_VALUED_SET_DOC_STRIDE == 0;
            assertThat(singleValuedSet.advanceExact(doc), equalTo(hasSingleValuedSet));
            if (hasSingleValuedSet) {
                assertThat(singleValuedSet.docValueCount(), equalTo(1));
                assertThat(singleValuedSet.lookupOrd(singleValuedSet.nextOrd()).utf8ToString(), equalTo(singleValuedSetTerm(doc)));
            }

            assertTrue(multiValuedSet.advanceExact(doc));
            assertThat(multiValuedSet.docValueCount(), equalTo(VALUES_PER_MULTI_DOC));
            final List<String> terms = new ArrayList<>(VALUES_PER_MULTI_DOC);
            for (int value = 0; value < VALUES_PER_MULTI_DOC; value++) {
                terms.add(multiValuedSet.lookupOrd(multiValuedSet.nextOrd()).utf8ToString());
            }
            assertThat(terms, containsInAnyOrder(multiValuedSetTerms(doc).toArray()));
        }
    }

    /**
     * Builds the ES95 format with the given codec slots and attaches it to a codec directly, without the
     * per-field indirection: {@code XPerFieldDocValuesFormat} resolves the doc values format by name when
     * it opens a segment, which would replace this instance with the registered default.
     */
    private static Codec codecWith(final SortedOrdinalCodec sortedCodec, final SortedSetOrdinalCodec sortedSetCodec) {
        final DocValuesFormat format = new ES95TSDBDocValuesFormat(
            ES95TSDBDocValuesFormat.CODEC_NAME,
            ES95TSDBDocValuesFormat.DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ES95TSDBDocValuesFormat.ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            // both the optimized and the regular merge must dispatch the same way
            randomBoolean(),
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT,
            false,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            NumericCodecFactory.DEFAULT,
            ES95NumericFieldReader::defaultFallbackDecoder,
            null,
            sortedCodec,
            sortedSetCodec
        );
        final Codec base = TestUtil.getDefaultCodec();
        return new FilterCodec(base.getName(), base) {
            @Override
            public DocValuesFormat docValuesFormat() {
                return format;
            }
        };
    }

    /**
     * Delegates to the real {@link ES95TSDBDocValuesFormat#SORTED_CODEC} and records what passes through this slot. Needed
     * because the shipped codec exposes nothing about the fields it handled. The reader interface does
     * not see the field, so it records the parsed value count and the requested term count instead.
     */
    private static final class RecordingSortedCodec implements SortedOrdinalCodec {

        private final SortedOrdinalCodec delegate = ES95TSDBDocValuesFormat.SORTED_CODEC;
        final List<String> writtenFields = new ArrayList<>();
        final List<Long> readNumValues = new ArrayList<>();
        final List<Long> ordinalsMaxOrds = new ArrayList<>();

        @Override
        public SortedOrdinalWriter createWriter(final NumericWriteContext ctx) {
            final SortedOrdinalWriter writer = delegate.createWriter(ctx);
            return (field, values, maxOrd, sortedFieldObserver) -> {
                writtenFields.add(field.name);
                return writer.writeOrdinals(field, values, maxOrd, sortedFieldObserver);
            };
        }

        @Override
        public SortedOrdinalReader readOrdinalMeta(
            final NumericReadContext ctx,
            final IndexInput meta,
            final NumericEntry entry,
            int numericBlockShift
        ) throws IOException {
            final SortedOrdinalReader reader = delegate.readOrdinalMeta(ctx, meta, entry, numericBlockShift);
            readNumValues.add(entry.numValues);
            return (readCtx, maxOrd) -> {
                ordinalsMaxOrds.add(maxOrd);
                return reader.ordinals(readCtx, maxOrd);
            };
        }
    }

    /**
     * Sorted-set counterpart of {@link RecordingSortedCodec}, delegating to the real {@link ES95TSDBDocValuesFormat#SORTED_SET_CODEC}.
     */
    private static final class RecordingSortedSetCodec implements SortedSetOrdinalCodec {

        private final SortedSetOrdinalCodec delegate = ES95TSDBDocValuesFormat.SORTED_SET_CODEC;
        final List<String> writtenFields = new ArrayList<>();
        final List<Long> readNumValues = new ArrayList<>();
        final List<Long> ordinalsMaxOrds = new ArrayList<>();

        @Override
        public SortedSetOrdinalWriter createWriter(final NumericWriteContext ctx) {
            final SortedSetOrdinalWriter writer = delegate.createWriter(ctx);
            return (field, values, maxOrd) -> {
                writtenFields.add(field.name);
                return writer.writeOrdinals(field, values, maxOrd);
            };
        }

        @Override
        public SortedSetOrdinalReader readOrdinalMeta(
            final NumericReadContext ctx,
            final IndexInput meta,
            final SortedNumericEntry entry,
            int numericBlockShift
        ) throws IOException {
            final SortedSetOrdinalReader reader = delegate.readOrdinalMeta(ctx, meta, entry, numericBlockShift);
            readNumValues.add(entry.numValues);
            return (readCtx, maxOrd) -> {
                ordinalsMaxOrds.add(maxOrd);
                return reader.ordinals(readCtx, maxOrd);
            };
        }
    }
}
