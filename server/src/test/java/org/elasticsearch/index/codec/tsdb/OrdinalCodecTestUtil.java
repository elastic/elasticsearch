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
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Scaffolding shared by the unit tests of the ordinal codecs that wrap an {@link OrdinalBlockCodec}: in-memory
 * segment outputs, write and read contexts that select the blocked layout, array-backed doc values sources,
 * and a block codec that records the contexts it receives.
 */
public final class OrdinalCodecTestUtil {

    private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    /** Sentinel the producer/consumer use when the segment has no primary sort field. */
    private static final int NO_PRIMARY_SORT_FIELD = -1;
    /** Marks a document without a value in a single-valued test ordinal array. */
    public static final long MISSING = -1L;

    private OrdinalCodecTestUtil() {}

    /** Output of one field write: the metadata bytes, the data bytes and the returned statistics. */
    public record Written(byte[] meta, byte[] data, DocValueFieldCountStats stats) {}

    /**
     * Runs {@code fieldWriter} against fresh in-memory meta/data outputs and captures what it wrote.
     */
    public static Written write(
        final int blockSize,
        final int maxDoc,
        final CheckedFunction<NumericWriteContext, DocValueFieldCountStats, IOException> fieldWriter
    ) throws IOException {
        return write(null, blockSize, maxDoc, fieldWriter);
    }

    /**
     * As {@link #write(int, int, CheckedFunction)}, with a directory for the temp files of the merge-time
     * accumulators.
     */
    static Written write(
        @Nullable final Directory dir,
        final int blockSize,
        final int maxDoc,
        final CheckedFunction<NumericWriteContext, DocValueFieldCountStats, IOException> fieldWriter
    ) throws IOException {
        return write(dir, blockSize, maxDoc, NO_PRIMARY_SORT_FIELD, fieldWriter);
    }

    /**
     * As {@link #write(int, int, CheckedFunction)}, with the given field number as the segment's primary
     * sort field.
     */
    public static Written write(
        final int blockSize,
        final int maxDoc,
        final int primarySortFieldNumber,
        final CheckedFunction<NumericWriteContext, DocValueFieldCountStats, IOException> fieldWriter
    ) throws IOException {
        return write(null, blockSize, maxDoc, primarySortFieldNumber, fieldWriter);
    }

    private static Written write(
        @Nullable final Directory dir,
        final int blockSize,
        final int maxDoc,
        final int primarySortFieldNumber,
        final CheckedFunction<NumericWriteContext, DocValueFieldCountStats, IOException> fieldWriter
    ) throws IOException {
        final ByteBuffersDataOutput metaOut = new ByteBuffersDataOutput();
        final ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        final DocValueFieldCountStats stats;
        try (
            IndexOutput meta = new ByteBuffersIndexOutput(metaOut, "test-meta", "test-meta");
            IndexOutput data = new ByteBuffersIndexOutput(dataOut, "test-data", "test-data")
        ) {
            final IOContext ioContext = dir == null ? null : IOContext.DEFAULT;
            stats = fieldWriter.apply(
                new NumericWriteContext(meta, data, dir, ioContext, maxDoc, blockSize, primarySortFieldNumber, formatConfig(blockSize))
            );
        }
        return new Written(metaOut.toArrayCopy(), dataOut.toArrayCopy(), stats);
    }

    /**
     * Builds a write context with no primary sort field, so the writer always picks the block layout
     * rather than the ordinal-range layout, and no directory, which only the merge-time accumulators need.
     */
    static NumericWriteContext writeContext(final IndexOutput meta, final IndexOutput data, final int maxDoc, final int blockSize) {
        return writeContext(meta, data, null, maxDoc, blockSize);
    }

    /**
     * As {@link #writeContext(IndexOutput, IndexOutput, int, int)}, with a directory for the temp files of
     * the merge-time accumulators.
     */
    static NumericWriteContext writeContext(
        final IndexOutput meta,
        final IndexOutput data,
        @Nullable final Directory dir,
        final int maxDoc,
        final int blockSize
    ) {
        final IOContext ioContext = dir == null ? null : IOContext.DEFAULT;
        return new NumericWriteContext(meta, data, dir, ioContext, maxDoc, blockSize, NO_PRIMARY_SORT_FIELD, formatConfig(blockSize));
    }

    /** A read context for the current format version and the given block size. */
    public static NumericReadContext readContext(final int blockSize) {
        return new NumericReadContext(blockSize, null, TSDBDocValuesFormatConfig.VERSION_CURRENT);
    }

    /**
     * The ordinal writer only consults the numeric settings and the direct-monotonic block shift; the
     * terms-dict, skip-index and binary sections are irrelevant to the ordinal stream and left unset.
     */
    private static TSDBDocValuesFormatConfig formatConfig(final int blockSize) {
        final int blockShift = Integer.numberOfTrailingZeros(blockSize);
        return new TSDBDocValuesFormatConfig(
            TSDBDocValuesFormatConfig.VERSION_CURRENT,
            null,
            null,
            new TSDBDocValuesFormatConfig.NumericConfig(blockShift, blockShift, Integer.MAX_VALUE),
            null,
            DIRECT_MONOTONIC_BLOCK_SHIFT,
            false
        );
    }

    /** A doc values field of the given type; nothing else about the field matters to the ordinal codecs. */
    public static FieldInfo fieldInfo(final DocValuesType docValuesType) {
        return fieldInfo("test_field", docValuesType);
    }

    /** As {@link #fieldInfo(DocValuesType)}, with the given name and field number 0. */
    public static FieldInfo fieldInfo(final String name, final DocValuesType docValuesType) {
        return new FieldInfo(
            name,
            0,
            false,
            false,
            false,
            IndexOptions.NONE,
            docValuesType,
            DocValuesSkipIndexType.NONE,
            -1,
            Map.of(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    public static int randomBlockSize() {
        return 1 << ESTestCase.randomIntBetween(7, 9);
    }

    /**
     * Wraps {@code ordinals} as the doc values source of a sorted field, where {@link #MISSING} marks a document
     * without a value. The writer iterates the field several times (counting, encoding, DISI), so a fresh
     * iterator is returned on every call.
     */
    public static TsdbDocValuesProducer sortedValuesSource(final long[] ordinals) {
        return new TsdbDocValuesProducer(new DocValuesConsumerUtil.MergeStats(false, 0, 0, 0, 0)) {
            @Override
            public SortedNumericDocValues getSortedNumeric(final FieldInfo field) {
                return DocValues.singleton(new ArrayNumericDocValues(ordinals));
            }
        };
    }

    /**
     * Wraps {@code ordinals} as the doc values source of a sorted-set field, where an empty array marks a
     * document without a value. With {@code mergeStats} the source also carries the value and document counts
     * a merge knows up front, which switches the writer to its single-pass path. The writer iterates the field
     * several times (counting, encoding, DISI, addresses), so a fresh iterator is returned on every call.
     */
    public static TsdbDocValuesProducer sortedSetValuesSource(final long[][] ordinals, final boolean mergeStats) {
        final long numValues = Arrays.stream(ordinals).mapToLong(docOrdinals -> docOrdinals.length).sum();
        final int numDocsWithField = (int) Arrays.stream(ordinals).filter(docOrdinals -> docOrdinals.length > 0).count();
        return new TsdbDocValuesProducer(new DocValuesConsumerUtil.MergeStats(mergeStats, numValues, numDocsWithField, 0, 0)) {
            @Override
            public SortedNumericDocValues getSortedNumeric(final FieldInfo field) {
                return new ArraySortedNumericDocValues(ordinals);
            }
        };
    }

    /**
     * Encodes one random block with the block codec's encoder and decodes it with {@code decoder}: only the
     * block codec's own decoder reads its bit-packed layout back, so a match proves which decoder was handed
     * to the factory.
     */
    static void assertDecodesBlockCodecOutput(final OrdinalFieldReader.Decoder decoder, final int blockSize, final long maxOrd)
        throws IOException {
        final int bitsPerOrd = PackedInts.bitsRequired(maxOrd - 1);
        final long[] ordinals = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            ordinals[i] = ESTestCase.randomLongBetween(0, maxOrd - 1);
        }
        final ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(buffer, "block", "block")) {
            new TSDBOrdinalBlockCodec().createWriter(writeContext(null, null, 0, blockSize))
                .encoder()
                .encodeOrdinals(ordinals.clone(), out, bitsPerOrd);
        }
        final long[] decoded = new long[blockSize];
        decoder.decodeOrdinals(new ByteArrayDataInput(buffer.toArrayCopy()), decoded, bitsPerOrd);
        assertThat(decoded, equalTo(ordinals));
    }

    /**
     * An {@link OrdinalBlockCodec} that behaves exactly like {@link TSDBOrdinalBlockCodec} but records
     * the contexts it is created with. The real block codecs do not expose the context they received,
     * so this is the only way to observe that a wrapping codec forwards it unchanged.
     */
    static final class RecordingOrdinalBlockCodec implements OrdinalBlockCodec {

        private final OrdinalBlockCodec delegate = new TSDBOrdinalBlockCodec();
        final List<NumericWriteContext> writeContexts = new ArrayList<>();
        final List<NumericReadContext> readContexts = new ArrayList<>();

        @Override
        public OrdinalFieldReader createReader(final NumericReadContext ctx) {
            readContexts.add(ctx);
            return delegate.createReader(ctx);
        }

        @Override
        public OrdinalFieldWriter createWriter(final NumericWriteContext ctx) {
            writeContexts.add(ctx);
            return delegate.createWriter(ctx);
        }
    }

    /**
     * Single-valued doc values over an array, where {@link #MISSING} marks a document without a value.
     */
    private static final class ArrayNumericDocValues extends NumericDocValues {

        private final long[] ordinals;
        private int doc = -1;

        ArrayNumericDocValues(final long[] ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public long longValue() {
            return ordinals[doc];
        }

        @Override
        public boolean advanceExact(final int target) {
            doc = target;
            return ordinals[doc] != MISSING;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(final int target) {
            for (doc = target; doc < ordinals.length; doc++) {
                if (ordinals[doc] != MISSING) {
                    return doc;
                }
            }
            doc = NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            return ordinals.length;
        }
    }

    /**
     * Multi-valued doc values over a per-document array, where an empty array marks a document
     * without a value.
     */
    private static final class ArraySortedNumericDocValues extends SortedNumericDocValues {

        private final long[][] ordinals;
        private int doc = -1;
        private int index;

        ArraySortedNumericDocValues(final long[][] ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public long nextValue() {
            return ordinals[doc][index++];
        }

        @Override
        public int docValueCount() {
            return ordinals[doc].length;
        }

        @Override
        public boolean advanceExact(final int target) {
            doc = target;
            index = 0;
            return ordinals[doc].length > 0;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(final int target) {
            index = 0;
            for (doc = target; doc < ordinals.length; doc++) {
                if (ordinals[doc].length > 0) {
                    return doc;
                }
            }
            doc = NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            return ordinals.length;
        }
    }
}
