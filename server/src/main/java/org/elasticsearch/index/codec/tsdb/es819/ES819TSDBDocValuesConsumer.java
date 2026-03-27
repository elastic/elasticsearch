/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.DISIAccumulator;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.NumericFieldWriter;
import org.elasticsearch.index.codec.tsdb.NumericFieldWriter.OffsetsConsumer;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserverFactory;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Doc values consumer for the ES819 TSDB format. Delegates all shared wire-format logic
 * to {@link AbstractTSDBDocValuesConsumer} and provides the ES819-specific numeric
 * encoding strategy via {@link TSDBDocValuesEncoder}.
 */
final class ES819TSDBDocValuesConsumer extends AbstractTSDBDocValuesConsumer {

    ES819TSDBDocValuesConsumer(
        final SegmentWriteState state,
        boolean enableOptimizedMerge,
        final String dataCodec,
        final String dataExtension,
        final String metaCodec,
        final String metaExtension,
        final TSDBDocValuesFormatConfig formatConfig,
        final DocOffsetsCodec.Encoder docOffsetsEncoder,
        final SortedFieldObserverFactory sortedFieldObserverFactory
    ) throws IOException {
        super(
            state,
            enableOptimizedMerge,
            dataCodec,
            dataExtension,
            metaCodec,
            metaExtension,
            formatConfig,
            docOffsetsEncoder,
            sortedFieldObserverFactory
        );
    }

    @Override
    protected NumericFieldWriter createNumericFieldWriter(final NumericWriteContext ctx) {
        return new ES819NumericFieldWriter(ctx);
    }

    private static final class ES819NumericFieldWriter implements NumericFieldWriter {

        private final NumericWriteContext ctx;

        ES819NumericFieldWriter(final NumericWriteContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public Encoder encoder() {
            final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(ctx.blockSize());
            return new Encoder() {
                @Override
                public void encodeBlock(final long[] values, int blockSize, final IndexOutput data) throws IOException {
                    encoder.encode(values, data);
                }

                @Override
                public void encodeOrdinals(final long[] values, final IndexOutput data, int bitsPerOrd) throws IOException {
                    encoder.encodeOrdinals(values, data, bitsPerOrd);
                }
            };
        }

        @Override
        public long[] writeField(
            final FieldInfo field,
            final TsdbDocValuesProducer valuesSource,
            long maxOrd,
            final OffsetsConsumer offsetsConsumer,
            final SortedFieldObserver sortedFieldObserver
        ) throws IOException {
            final IndexOutput meta = ctx.meta();
            final IndexOutput data = ctx.data();
            final int blockSize = ctx.blockSize();
            final Encoder blockEncoder = encoder();
            final int blockShift = Integer.numberOfTrailingZeros(blockSize);
            final int maxDoc = ctx.maxDoc();
            final TSDBDocValuesFormatConfig formatConfig = ctx.formatConfig();

            int numDocsWithValue = 0;
            long numValues = 0;

            SortedNumericDocValues values;
            if (valuesSource.mergeStats.supported()) {
                numDocsWithValue = valuesSource.mergeStats.sumNumDocsWithField();
                numValues = valuesSource.mergeStats.sumNumValues();
            } else {
                values = valuesSource.getSortedNumeric(field);
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    numDocsWithValue++;
                    numValues += values.docValueCount();
                }
            }

            meta.writeLong(numValues);
            meta.writeInt(numDocsWithValue);

            DISIAccumulator disiAccumulator = null;
            try {
                if (numValues > 0) {
                    assert numDocsWithValue > 0;
                    final ByteBuffersDataOutput indexOut = new ByteBuffersDataOutput();
                    DirectMonotonicWriter indexWriter = null;

                    final long valuesDataOffset = data.getFilePointer();
                    if (maxOrd == 1) {
                        meta.writeInt(INDEX_SINGLE_ORDINAL);
                        if (sortedFieldObserver != null) {
                            sortedFieldObserver.onDoc(0, 0);
                        }
                    } else if (shouldEncodeOrdinalRange(ctx, field, maxOrd, numDocsWithValue, numValues)) {
                        assert offsetsConsumer == null;
                        meta.writeInt(INDEX_ORDINAL_RANGE);
                        meta.writeVInt(Math.toIntExact(maxOrd));
                        meta.writeByte((byte) formatConfig.ordinalRangeBlockShift());
                        values = valuesSource.getSortedNumeric(field);
                        if (valuesSource.mergeStats.supported() && numDocsWithValue < maxDoc) {
                            disiAccumulator = new DISIAccumulator(ctx.dir(), ctx.ioContext(), data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                        }
                        final DirectMonotonicWriter startDocs = DirectMonotonicWriter.getInstance(
                            meta,
                            data,
                            maxOrd + 1,
                            formatConfig.ordinalRangeBlockShift()
                        );
                        long lastOrd = 0;
                        startDocs.add(0);
                        if (sortedFieldObserver != null) {
                            sortedFieldObserver.onDoc(0, lastOrd);
                        }
                        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                            if (disiAccumulator != null) {
                                disiAccumulator.addDocId(doc);
                            }
                            final long nextOrd = values.nextValue();
                            if (nextOrd != lastOrd) {
                                lastOrd = nextOrd;
                                startDocs.add(doc);
                                if (sortedFieldObserver != null) {
                                    sortedFieldObserver.onDoc(doc, nextOrd);
                                }
                            }
                        }
                        startDocs.add(maxDoc);
                        startDocs.finish();
                    } else {
                        indexWriter = DirectMonotonicWriter.getInstance(
                            meta,
                            new ByteBuffersIndexOutput(indexOut, "temp-dv-index", "temp-dv-index"),
                            1L + ((numValues - 1) >>> blockShift),
                            formatConfig.directMonotonicBlockShift()
                        );
                        meta.writeInt(formatConfig.directMonotonicBlockShift());
                        final long[] buffer = new long[blockSize];
                        int bufferSize = 0;
                        final boolean useOrdinals = maxOrd >= 0;
                        values = valuesSource.getSortedNumeric(field);
                        final int bitsPerOrd = useOrdinals ? PackedInts.bitsRequired(maxOrd - 1) : -1;
                        if (valuesSource.mergeStats.supported() && numDocsWithValue < maxDoc) {
                            disiAccumulator = new DISIAccumulator(ctx.dir(), ctx.ioContext(), data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                        }
                        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                            if (disiAccumulator != null) {
                                disiAccumulator.addDocId(doc);
                            }
                            final int count = values.docValueCount();
                            if (offsetsConsumer != null) {
                                offsetsConsumer.accept(count);
                            }
                            for (int i = 0; i < count; ++i) {
                                final long v = values.nextValue();
                                if (sortedFieldObserver != null) {
                                    sortedFieldObserver.onDoc(doc, v);
                                }
                                buffer[bufferSize++] = v;
                                if (bufferSize == blockSize) {
                                    indexWriter.add(data.getFilePointer() - valuesDataOffset);
                                    if (useOrdinals) {
                                        blockEncoder.encodeOrdinals(buffer, data, bitsPerOrd);
                                    } else {
                                        blockEncoder.encodeBlock(buffer, blockSize, data);
                                    }
                                    bufferSize = 0;
                                }
                            }
                        }
                        if (bufferSize > 0) {
                            indexWriter.add(data.getFilePointer() - valuesDataOffset);
                            Arrays.fill(buffer, bufferSize, blockSize, 0L);
                            if (useOrdinals) {
                                blockEncoder.encodeOrdinals(buffer, data, bitsPerOrd);
                            } else {
                                blockEncoder.encodeBlock(buffer, blockSize, data);
                            }
                        }
                    }

                    final long valuesDataLength = data.getFilePointer() - valuesDataOffset;
                    if (indexWriter != null) {
                        indexWriter.finish();
                    }
                    final long indexDataOffset = data.getFilePointer();
                    data.copyBytes(indexOut.toDataInput(), indexOut.size());
                    meta.writeLong(indexDataOffset);
                    meta.writeLong(data.getFilePointer() - indexDataOffset);

                    meta.writeLong(valuesDataOffset);
                    meta.writeLong(valuesDataLength);
                }

                writeDISI(meta, data, ctx, valuesSource, field, maxOrd, numDocsWithValue, disiAccumulator);
            } finally {
                IOUtils.close(disiAccumulator);
            }

            return new long[] { numDocsWithValue, numValues };
        }

        private static boolean shouldEncodeOrdinalRange(
            final NumericWriteContext ctx,
            final FieldInfo field,
            long maxOrd,
            int numDocsWithValue,
            long numValues
        ) {
            return ctx.maxDoc() > 1
                && field.number == ctx.primarySortFieldNumber()
                && numDocsWithValue == numValues
                && (numDocsWithValue / maxOrd) >= ctx.formatConfig().minDocsPerOrdinalForRangeEncoding();
        }

        private static void writeDISI(
            final IndexOutput meta,
            final IndexOutput data,
            final NumericWriteContext ctx,
            final TsdbDocValuesProducer valuesSource,
            final FieldInfo field,
            long maxOrd,
            int numDocsWithValue,
            final DISIAccumulator disiAccumulator
        ) throws IOException {
            if (numDocsWithValue == 0) {
                meta.writeLong(-2);
                meta.writeLong(0L);
                meta.writeShort((short) -1);
                meta.writeByte((byte) -1);
            } else if (numDocsWithValue == ctx.maxDoc()) {
                meta.writeLong(-1);
                meta.writeLong(0L);
                meta.writeShort((short) -1);
                meta.writeByte((byte) -1);
            } else {
                long offset = data.getFilePointer();
                meta.writeLong(offset);
                final short jumpTableEntryCount;
                if (maxOrd != 1 && disiAccumulator != null) {
                    jumpTableEntryCount = disiAccumulator.build(data);
                } else {
                    final SortedNumericDocValues values = valuesSource.getSortedNumeric(field);
                    jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }
                meta.writeLong(data.getFilePointer() - offset);
                meta.writeShort(jumpTableEntryCount);
                meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
            }
        }
    }
}
