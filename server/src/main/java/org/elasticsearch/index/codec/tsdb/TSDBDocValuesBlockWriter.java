/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * Shared write loop for TSDB doc values fields.
 *
 * <p>Both numeric and ordinal fields use the same on-disk block layout: a block index, value
 * blocks encoded by a caller-supplied {@link BlockEncoder}, and a DISI section. This class owns
 * that shared layout so {@link TSDBNumericFieldWriter} and {@link TSDBOrdinalFieldWriter} can
 * delegate to a single implementation without coupling to each other.
 */
final class TSDBDocValuesBlockWriter {

    @FunctionalInterface
    interface BlockEncoder {
        void encode(long[] buffer, IndexOutput data) throws IOException;
    }

    /**
     * Writes one field's value blocks, block index, and DISI metadata.
     *
     * @param ctx                 segment-scoped write state
     * @param field               field being written
     * @param valuesSource        source of doc values for this field
     * @param maxOrd              maximum ordinal for ordinal fields, or
     *                            {@link AbstractTSDBDocValuesConsumer#NO_MAX_ORD} for numeric fields
     * @param docValueCountConsumer receives the per-doc value count for offset tracking,
     *                              or {@code null} when offsets are not needed
     * @param sortedFieldObserver receives {@code (docId, value)} pairs during the doc pass,
     *                            or {@code null} when no observer is attached
     * @param blockEncoder        codec-specific encoder for each value block
     * @return the field's doc value count statistics
     */
    DocValueFieldCountStats writeField(
        final NumericWriteContext ctx,
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        long maxOrd,
        final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        final SortedFieldObserver sortedFieldObserver,
        final BlockEncoder blockEncoder
    ) throws IOException {
        final IndexOutput meta = ctx.meta();
        final IndexOutput data = ctx.data();
        final int blockSize = ctx.blockSize();
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
                // TODO: the three branches below (single ordinal, ordinal range, block layout) are
                // three encoding strategies that could be extracted behind a common delegate interface.
                // Stats gathering and DISI writing are shared; only the value encoding varies.
                if (maxOrd == 1) {
                    meta.writeInt(AbstractTSDBDocValuesConsumer.INDEX_SINGLE_ORDINAL);
                    if (sortedFieldObserver != null) {
                        sortedFieldObserver.onDoc(0, 0);
                    }
                } else if (shouldEncodeOrdinalRange(ctx, field, maxOrd, numDocsWithValue, numValues)) {
                    assert docValueCountConsumer == null;
                    meta.writeInt(AbstractTSDBDocValuesConsumer.INDEX_ORDINAL_RANGE);
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
                    values = valuesSource.getSortedNumeric(field);
                    if (valuesSource.mergeStats.supported() && numDocsWithValue < maxDoc) {
                        disiAccumulator = new DISIAccumulator(ctx.dir(), ctx.ioContext(), data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                    }
                    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                        if (disiAccumulator != null) {
                            disiAccumulator.addDocId(doc);
                        }
                        final int count = values.docValueCount();
                        if (docValueCountConsumer != null) {
                            docValueCountConsumer.accept(count);
                        }
                        for (int i = 0; i < count; ++i) {
                            final long v = values.nextValue();
                            if (sortedFieldObserver != null) {
                                sortedFieldObserver.onDoc(doc, v);
                            }
                            buffer[bufferSize++] = v;
                            if (bufferSize == blockSize) {
                                indexWriter.add(data.getFilePointer() - valuesDataOffset);
                                blockEncoder.encode(buffer, data);
                                bufferSize = 0;
                            }
                        }
                    }
                    if (bufferSize > 0) {
                        indexWriter.add(data.getFilePointer() - valuesDataOffset);
                        Arrays.fill(buffer, bufferSize, blockSize, 0L);
                        blockEncoder.encode(buffer, data);
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

        return new DocValueFieldCountStats(numDocsWithValue, numValues);
    }

    /**
     * Decides between two on-disk layouts for an ordinal field:
     *
     * <ul>
     *   <li><b>Block layout</b> stores every ordinal value in fixed-size blocks with a block
     *       index for seeking. General-purpose but proportional to the number of values.</li>
     *   <li><b>Ordinal range layout</b> stores one {@code startDoc} per ordinal using a
     *       {@link DirectMonotonicWriter}. This works only when documents are sorted by
     *       the field so that each ordinal forms a contiguous run of doc IDs. The reader
     *       binary-searches the {@code startDoc} array to resolve a doc ID to its ordinal.</li>
     * </ul>
     *
     * <p>Ordinal range layout is chosen when the field is the primary sort field (so ordinals
     * are monotonic across docs), every document has exactly one value, and the average run
     * length per ordinal is at least {@code minDocsPerOrdinalForRangeEncoding}.
     *
     * @param ctx              segment-scoped write state
     * @param field            field being written
     * @param maxOrd           maximum ordinal value for this field
     * @param numDocsWithValue number of documents with at least one value
     * @param numValues        total number of values across all documents
     * @return {@code true} if ordinal range layout should be used
     */
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
