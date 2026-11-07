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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
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
public final class TSDBDocValuesBlockWriter {

    /** Encodes one block of values into the data output. */
    @FunctionalInterface
    public interface BlockEncoder {
        void encode(long[] buffer, IndexOutput data) throws IOException;
    }

    /**
     * Optional callback invoked after the block-shift marker is written to metadata but before
     * the block encoding loop begins. Codec-specific formats (e.g. ES95) use this to write
     * additional per-field metadata such as a {@link org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor}.
     */
    @FunctionalInterface
    public interface FieldMetaWriter {
        void write() throws IOException;
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
     * @param blockSize           number of values per block for this field; must be a
     *                            positive power of two. ES95 passes the resolved
     *                            {@code PipelineConfig.blockSize()}, while ES819 numeric
     *                            and ordinal writers pass {@code ctx.blockSize()}
     * @return the field's doc value count statistics
     */
    public DocValueFieldCountStats writeFieldEntry(
        final NumericWriteContext ctx,
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        long maxOrd,
        final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        final SortedFieldObserver sortedFieldObserver,
        final BlockEncoder blockEncoder,
        int blockSize
    ) throws IOException {
        return writeFieldEntry(
            ctx,
            field,
            valuesSource,
            maxOrd,
            docValueCountConsumer,
            sortedFieldObserver,
            null,
            blockEncoder,
            null,
            blockSize
        );
    }

    /**
     * Writes one field's value blocks, block index, and DISI metadata with an optional
     * metadata header hook.
     *
     * <p>Numeric fields are written in a single pass over the values: the value counts, the
     * block index and anything attached through {@code valuesObserver} are all collected while
     * the blocks are encoded, and the field's metadata is written afterwards. Ordinal fields
     * still count first, because the choice between the three ordinal layouts depends on the
     * counts.
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
     * @param valuesObserver      receives every document and value during the single encode
     *                            pass of a numeric field, or {@code null}; its
     *                            {@link NumericValuesObserver#finish()} runs before this
     *                            field's metadata is written. Must be {@code null} for ordinal
     *                            fields
     * @param blockEncoder        codec-specific encoder for each value block
     * @param fieldMetaWriter    optional callback invoked after the block-shift marker to write
     *                            additional per-field metadata, or {@code null}
     * @param blockSize           number of values per block for this field; must be a
     *                            positive power of two. ES95 passes the resolved
     *                            {@code PipelineConfig.blockSize()}, while ES819 numeric
     *                            and ordinal writers pass {@code ctx.blockSize()}
     * @return the field's doc value count statistics
     */
    public DocValueFieldCountStats writeFieldEntry(
        final NumericWriteContext ctx,
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        long maxOrd,
        final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        final SortedFieldObserver sortedFieldObserver,
        final NumericValuesObserver valuesObserver,
        final BlockEncoder blockEncoder,
        final FieldMetaWriter fieldMetaWriter,
        int blockSize
    ) throws IOException {
        assert blockSize > 0 && (blockSize & (blockSize - 1)) == 0 : "blockSize must be a positive power of two, got " + blockSize;
        assert valuesObserver == null || maxOrd == AbstractTSDBDocValuesConsumer.NO_MAX_ORD
            : "a values observer is only supported for numeric fields";
        final IndexOutput meta = ctx.meta();
        final IndexOutput data = ctx.data();
        final int blockShift = Integer.numberOfTrailingZeros(blockSize);
        final int maxDoc = ctx.maxDoc();
        final TSDBDocValuesFormatConfig formatConfig = ctx.formatConfig();

        int numDocsWithValue = 0;
        long numValues = 0;
        // Ordinal fields need the counts up front to choose a layout. Numeric fields always use the
        // block layout, so on flush (no merge stats) they pick the counts up during the encode pass
        // instead of paying for a counting pass over the values.
        boolean countsKnown = false;
        if (valuesSource.mergeStats.supported()) {
            numDocsWithValue = valuesSource.mergeStats.sumNumDocsWithField();
            numValues = valuesSource.mergeStats.sumNumValues();
            countsKnown = true;
        } else if (maxOrd != AbstractTSDBDocValuesConsumer.NO_MAX_ORD) {
            final SortedNumericDocValues values = valuesSource.getSortedNumeric(field);
            final NumericDocValues singleton = DocValues.unwrapSingleton(values);
            if (singleton != null) {
                for (int doc = singleton.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = singleton.nextDoc()) {
                    numDocsWithValue++;
                }
                numValues = numDocsWithValue;
            } else {
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    numDocsWithValue++;
                    numValues += values.docValueCount();
                }
            }
            countsKnown = true;
        }

        DISIAccumulator disiAccumulator = null;
        try {
            // TODO: the three branches below (single ordinal, ordinal range, block layout) are
            // three encoding strategies that could be extracted behind a common delegate interface.
            // Stats gathering and DISI writing are shared; only the value encoding varies.
            if (countsKnown && numValues == 0) {
                assert numDocsWithValue == 0;
                writeCounts(meta, numValues, numDocsWithValue);
            } else if (countsKnown && maxOrd == 1) {
                writeCounts(meta, numValues, numDocsWithValue);
                final long valuesDataOffset = data.getFilePointer();
                meta.writeInt(AbstractTSDBDocValuesConsumer.INDEX_SINGLE_ORDINAL);
                if (sortedFieldObserver != null) {
                    sortedFieldObserver.onDoc(0, 0);
                }
                writeOffsets(meta, data, valuesDataOffset, data.getFilePointer());
            } else if (countsKnown && shouldEncodeOrdinalRange(ctx, field, maxOrd, numDocsWithValue, numValues)) {
                assert docValueCountConsumer == null;
                writeCounts(meta, numValues, numDocsWithValue);
                final long valuesDataOffset = data.getFilePointer();
                meta.writeInt(AbstractTSDBDocValuesConsumer.INDEX_ORDINAL_RANGE);
                meta.writeVInt(Math.toIntExact(maxOrd));
                meta.writeByte((byte) formatConfig.ordinalRangeBlockShift());
                final SortedNumericDocValues values = valuesSource.getSortedNumeric(field);
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
                writeOffsets(meta, data, valuesDataOffset, data.getFilePointer());
            } else {
                // Block layout. Everything that goes to meta is written after the pass: the counts
                // may still be unknown, and a values observer may have metadata of its own that the
                // reader expects before this field's entry. Block start offsets are therefore kept
                // in memory (8 bytes per block of blockSize values) and fed to the block index
                // writer once the values are out.
                final SortedNumericDocValues values = valuesSource.getSortedNumeric(field);
                if (valuesSource.mergeStats.supported() && numDocsWithValue < maxDoc) {
                    disiAccumulator = new DISIAccumulator(ctx.dir(), ctx.ioContext(), data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }
                final int expectedBlocks = countsKnown ? Math.toIntExact(1L + ((numValues - 1) >>> blockShift)) : 16;
                final BlockPass pass = new BlockPass(data, blockEncoder, blockSize, expectedBlocks);
                // Iterate a single-valued field directly: through the wrapper, the call to the underlying iterator is
                // shared with every other user of the wrapper in the JVM and ends up megamorphic.
                final NumericDocValues singleton = DocValues.unwrapSingleton(values);
                if (singleton != null) {
                    pass.writeSingleValued(singleton, disiAccumulator, docValueCountConsumer, sortedFieldObserver, valuesObserver);
                } else {
                    pass.writeMultiValued(values, disiAccumulator, docValueCountConsumer, sortedFieldObserver, valuesObserver);
                }
                pass.finish();
                if (countsKnown) {
                    assert pass.numDocsWithValue == numDocsWithValue : pass.numDocsWithValue + " != " + numDocsWithValue;
                    assert pass.numValues == numValues : pass.numValues + " != " + numValues;
                } else {
                    numDocsWithValue = pass.numDocsWithValue;
                    numValues = pass.numValues;
                }
                assert numValues == 0 ? pass.numBlocks == 0 : pass.numBlocks == 1L + ((numValues - 1) >>> blockShift);

                if (valuesObserver != null) {
                    valuesObserver.finish();
                }
                writeCounts(meta, numValues, numDocsWithValue);
                if (numValues > 0) {
                    assert numDocsWithValue > 0;
                    meta.writeInt(formatConfig.directMonotonicBlockShift());
                    if (fieldMetaWriter != null) {
                        fieldMetaWriter.write();
                    }
                    final long indexDataOffset = data.getFilePointer();
                    final DirectMonotonicWriter indexWriter = DirectMonotonicWriter.getInstance(
                        meta,
                        data,
                        pass.numBlocks,
                        formatConfig.directMonotonicBlockShift()
                    );
                    for (int i = 0; i < pass.numBlocks; i++) {
                        indexWriter.add(pass.blockOffsets[i]);
                    }
                    indexWriter.finish();
                    meta.writeLong(indexDataOffset);
                    meta.writeLong(data.getFilePointer() - indexDataOffset);
                    meta.writeLong(pass.valuesDataOffset);
                    meta.writeLong(pass.valuesDataEnd - pass.valuesDataOffset);
                }
            }

            writeDISI(meta, data, ctx, valuesSource, field, maxOrd, numDocsWithValue, disiAccumulator);
        } finally {
            IOUtils.close(disiAccumulator);
        }

        return new DocValueFieldCountStats(numDocsWithValue, numValues);
    }

    private static void writeCounts(final IndexOutput meta, long numValues, int numDocsWithValue) throws IOException {
        meta.writeLong(numValues);
        meta.writeInt(numDocsWithValue);
    }

    /**
     * Writes the (empty) block index location and the values location for the two ordinal
     * layouts that have no block index, keeping the metadata shape identical to the block layout.
     */
    private static void writeOffsets(final IndexOutput meta, final IndexOutput data, long valuesDataOffset, long valuesDataEnd)
        throws IOException {
        meta.writeLong(data.getFilePointer()); // index data offset
        meta.writeLong(0L); // index data length
        meta.writeLong(valuesDataOffset);
        meta.writeLong(valuesDataEnd - valuesDataOffset);
    }

    /**
     * One block-layout pass over a field's values: fills and encodes blocks, remembers where each
     * block starts (8 bytes per block, so the block index can be written after the pass) and
     * counts documents and values.
     *
     * <p>The two iteration loops are deliberately kept in small methods of their own rather than
     * inlined into {@link #writeFieldEntry}: that method is large, and when the loops lived there
     * C2 stopped inlining the per-value iterator calls, which cost more than the pass that this
     * class saves.
     */
    private static final class BlockPass {
        private final IndexOutput data;
        private final BlockEncoder blockEncoder;
        private final long[] buffer;
        private int bufferSize;
        final long valuesDataOffset;
        long valuesDataEnd;
        long[] blockOffsets;
        int numBlocks;
        int numDocsWithValue;
        long numValues;

        BlockPass(final IndexOutput data, final BlockEncoder blockEncoder, int blockSize, int expectedBlocks) {
            this.data = data;
            this.blockEncoder = blockEncoder;
            this.buffer = new long[blockSize];
            this.valuesDataOffset = data.getFilePointer();
            this.blockOffsets = new long[expectedBlocks];
        }

        void writeSingleValued(
            final NumericDocValues values,
            final DISIAccumulator disiAccumulator,
            final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
            final SortedFieldObserver sortedFieldObserver,
            final NumericValuesObserver valuesObserver
        ) throws IOException {
            // Per-value state lives in locals: C2 reloads fields on every iteration, which is
            // measurable at this call frequency.
            final long[] buffer = this.buffer;
            int bufferSize = this.bufferSize;
            int docs = 0;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                if (disiAccumulator != null) {
                    disiAccumulator.addDocId(doc);
                }
                if (docValueCountConsumer != null) {
                    docValueCountConsumer.accept(1);
                }
                final long v = values.longValue();
                if (sortedFieldObserver != null) {
                    sortedFieldObserver.onDoc(doc, v);
                }
                if (valuesObserver != null) {
                    valuesObserver.startDoc(doc, 1, v);
                }
                docs++;
                buffer[bufferSize++] = v;
                if (bufferSize == buffer.length) {
                    encodeBlock();
                    bufferSize = 0;
                }
            }
            this.bufferSize = bufferSize;
            numDocsWithValue = docs;
            numValues = docs;
        }

        void writeMultiValued(
            final SortedNumericDocValues values,
            final DISIAccumulator disiAccumulator,
            final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
            final SortedFieldObserver sortedFieldObserver,
            final NumericValuesObserver valuesObserver
        ) throws IOException {
            final long[] buffer = this.buffer;
            int bufferSize = this.bufferSize;
            int docs = 0;
            long vals = 0;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                if (disiAccumulator != null) {
                    disiAccumulator.addDocId(doc);
                }
                final int count = values.docValueCount();
                if (docValueCountConsumer != null) {
                    docValueCountConsumer.accept(count);
                }
                docs++;
                vals += count;
                // The first value is peeled out of the inner loop so the observer sees the
                // document boundary without a branch per value.
                long v = values.nextValue();
                if (sortedFieldObserver != null) {
                    sortedFieldObserver.onDoc(doc, v);
                }
                if (valuesObserver != null) {
                    valuesObserver.startDoc(doc, count, v);
                }
                buffer[bufferSize++] = v;
                if (bufferSize == buffer.length) {
                    encodeBlock();
                    bufferSize = 0;
                }
                for (int i = 1; i < count; ++i) {
                    v = values.nextValue();
                    if (sortedFieldObserver != null) {
                        sortedFieldObserver.onDoc(doc, v);
                    }
                    if (valuesObserver != null) {
                        valuesObserver.addValue(v);
                    }
                    buffer[bufferSize++] = v;
                    if (bufferSize == buffer.length) {
                        encodeBlock();
                        bufferSize = 0;
                    }
                }
            }
            this.bufferSize = bufferSize;
            numDocsWithValue = docs;
            numValues = vals;
        }

        /** Encodes the trailing partial block, if any, and records where the values end. */
        void finish() throws IOException {
            if (bufferSize > 0) {
                Arrays.fill(buffer, bufferSize, buffer.length, 0L);
                encodeBlock();
            }
            valuesDataEnd = data.getFilePointer();
        }

        private void encodeBlock() throws IOException {
            if (numBlocks == blockOffsets.length) {
                blockOffsets = ArrayUtil.grow(blockOffsets, numBlocks + 1);
            }
            blockOffsets[numBlocks++] = data.getFilePointer() - valuesDataOffset;
            blockEncoder.encode(buffer, data);
        }
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
            final long offset = data.getFilePointer();
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
