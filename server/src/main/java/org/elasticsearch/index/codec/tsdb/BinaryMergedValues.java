/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Merged view of a binary doc values field across the segments of a merge, which additionally reports when
 * the values it is about to produce are exactly the contents of one compressed block of a source segment.
 * When that happens the caller can copy that block's bytes verbatim instead of decompressing every value and
 * compressing it again.
 *
 * <p>Plain iteration behaves like Lucene's merged binary doc values: a {@link DocIDMerger} yields the subs'
 * documents in target doc id order, interleaving segments as the index sort requires. The splice opportunity
 * is then recognised with a single doc map lookup per block, see {@link #blockCopyCandidate()}.
 */
final class BinaryMergedValues extends BinaryDocValues {

    /** Sentinel for a sub that has no further block boundaries to offer. */
    private static final int NO_MORE_BLOCKS = Integer.MAX_VALUE;

    /** Returned by {@link #blockCopyCandidate()} when the current position is not a spliceable block start. */
    static final int NO_BLOCK = -1;

    private final DocIDMerger<Sub> docIDMerger;
    private final long cost;

    private Sub current;
    private int docID = -1;

    private BinaryMergedValues(DocIDMerger<Sub> docIDMerger, long cost) {
        this.docIDMerger = docIDMerger;
        this.cost = cost;
    }

    /**
     * Builds the merged view. A sub only offers blocks for splicing when its segment stores them in a layout
     * the target can consume unchanged, which {@link #spliceableSource} decides; every other sub still takes
     * part in the merge, just value by value.
     */
    static BinaryMergedValues create(
        FieldInfo mergeFieldInfo,
        MergeState mergeState,
        BinaryDVCompressionMode targetCompression,
        DocOffsetsCodec targetDocOffsetsCodec
    ) throws IOException {
        final List<Sub> subs = new ArrayList<>();
        long cost = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            BinaryDocValues values = null;
            BinaryBlockSource blockSource = null;
            final DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
                final FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                    values = docValuesProducer.getBinary(readerFieldInfo);
                    blockSource = spliceableSource(docValuesProducer, readerFieldInfo, targetCompression, targetDocOffsetsCodec);
                }
            }
            if (values != null) {
                cost += values.cost();
                subs.add(new Sub(mergeState.docMaps[i], values, blockSource));
            }
        }
        return new BinaryMergedValues(DocIDMerger.of(subs, mergeState.needsIndexSort), cost);
    }

    /**
     * The source segment's blocks for this field when they can be spliced into a target with the given
     * compression and doc offsets encoding, or {@code null} when they cannot and values must be read
     * one at a time instead.
     *
     * <p>Neither the doc offsets encoding nor the field-level compression mode is recorded inside a block, so
     * blocks only travel between segments that agree on both. A splice also reads bytes straight out of the
     * source file, so it has to be sure no wrapper in the producer chain rewrites binary values on the way out.
     */
    private static BinaryBlockSource spliceableSource(
        DocValuesProducer docValuesProducer,
        FieldInfo fieldInfo,
        BinaryDVCompressionMode targetCompression,
        DocOffsetsCodec targetDocOffsetsCodec
    ) throws IOException {
        if (DocValuesConsumerUtil.binaryValuesPassThroughUnchanged(docValuesProducer) == false) {
            return null;
        }
        final var perFieldReader = DocValuesConsumerUtil.perFieldReader(docValuesProducer);
        if (perFieldReader == null) {
            return null;
        }
        if (perFieldReader.getDocValuesProducer(fieldInfo) instanceof AbstractTSDBDocValuesProducer tsdbDocValuesProducer) {
            final BinaryBlockSource source = tsdbDocValuesProducer.binaryBlockSource(fieldInfo);
            if (source != null && source.compression == targetCompression && source.docOffsetsCodec == targetDocOffsetsCodec) {
                return source;
            }
        }
        return null;
    }

    /**
     * The source block that begins at the current document and whose documents all land consecutively in the
     * merged output, or {@link #NO_BLOCK} when the current document does not start such a block.
     *
     * <p>Cheap enough to call for every document: a sub's documents arrive in increasing source doc id order, so
     * recognising a block boundary is one comparison against a cached value, and the contiguity test is two doc
     * map lookups made once per block rather than once per document.
     */
    int blockCopyCandidate() {
        final Sub sub = current;
        if (sub == null || sub.nextBlockFirstDoc == NO_MORE_BLOCKS) {
            return NO_BLOCK;
        }
        // The field is dense in a sub that offers blocks, so a source doc id is also its index into the value stream.
        final int sourceDoc = sub.values.docID();
        while (sourceDoc > sub.nextBlockFirstDoc) {
            // We wrote this block's documents one at a time rather than splicing it; it is behind us now.
            sub.advanceBlock();
            if (sub.nextBlockFirstDoc == NO_MORE_BLOCKS) {
                return NO_BLOCK;
            }
        }
        if (sourceDoc != sub.nextBlockFirstDoc) {
            return NO_BLOCK;
        }
        final int numDocs = sub.blockSource.numDocs(sub.nextBlock);
        // A block's documents are contiguous in the source. A merge doc map is strictly increasing within a
        // segment and no segment on this path has deletions, so those documents stay contiguous in the target
        // exactly when the last one lands numDocs - 1 slots after the first. When it does, they also occupy that
        // whole target range, which means no other segment interleaves into the block and the block's values are
        // consecutive in the merged value stream.
        if (sub.docMap.get(sourceDoc + numDocs - 1) != sub.docMap.get(sourceDoc) + numDocs - 1) {
            return NO_BLOCK;
        }
        return sub.nextBlock;
    }

    /** The blocks of the segment the current document came from. Only meaningful after a positive {@link #blockCopyCandidate()}. */
    BinaryBlockSource currentBlockSource() {
        return current.blockSource;
    }

    /**
     * Advances past every document of a spliced block, reporting each one's target doc id to {@code disiAccumulator}
     * without decoding a single value, and returns the next document in merged order.
     */
    int consumeBlock(int block, DISIAccumulator disiAccumulator) throws IOException {
        final Sub sub = current;
        final int numDocs = sub.blockSource.numDocs(block);
        assert sub.values.docID() == sub.blockSource.firstValueIndex(block);
        for (int i = 0; i < numDocs; i++) {
            if (i > 0) {
                current = docIDMerger.next();
                assert current == sub : "documents of a spliced block must all come from the same segment";
                assert sub.values.docID() == sub.blockSource.firstValueIndex(block) + i;
            }
            if (disiAccumulator != null) {
                disiAccumulator.addDocId(current.mappedDocID);
            }
        }
        sub.advanceBlock();
        return nextDoc();
    }

    @Override
    public int nextDoc() throws IOException {
        current = docIDMerger.next();
        docID = current == null ? NO_MORE_DOCS : current.mappedDocID;
        return docID;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return current.values.binaryValue();
    }

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int advance(int target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        return cost;
    }

    private static final class Sub extends DocIDMerger.Sub {

        final BinaryDocValues values;
        /** Kept alongside the one {@link DocIDMerger.Sub} holds, which is not visible from this package. */
        final MergeState.DocMap docMap;
        /** The segment's blocks, or {@code null} when this sub's values can only be merged one at a time. */
        final BinaryBlockSource blockSource;

        int nextBlock;
        /** First source doc of {@link #nextBlock}, cached so a block boundary costs one comparison per document. */
        int nextBlockFirstDoc;

        Sub(MergeState.DocMap docMap, BinaryDocValues values, BinaryBlockSource blockSource) {
            super(docMap);
            this.docMap = docMap;
            this.values = values;
            this.blockSource = blockSource;
            this.nextBlockFirstDoc = blockSource == null ? NO_MORE_BLOCKS : blockSource.firstValueIndex(0);
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }

        void advanceBlock() {
            nextBlock++;
            nextBlockFirstDoc = nextBlock < blockSource.numBlocks ? blockSource.firstValueIndex(nextBlock) : NO_MORE_BLOCKS;
        }
    }
}
