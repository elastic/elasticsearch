/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;

/**
 * Loads values by iterating in original document sequence order instead of
 * the sorted {@code (shard, segment, doc)} order used by {@link ValuesFromManyReader}.
 * This avoids the backwards reorder and supports partial pages bounded by
 * {@link ValuesSourceReaderOperator#jumboBytes}.
 * <p>
 *     Pages may span multiple shards and segments. Whenever the shard, segment,
 *     or doc order changes, {@link ValuesSourceReaderOperator#positionFieldWork}
 *     invalidates non-reusable readers and {@link Run#fieldsMoved} refreshes the
 *     stored-field loader for the new context.
 * </p>
 * <p>
 *     Partial pages may produce single-position blocks whose memory is freed by
 *     downstream operators between pages. To prevent the circuit breaker from
 *     losing track of cumulative output, a reservation equal to each partial
 *     page's block size is held on the breaker (see {@link #docSequenceReservation}).
 *     This reservation accumulates across partial pages, ensuring the breaker
 *     trips when the total data produced exceeds its limit.
 * </p>
 */
class ValuesFromDocSequence extends ValuesReader {
    private static final Logger log = LogManager.getLogger(ValuesFromDocSequence.class);

    /**
     * Cumulative circuit breaker reservation held across partial pages produced
     * by {@link DocSequenceRun#load}. Each partial page's block data is tracked on the breaker
     * while the blocks exist, but downstream operators release them before the next
     * partial page is produced. This reservation keeps the breaker aware of the total
     * data produced so it can trip when the accumulated output exceeds the limit.
     */
    private long docSequenceReservation;

    ValuesFromDocSequence(ValuesSourceReaderOperator operator, DocVector docs) {
        super(operator, docs);
        log.debug("initializing {} positions", docs.getPositionCount());
    }

    @Override
    protected void load(Block[] target, int offset) throws IOException {
        try (DocSequenceRun run = new DocSequenceRun(target)) {
            run.load(offset);
        }
    }

    @Override
    public void close() {
        if (docSequenceReservation > 0) {
            operator.driverContext.blockFactory().breaker().addWithoutBreaking(-docSequenceReservation);
            docSequenceReservation = 0;
        }
    }

    class DocSequenceRun extends ValuesReader.Run {
        DocSequenceRun(Block[] target) {
            super(target);
        }

        void load(int offset) throws IOException {
            initFinalBuilders(offset);
            int shard = docs.shards().getInt(offset);
            int segment = docs.segments().getInt(offset);
            int firstDoc = docs.docs().getInt(offset);
            operator.positionFieldWork(shard, segment, firstDoc);
            LeafReaderContext ctx = operator.ctx(shard, segment);
            fieldsMoved(ctx, shard);
            int runStart = offset;
            readRowStride(firstDoc);
            int prevDoc = firstDoc;
            int i = offset + 1;
            long estimated = estimatedRamBytesUsed();
            while (i < docs.getPositionCount() && estimated < operator.jumboBytes) {
                int newShard = docs.shards().getInt(i);
                int newSegment = docs.segments().getInt(i);
                int doc = docs.docs().getInt(i);
                if (newShard != shard || newSegment != segment || doc < prevDoc) {
                    readColumnAtATimeBatch(runStart, i);
                    shard = newShard;
                    segment = newSegment;
                    operator.positionFieldWork(shard, segment, doc);
                    ctx = operator.ctx(shard, segment);
                    fieldsMoved(ctx, shard);
                    runStart = i;
                }
                readRowStride(doc);
                prevDoc = doc;
                i++;
                estimated = estimatedRamBytesUsed();
                log.trace("{}: bytes loaded {}/{}", i, estimated, operator.jumboBytes);
            }
            readColumnAtATimeBatch(runStart, i);
            int count = i - offset;
            buildBlocks(count);
            long actualBytes = 0;
            for (Block b : target) {
                actualBytes += b.ramBytesUsed();
            }
            if (count < docs.getPositionCount() - offset) {
                CircuitBreaker breaker = operator.driverContext.blockFactory().breaker();
                breaker.addEstimateBytesAndMaybeBreak(actualBytes, "loadDocSequence");
                docSequenceReservation += actualBytes;
            }
            if (log.isDebugEnabled()) {
                log.debug("loaded {} positions doc sequence estimated/actual {}/{} bytes", count, estimated, actualBytes);
            }
        }

        /**
         * Batch-reads column-at-a-time fields for an ascending run of positions {@code [start, end)}.
         * Within this range doc IDs are non-decreasing, so the underlying doc values iterators
         * can advance forward without needing a reset.
         */
        private void readColumnAtATimeBatch(int start, int end) throws IOException {
            if (columnAtATime.isEmpty() || start >= end) {
                return;
            }
            ValuesReaderDocs readerDocs = new ValuesReaderDocs(docs);
            readerDocs.setCount(end);
            for (CurrentWork c : columnAtATime) {
                assert c.rowStride == null;
                try (Block read = (Block) c.columnAtATime.read(blockFactory, readerDocs, start, c.field.info.nullsFiltered())) {
                    assert read.getPositionCount() == end - start : read.getPositionCount() + " == " + end + " - " + start + " " + read;
                    c.builder.copyFrom(read, 0, read.getPositionCount());
                }
            }
        }

        /**
         * Builds blocks directly from the builders without reordering.
         */
        private void buildBlocks(int count) {
            convertAndAccumulate();
            for (int f = 0; f < target.length; f++) {
                target[f] = finalBuilders[f].build();
                assert target[f].getPositionCount() == count : target[f].getPositionCount() + " == " + count + " " + target[f];
                operator.sanityCheckBlock(current[f].rowStride, count, target[f], f);
            }
        }
    }
}
