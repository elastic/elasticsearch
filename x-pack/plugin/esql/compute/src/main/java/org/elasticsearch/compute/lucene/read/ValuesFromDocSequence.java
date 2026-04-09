/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.LeafReaderContext;
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
 */
class ValuesFromDocSequence extends ValuesReader {
    private static final Logger log = LogManager.getLogger(ValuesFromDocSequence.class);

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
            if (log.isDebugEnabled()) {
                long actualBytes = 0;
                for (Block b : target) {
                    actualBytes += b.ramBytesUsed();
                }
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
