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
 * Loads values from a many leaves. Much less efficient than {@link ValuesFromSingleReader}.
 */
class ValuesFromManyReader extends ValuesReader {
    private static final Logger log = LogManager.getLogger(ValuesFromManyReader.class);

    private final int[] forwards;
    private final int[] backwards;

    ValuesFromManyReader(ValuesSourceReaderOperator operator, DocVector docs) {
        super(operator, docs);
        forwards = docs.shardSegmentDocMapForwards();
        backwards = docs.shardSegmentDocMapBackwards();
        log.debug("initializing {} positions", docs.getPositionCount());
    }

    @Override
    protected void load(Block[] target, int offset) throws IOException {
        try (ForwardSequenceRun run = new ForwardSequenceRun(target)) {
            run.run(offset);
        }
    }

    class ForwardSequenceRun extends ValuesReader.Run {
        ForwardSequenceRun(Block[] target) {
            super(target);
        }

        void run(int offset) throws IOException {
            initFinalBuilders(offset);
            if (log.isDebugEnabled()) {
                log.debug("load according to forward sequence");
            }
            loadForwardSequence(offset);
        }

        /**
         * General path that iterates in forwards (shard/segment/doc sorted) order, handling
         * multiple shards/segments and column-at-a-time readers. Always loads the full page.
         */
        private void loadForwardSequence(int offset) throws IOException {
            assert offset == 0; // TODO allow non-0 offset to support splitting pages
            int p = forwards[offset];
            int shard = docs.shards().getInt(p);
            int segment = docs.segments().getInt(p);
            int firstDoc = docs.docs().getInt(p);
            operator.positionFieldWork(shard, segment, firstDoc);
            LeafReaderContext ctx = operator.ctx(shard, segment);
            fieldsMoved(ctx, shard);
            readRowStride(firstDoc);

            int segmentStart = offset;
            int i = offset + 1;
            long estimated = estimatedRamBytesUsed();
            long dangerZoneBytes = Long.MAX_VALUE; // TODO danger_zone if ascending
            while (i < forwards.length && estimated < dangerZoneBytes) {
                p = forwards[i];
                shard = docs.shards().getInt(p);
                segment = docs.segments().getInt(p);
                boolean changedSegment = operator.positionFieldWorkDocGuaranteedAscending(shard, segment);
                if (changedSegment) {
                    readColumnAtATime(segmentStart, i);
                    segmentStart = i;
                    ctx = operator.ctx(shard, segment);
                    fieldsMoved(ctx, shard);
                }
                readRowStride(docs.docs().getInt(p));
                i++;
                estimated = estimatedRamBytesUsed();
                log.trace("{}: bytes loaded {}/{}", p, estimated, dangerZoneBytes);
            }
            readColumnAtATime(segmentStart, i);
            buildBlocksSortedBackToOriginalDocSequence();
            if (log.isDebugEnabled()) {
                long actual = 0;
                for (Block b : target) {
                    actual += b.ramBytesUsed();
                }
                log.debug("loaded {} positions total estimated/actual {}/{} bytes", p + 1, estimated, actual);
            }
        }

        /**
         * Builds blocks and reorders from forwards (sorted) order back to original page order
         * using the backwards map. Large pages are not split into smaller ones.
         */
        private void buildBlocksSortedBackToOriginalDocSequence() {
            convertAndAccumulate();
            for (int f = 0; f < target.length; f++) {
                try (Block targetBlock = finalBuilders[f].build()) {
                    assert targetBlock.getPositionCount() == backwards.length
                        : targetBlock.getPositionCount() + " == " + backwards.length + " " + targetBlock;
                    target[f] = targetBlock.filter(false, backwards);
                }
                operator.sanityCheckBlock(current[f].rowStride, backwards.length, target[f], f);
            }
            if (target[0].getPositionCount() != docs.getPositionCount()) {
                throw new IllegalStateException("partial pages not yet supported");
            }
        }

        private void readColumnAtATime(int segmentStart, int segmentEnd) throws IOException {
            ValuesReaderDocs readerDocs = new ValuesReaderDocs(docs).mapped(forwards, segmentStart, segmentEnd);
            readerDocs.setCount(segmentEnd);
            for (CurrentWork c : columnAtATime) {
                assert c.rowStride == null;
                try (Block read = (Block) c.columnAtATime.read(blockFactory, readerDocs, segmentStart, c.field.info.nullsFiltered())) {
                    // TODO add a `read(builder, docs, offset, nullsFiltered)` override. Maybe even with the map.
                    assert read.getPositionCount() == segmentEnd - segmentStart
                        : read.getPositionCount() + " == " + segmentEnd + " - " + segmentStart + " " + read;
                    c.builder.copyFrom(read, 0, read.getPositionCount());
                }
            }
        }
    }
}
