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
 * See {@link ValuesSourceReaderOperator} for an introduction. This takes a page containing
 * a {@link DocVector} like:
 * {@snippet lang="txt" :
 * ┌───────────────────────┐
 * │          doc          │
 * ├───────┬─────────┬─────┤
 * │ shard │ segment │ doc │
 * ├───────┼─────────┼─────┤
 * │     0 │       0 │   0 │
 * │     0 │       1 │   0 │
 * │     0 │       1 │   1 │
 * │     1 │       0 │   1 │
 * │     1 │       1 │  12 │
 * └───────┴─────────┴─────┘
 * }
 * <p>
 *     and loads columns from lucene:
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────────────┬─────┐
 * │          doc          │     │
 * ├───────┬─────────┬─────│ ref │
 * │ shard │ segment │ doc │     │
 * ├───────┼─────────┼─────┼─────┤
 * │     0 │       0 │   0 │ 173 │
 * │     0 │       1 │   0 │ 049 │
 * │     0 │       1 │   1 │ 096 │
 * │     1 │       0 │   1 │ 682 │
 * │     1 │       1 │  12 │ 055 │
 * └───────┴─────────┴─────┴─────┘
 * }
 * <h2>Are the documents non-decreasing?</h2>
 * <p>
 *     Lucene's tools for loading values need to load documents in non-decreasing order.
 *     Think {@code 1, 2, 3, 4, 4, 4, 5, 9, 1000, etc}. The reader can only go "forwards".
 *     It can go forwards {@code 0} documents, but never backwards. This implementation
 *     always loads values in this order, regardless of the order in the actual page.
 *     If we're lucky then they are already in that order, like the example above. If,
 *     instead, the incoming page looks like:
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────────────┐
 * │          doc          │
 * ├───────┬─────────┬─────┤
 * │ shard │ segment │ doc │
 * ├───────┼─────────┼─────┤
 * │     0 │       1 │   0 │
 * │     0 │       0 │   0 │
 * │     1 │       1 │  12 │
 * │     0 │       1 │   1 │
 * │     1 │       0 │   1 │
 * └───────┴─────────┴─────┘
 * }
 * <p>
 *     Then we map the rows <strong>into</strong> non-decreasing order. We load in that
 *     order. Then put them back in order:
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────────────┐   ┌───────────────────────┐   ┌───────────────────────┬─────┐   ┌───────────────────────┬─────┐
 * │          doc          │   │          doc          │   │          doc          │     │   │          doc          │     │
 * ├───────┬─────────┬─────┤   ├───────┬─────────┬─────┤   ├───────┬─────────┬─────┤ ref │   ├───────┬─────────┬─────┤ ref │
 * │ shard │ segment │ doc │   │ shard │ segment │ doc │   │ shard │ segment │ doc │     │   │ shard │ segment │ doc │     │
 * ├───────┼─────────┼─────┤   ├───────┼─────────┼─────┤   ├───────┼─────────┼─────┼─────┤   ├───────┼─────────┼─────┼─────┤
 * │     0 │       1 │   0 │   │     0 │       0 │   0 │   │     0 │       0 │   0 │ 173 │   │     0 │       1 │   0 │ 049 │
 * │     0 │       0 │   0 │   │     0 │       1 │   0 │   │     0 │       1 │   0 │ 049 │   │     0 │       0 │   0 │ 173 │
 * │     1 │       1 │  12 │ ⟶ │     0 │       1 │   1 │ ⟶ │     0 │       1 │   1 │ 096 │ ⟶ │     1 │       1 │  12 │ 055 │
 * │     0 │       1 │   1 │   │     1 │       0 │   1 │   │     1 │       0 │   1 │ 682 │   │     0 │       1 │   1 │ 096 │
 * │     1 │       0 │   1 │   │     1 │       1 │  12 │   │     1 │       1 │  12 │ 055 │   │     1 │       0 │   1 │ 682 │
 * └───────┴─────────┴─────┘   └───────┴─────────┴─────┘   └───────┴─────────┴─────┴─────┘   └───────┴─────────┴─────┴─────┘
 * }
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
