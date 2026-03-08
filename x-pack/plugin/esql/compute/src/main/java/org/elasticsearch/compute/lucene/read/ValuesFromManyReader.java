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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads values from a many leaves. Much less efficient than {@link ValuesFromSingleReader}.
 */
class ValuesFromManyReader extends ValuesReader {
    private static final Logger log = LogManager.getLogger(ValuesFromManyReader.class);

    private final int[] forwards;
    private final int[] backwards;

    private BlockLoaderStoredFieldsFromLeafLoader storedFields;

    /**
     * Cumulative circuit breaker reservation held across partial pages produced
     * by {@link Run#loadDocSequence}. Each partial page's block data is tracked
     * on the breaker while the blocks exist, but downstream operators release
     * them before the next partial page is produced. This reservation keeps the
     * breaker aware of the total data produced so it can trip when the
     * accumulated output exceeds the limit.
     */
    private long docSequenceReservation;

    ValuesFromManyReader(ValuesSourceReaderOperator operator, DocVector docs) {
        super(operator, docs);
        forwards = docs.shardSegmentDocMapForwards();
        backwards = docs.shardSegmentDocMapBackwards();
        log.debug("initializing {} positions", docs.getPositionCount());
    }

    @Override
    protected void load(Block[] target, int offset) throws IOException {
        try (Run run = new Run(target)) {
            run.run(offset);
        }
    }

    @Override
    public void close() {
        if (docSequenceReservation > 0) {
            operator.driverContext.blockFactory().breaker().addWithoutBreaking(-docSequenceReservation);
            docSequenceReservation = 0;
        }
    }

    class Run implements Releasable {
        private final ComputeBlockLoaderFactory blockFactory = new ComputeBlockLoaderFactory(operator.driverContext.blockFactory());
        private final Block[] target;
        /**
         * The "final" builder for the block we're going to return. See {@link #current} for
         * how these are built.
         */
        private final Block.Builder[] finalBuilders;

        /**
         * The builders for the current shard. These start {@code null} and are filled in when we move
         * to the shard for the first time. When we finish with the shard we build a {@link Block}
         * and convert it to the target type and add it to {@link #finalBuilders}. Then we fill these
         * in for the next shard.
         * <p>
         *     Important: We load in {@code (shard, segment, doc)} sorted order. So we load all values
         *     for a shard at once, meaning once we move to the next shard, we'll never visit the same
         *     shard again.
         * </p>
         */
        private final CurrentWork[] current;

        private final List<CurrentWork> columnAtATime;
        private final List<CurrentWork> rowStride;
        private int currentShard = -1;

        Run(Block[] target) {
            this.target = target;
            finalBuilders = new Block.Builder[target.length];
            current = new CurrentWork[target.length];
            columnAtATime = new ArrayList<>(target.length);
            rowStride = new ArrayList<>(target.length);
        }

        void run(int offset) throws IOException {
            for (int f = 0; f < operator.fields.length; f++) {
                /*
                 * Important note: each field has a desired type, which might not match the mapped type (in the case of union-types).
                 * We create the final block builders using the desired type, one for each field, but then also use inner builders
                 * (one for each field and shard), and converters (again one for each field and shard) to actually perform the field
                 * loading in a way that is correct for the mapped field type, and then convert between that type and the desired type.
                 */
                finalBuilders[f] = operator.fields[f].info.type()
                    .newBlockBuilder(docs.getPositionCount() - offset, operator.driverContext.blockFactory());
            }
            int bytesRefFieldCount = bytesRefFieldCount();
            if (docs.singleShardSingleSegment() && bytesRefFieldCount >= operator.docSequenceBytesRefFieldThreshold) {
                if (log.isDebugEnabled()) {
                    log.debug("load according to doc sequence, the number of BytesRef fields is {}", bytesRefFieldCount);
                }
                loadDocSequence(offset);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("load according to forward sequence");
                }
                loadForwardSequence(offset);
            }
        }

        /**
         * Like {@link #loadForwardSequence(int)} but iterates in original page order instead of
         * {@code forwards} order. This avoids the {@code backwards} reorder at the end and
         * supports partial pages bounded by {@code jumboBytes}. Both row-stride and
         * column-at-a-time fields are read inside the loop so their sizes are included in
         * the {@code jumboBytes} check. Readers are recreated when doc IDs go backwards.
         * <p>
         *     Partial pages may produce single-position blocks whose memory is freed by
         *     downstream operators between pages. To prevent the circuit breaker from
         *     losing track of cumulative output, a reservation equal to each partial
         *     page's block size is held on the breaker (see
         *     {@link ValuesFromManyReader#docSequenceReservation}). This reservation
         *     accumulates across partial pages, ensuring the breaker trips when the
         *     total data produced exceeds its limit.
         * </p>
         */
        private void loadDocSequence(int offset) throws IOException {
            int shard = docs.shards().getInt(offset);
            int segment = docs.segments().getInt(offset);
            int firstDoc = docs.docs().getInt(offset);
            operator.positionFieldWork(shard, segment, firstDoc);
            LeafReaderContext ctx = operator.ctx(shard, segment);
            resetReaders(ctx);
            fieldsMoved(ctx, shard);
            int runStart = offset;
            readRowStride(firstDoc);
            int prevDoc = firstDoc;
            int i = offset + 1;
            long estimated = estimatedRamBytesUsed();
            while (i < docs.getPositionCount() && estimated < operator.jumboBytes) {
                int doc = docs.docs().getInt(i);
                if (doc < prevDoc) {
                    readColumnAtATimeBatch(runStart, i);
                    resetReaders(ctx);
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
            buildBlocksDirect(count);
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
         * Force-closes and recreates all row-stride and column-at-a-time readers.
         * Needed because we read in original page order which may start at a lower
         * doc ID than the reader's current position.
         */
        private void resetReaders(LeafReaderContext ctx) throws IOException {
            for (CurrentWork c : rowStride) {
                if (c.field.rowStride != null) {
                    c.field.rowStride.close();
                    c.field.rowStride = null;
                }
                c.rowStride = c.field.rowStride(ctx);
            }
            for (CurrentWork c : columnAtATime) {
                if (c.field.columnAtATime != null) {
                    c.field.columnAtATime.close();
                    c.field.columnAtATime = null;
                }
                c.columnAtATime = c.field.columnAtATime(ctx);
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
            buildBlocksReordered();
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
        private void buildBlocksReordered() {
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

        /**
         * Builds blocks directly from the builders without reordering.
         * This is used when fetching according to doc sequence in the DocVector.
         */
        private void buildBlocksDirect(int count) {
            convertAndAccumulate();
            for (int f = 0; f < target.length; f++) {
                target[f] = finalBuilders[f].build();
                assert target[f].getPositionCount() == count : target[f].getPositionCount() + " == " + count + " " + target[f];
                operator.sanityCheckBlock(current[f].rowStride, count, target[f], f);
            }
        }

        private void readRowStride(int doc) throws IOException {
            storedFields.advanceTo(doc);
            for (CurrentWork r : rowStride) {
                assert r.columnAtATime == null;
                r.rowStride.read(doc, storedFields, r.builder);
            }
            operator.trackSourceBytesAndRelease(storedFields);
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

        @Override
        public void close() {
            Releasables.closeExpectNoException(blockFactory, Releasables.wrap(finalBuilders), Releasables.wrap(current));
        }

        private long estimatedRamBytesUsed() {
            long sum = 0;
            for (int f = 0; f < current.length; f++) {
                sum += finalBuilders[f].estimatedBytes();
                sum += current[f].builder.estimatedBytes();
            }
            return sum;
        }

        private void fieldsMoved(LeafReaderContext ctx, int shard) throws IOException {
            if (currentShard != shard) {
                if (currentShard >= 0) {
                    convertAndAccumulate();
                }
                moveBuildersAndLoadersToShard();
                currentShard = shard;
            }
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            columnAtATime.clear();
            rowStride.clear();
            for (CurrentWork field : current) {
                field.columnAtATime = field.field.columnAtATime(ctx);
                if (field.columnAtATime != null) {
                    columnAtATime.add(field);
                } else {
                    field.rowStride = field.field.rowStride(ctx);
                    storedFieldsSpec = storedFieldsSpec.merge(field.field.loader.rowStrideStoredFieldSpec());
                    rowStride.add(field);
                }
            }
            SourceLoader sourceLoader = null;
            if (storedFieldsSpec.requiresSource()) {
                sourceLoader = operator.shardContexts.get(shard).newSourceLoader().apply(storedFieldsSpec.sourcePaths());
                storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(true, false, sourceLoader.requiredStoredFields()));
            }
            storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(ctx, null),
                sourceLoader != null ? sourceLoader.leaf(ctx.reader(), null) : null
            );
            if (false == storedFieldsSpec.equals(StoredFieldsSpec.NO_REQUIREMENTS)) {
                operator.trackStoredFields(storedFieldsSpec, false);
            }
        }

        private void convertAndAccumulate() {
            for (CurrentWork currentWork : current) {
                try {
                    currentWork.convertAndAccumulate();
                } finally {
                    currentWork.close();
                    /*
                     * Calling currentWork.close() here is redundant. Once you call
                     * `BlockBuilder#build`, `BlockBuilder#close` becomes a noop. Safe to call
                     * but not required. But calling it is more idiomatic, so we do it.
                     *
                     * In many cases this is the last consumer from of the doc vector, so we
                     * *could* aggressively free the shard context right here - as soon as we're
                     * done with it. But we don't because:
                     * 1. We don't know if we're the last user.
                     * 2. We don't have a code path to free just a single segment's worth of
                     *    references from the doc vector. It'd be easy to build, but much harder
                     *    to build the path that causes us to *NOT* double free.
                     */
                }
            }
        }

        private void moveBuildersAndLoadersToShard() {
            for (int f = 0; f < operator.fields.length; f++) {
                // NOTE: This relies on the operator.fields being positioned on the new shard.
                current[f] = new CurrentWork(blockFactory, docs, operator.fields[f], finalBuilders[f]);
            }
        }

        /**
         * Returns the number of fields whose element type is {@link ElementType#BYTES_REF},
         * which covers keyword and text fields.
         */
        int bytesRefFieldCount() {
            int count = 0;
            for (ValuesSourceReaderOperator.FieldWork field : operator.fields) {
                if (field.info.type() == ElementType.BYTES_REF) {
                    count++;
                }
            }
            return count;
        }
    }

    /**
     * Work for a single field for the current segment. If there's a conversion, then this contains
     * a "scratch" builder and {@link #convertAndAccumulate} accumulates the scratch builder into
     * the {@link #finalBuilder}. If there isn't a conversion then this accumulates directly into
     * the {@link #finalBuilder} immediately.
     */
    private static class CurrentWork implements Releasable {
        private final ValuesSourceReaderOperator.FieldWork field;
        /**
         * Converter for the field at the current segment. It's a copy of
         * {@code field.converter} at the time of construction. By the time we actually
         * go to use the converter, the field has moved onto another shard, changing
         * the value of {@code field.converter}.
         */
        @Nullable
        private final ValuesSourceReaderOperator.ConverterEvaluator converter;
        private final Block.Builder builder;
        private final Block.Builder finalBuilder;

        private BlockLoader.ColumnAtATimeReader columnAtATime;
        private BlockLoader.RowStrideReader rowStride;

        CurrentWork(
            ComputeBlockLoaderFactory blockFactory,
            DocVector docs,
            ValuesSourceReaderOperator.FieldWork field,
            Block.Builder finalBuilder
        ) {
            this.field = field;
            this.converter = field.converter;
            this.builder = converter == null ? finalBuilder : (Block.Builder) field.loader.builder(blockFactory, docs.getPositionCount());
            this.finalBuilder = finalBuilder;
        }

        void convertAndAccumulate() {
            if (converter == null) {
                // We built directly into the final block so there isn't any need to convert anything
                return;
            }
            try (Block orig = converter.convert(builder.build())) {
                finalBuilder.copyFrom(orig, 0, orig.getPositionCount());
            }
        }

        @Override
        public void close() {
            if (converter != null) {
                /*
                 * If there *isn't* a converter than the `builder` is just the final builder
                 * and it's closed by the Run.
                 */
                builder.close();
            }
        }
    }
}
