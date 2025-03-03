/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplingQuery;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.SplittableRandom;

public class RandomSampleOperator implements Operator {

    // The threshold for the number of rows to collect in a batch before starting sampling it.
    private static final int ROWS_BATCH_THRESHOLD = 10_000;

    private final double probability;
    private final int seed;

    private boolean collecting = true;
    private boolean isFinished = false;
    private final PageBatching pageBatching;
    private BatchSampling currentSampling;

    private int pagesCollected = 0;
    private int pagesEmitted = 0;
    private int rowsCollected = 0;
    private int rowsEmitted = 0;
    private int batchesSampled = 0;

    private long collectNanos;
    private long emitNanos;

    public RandomSampleOperator(double probability, int seed) {
        this.probability = probability;
        this.seed = seed;
        // TODO derive the threshold from the probability and a max cap
        pageBatching = new PageBatching(ROWS_BATCH_THRESHOLD);
    }

    public record Factory(double probability, int seed) implements OperatorFactory {

        @Override
        public RandomSampleOperator get(DriverContext driverContext) {
            return new RandomSampleOperator(probability, seed);
        }

        @Override
        public String describe() {
            return "RandomSampleOperator[probability = " + probability + ", seed = " + seed + "]";
        }
    }

    /**
     * whether the given operator can accept more input pages
     */
    @Override
    public boolean needsInput() {
        return collecting;
    }

    /**
     * adds an input page to the operator. only called when needsInput() == true and isFinished() == false
     *
     * @param page
     * @throws UnsupportedOperationException if the operator is a {@link SourceOperator}
     */
    @Override
    public void addInput(Page page) {
        final var addStart = System.nanoTime();
        collect(page);
        collectNanos += System.nanoTime() - addStart;
    }

    private void collect(Page page) {
        pagesCollected++;
        rowsCollected += page.getPositionCount();
        pageBatching.addPage(page);
    }

    /**
     * notifies the operator that it won't receive any more input pages
     */
    @Override
    public void finish() {
        if (collecting && rowsCollected > 0) { // finish() can be called multiple times
            pageBatching.flush();
        }
        collecting = false;
    }

    /**
     * whether the operator has finished processing all input pages and made the corresponding output pages available
     */
    @Override
    public boolean isFinished() {
        return isFinished;
    }

    /**
     * returns non-null if output page available. Only called when isFinished() == false
     *
     * @throws UnsupportedOperationException if the operator is a {@link SinkOperator}
     */
    @Override
    public Page getOutput() {
        final var emitStart = System.nanoTime();
        Page page = emit();
        emitNanos += System.nanoTime() - emitStart;
        return page;
    }

    private Page emit() {
        if (currentSampling == null) {
            if (pageBatching.hasNext() == false) {
                if (collecting == false) {
                    isFinished = true;
                }
                return null; // not enough pages on the input yet
            }
            final var currentBatch = pageBatching.next();
            currentSampling = new BatchSampling(currentBatch, probability, seed);
            batchesSampled++;
        }

        final var page = currentSampling.next();
        if (page != null) {
            rowsEmitted += page.getPositionCount();
            pagesEmitted++;
            return page;
        } // else: current batch is exhausted

        currentSampling.close();
        currentSampling = null;
        return emit();
    }


    /**
     * notifies the operator that it won't be used anymore (i.e. none of the other methods called),
     * and its resources can be cleaned up
     */
    @Override
    public void close() {
        pageBatching.close();
    }

    @Override
    public String toString() {
        return "RandomSampleOperator[sampled = " + rowsEmitted + "/" + rowsCollected + "]";
    }

    @Override
    public Operator.Status status() {
        return new Status(collectNanos, emitNanos, pagesCollected, pagesEmitted, rowsCollected, rowsEmitted, batchesSampled);
    }

    private static class SamplingIterator {

        private final RandomSamplingQuery.RandomSamplingIterator samplingIterator;
        private int nextDoc = -1;

        SamplingIterator(int maxDoc, double probability, int seed) {
            final SplittableRandom random = new SplittableRandom(BitMixer.mix(seed));
            samplingIterator = new RandomSamplingQuery.RandomSamplingIterator(maxDoc, probability, random::nextInt);
            advance();
        }

        boolean hasNext() {
            return nextDoc != DocIdSetIterator.NO_MORE_DOCS;
        }

        int next() {
            return nextDoc;
        }

        void advance() {
            assert hasNext() : "No more docs to sample";
            nextDoc = samplingIterator.nextDoc();
        }
    }

    private record PagesBatch(ArrayDeque<Page> batch, int rowCount) {}

    private static class PageBatching {

        private final int collectingRowThreshold;

        private final List<PagesBatch> batches = new ArrayList<>();

        private int collectingBatchRowCount = 0;
        private ArrayDeque<Page> collectingBatch = new ArrayDeque<>();

        PageBatching(int collectingRowThreshold) {
            this.collectingRowThreshold = collectingRowThreshold;
        }

        void addPage(Page page) {
            collectingBatch.add(page);
            collectingBatchRowCount += page.getPositionCount();
            if (collectingBatchRowCount >= collectingRowThreshold) {
                rotate();
            }
        }

        private void rotate() {
            batches.add(new PagesBatch(collectingBatch, collectingBatchRowCount));
            collectingBatch = new ArrayDeque<>();
            collectingBatchRowCount = 0;
        }

        boolean hasNext() {
            return batches.isEmpty() == false;
        }

        PagesBatch next() {
            return batches.removeFirst();
        }

        void flush() {
            while (batches.isEmpty() == false) {
                var batch = batches.removeFirst();
                collectingBatch.addAll(batch.batch);
                collectingBatchRowCount += batch.rowCount;
            }
            if (collectingBatch.isEmpty() == false) {
                rotate();
            }
        }

        void close() {
            assert batches.isEmpty() : "There are still available batches";
            assert collectingBatch.isEmpty() : "Current batch has not been rotated";
        }
    }

    private static class BatchSampling {

        private final ArrayDeque<Page> pagesDeque;
        private final SamplingIterator samplingIterator;

        private int rowsProcessed = 0;

        BatchSampling(PagesBatch batch, double probability, int seed) {
            pagesDeque = batch.batch;
            samplingIterator = new SamplingIterator(batch.rowCount, probability, seed);
        }

        Page next() {
            while (pagesDeque.isEmpty() == false) {
                final var page = pagesDeque.poll();
                final int positionCount = page.getPositionCount();
                final int[] sampledPositions = new int[positionCount];
                int sampledIdx = 0;

                while (true) {
                    if (samplingIterator.hasNext()) {
                        var docOffset = samplingIterator.next() - rowsProcessed;
                        if (docOffset < positionCount) {
                            sampledPositions[sampledIdx++] = docOffset;
                            samplingIterator.advance();
                        } else {
                            // position falls outside the current page
                            break;
                        }
                    } else {
                        // no more docs to sample
                        drainPages();
                        break;
                    }
                }
                rowsProcessed += positionCount;

                if (sampledIdx > 0) {
                    var filter = Arrays.copyOf(sampledPositions, sampledIdx);
                    return page.filter(filter);
                } // else: fetch a new page (if any left)

                releasePage(page);
            }

            return null;
        }

        private void drainPages() {
            Page page;
            do {
                page = pagesDeque.poll();
            } while (releasePage(page));
        }

        /**
         * Returns true if there was a non-null page that was released.
         */
        private static boolean releasePage(Page page) {
            if (page != null) {
                page.releaseBlocks();
                return true;
            }
            return false;
        }

        void close() {
            assert pagesDeque.isEmpty() : "There are still unreleased pages";
            assert samplingIterator.hasNext() == false : "There are still docs to sample";
        }
    }

    private record Status(
        long collectNanos,
        long emitNanos,
        int pagesCollected,
        int pagesEmitted,
        int rowsCollected,
        int rowsEmitted,
        int batchesSampled
    ) implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "random_sample",
            Status::new
        );

        Status(StreamInput streamInput) throws IOException {
            this(
                streamInput.readVLong(),
                streamInput.readVLong(),
                streamInput.readVInt(),
                streamInput.readVInt(),
                streamInput.readVInt(),
                streamInput.readVInt(),
                streamInput.readVInt()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(collectNanos);
            out.writeVLong(emitNanos);
            out.writeVInt(pagesCollected);
            out.writeVInt(pagesEmitted);
            out.writeVInt(rowsCollected);
            out.writeVInt(rowsEmitted);
            out.writeVInt(batchesSampled);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("collect_nanos", collectNanos);
            if (builder.humanReadable()) {
                builder.field("collect_time", TimeValue.timeValueNanos(collectNanos));
            }
            builder.field("emit_nanos", emitNanos);
            if (builder.humanReadable()) {
                builder.field("emit_time", TimeValue.timeValueNanos(emitNanos));
            }
            builder.field("pages_collected", pagesCollected);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_collected", rowsCollected);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("batches_sampled", batchesSampled);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status other = (Status) o;
            return collectNanos == other.collectNanos
                && emitNanos == other.emitNanos
                && pagesCollected == other.pagesCollected
                && pagesEmitted == other.pagesEmitted
                && rowsCollected == other.rowsCollected
                && rowsEmitted == other.rowsEmitted
                && batchesSampled == other.batchesSampled;
        }

        @Override
        public int hashCode() {
            return Objects.hash(collectNanos, emitNanos, pagesCollected, pagesEmitted, rowsCollected, rowsEmitted, batchesSampled);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ZERO;
        }
    }
}
