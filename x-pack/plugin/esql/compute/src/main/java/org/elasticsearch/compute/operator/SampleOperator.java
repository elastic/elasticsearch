/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

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
import java.util.Arrays;
import java.util.Deque;
import java.util.Objects;
import java.util.SplittableRandom;

public class SampleOperator implements Operator {

    public record Factory(double probability, int seed) implements OperatorFactory {

        @Override
        public SampleOperator get(DriverContext driverContext) {
            return new SampleOperator(probability, seed);
        }

        @Override
        public String describe() {
            return "SampleOperator[probability = " + probability + ", seed = " + seed + "]";
        }
    }

    private final Deque<Page> outputPages;

    /**
     * At any time this iterator will point to be next document that still
     * needs to be sampled. If this document is on the current page, it's
     * added to the output and the iterator is advanced. It the document is
     * not on the current page, the current page is finished and the index
     * is used for the next page.
     */
    private final RandomSamplingQuery.RandomSamplingIterator randomSamplingIterator;
    private boolean finished;

    private int pagesProcessed = 0;
    private int rowsReceived = 0;
    private int rowsEmitted = 0;
    private long collectNanos;
    private long emitNanos;

    private SampleOperator(double probability, int seed) {
        finished = false;
        outputPages = new ArrayDeque<>();
        SplittableRandom random = new SplittableRandom(seed);
        randomSamplingIterator = new RandomSamplingQuery.RandomSamplingIterator(Integer.MAX_VALUE, probability, random::nextInt);
        // Initialize the iterator to the next document that needs to be sampled.
        randomSamplingIterator.nextDoc();
    }

    /**
     * whether the given operator can accept more input pages
     */
    @Override
    public boolean needsInput() {
        return finished == false;
    }

    /**
     * adds an input page to the operator. only called when needsInput() == true and isFinished() == false
     *
     * @param page
     * @throws UnsupportedOperationException if the operator is a {@link SourceOperator}
     */
    @Override
    public void addInput(Page page) {
        long startTime = System.nanoTime();
        createOutputPage(page);
        rowsReceived += page.getPositionCount();
        page.releaseBlocks();
        pagesProcessed++;
        collectNanos += System.nanoTime() - startTime;
    }

    private void createOutputPage(Page page) {
        final int[] sampledPositions = new int[page.getPositionCount()];
        int sampledIdx = 0;
        for (int i = randomSamplingIterator.docID(); i - rowsReceived < page.getPositionCount(); i = randomSamplingIterator.nextDoc()) {
            sampledPositions[sampledIdx++] = i - rowsReceived;
        }
        if (sampledIdx > 0) {
            outputPages.add(page.filter(Arrays.copyOf(sampledPositions, sampledIdx)));
        }
    }

    /**
     * notifies the operator that it won't receive any more input pages
     */
    @Override
    public void finish() {
        finished = true;
    }

    /**
     * whether the operator has finished processing all input pages and made the corresponding output pages available
     */
    @Override
    public boolean isFinished() {
        return finished && outputPages.isEmpty();
    }

    @Override
    public Page getOutput() {
        final var emitStart = System.nanoTime();
        Page page;
        if (outputPages.isEmpty()) {
            page = null;
        } else {
            page = outputPages.removeFirst();
            rowsEmitted += page.getPositionCount();
        }
        emitNanos += System.nanoTime() - emitStart;
        return page;
    }

    /**
     * notifies the operator that it won't be used anymore (i.e. none of the other methods called),
     * and its resources can be cleaned up
     */
    @Override
    public void close() {
        for (Page page : outputPages) {
            page.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "SampleOperator[sampled = " + rowsEmitted + "/" + rowsReceived + "]";
    }

    @Override
    public Operator.Status status() {
        return new Status(collectNanos, emitNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }

    private record Status(long collectNanos, long emitNanos, int pagesProcessed, int rowsReceived, int rowsEmitted)
        implements
            Operator.Status {

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "sample",
            Status::new
        );

        Status(StreamInput streamInput) throws IOException {
            this(streamInput.readVLong(), streamInput.readVLong(), streamInput.readVInt(), streamInput.readVInt(), streamInput.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(collectNanos);
            out.writeVLong(emitNanos);
            out.writeVInt(pagesProcessed);
            out.writeVInt(rowsReceived);
            out.writeVInt(rowsEmitted);
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
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status other = (Status) o;
            return collectNanos == other.collectNanos
                && emitNanos == other.emitNanos
                && pagesProcessed == other.pagesProcessed
                && rowsReceived == other.rowsReceived
                && rowsEmitted == other.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(collectNanos, emitNanos, pagesProcessed, rowsReceived, rowsEmitted);
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
