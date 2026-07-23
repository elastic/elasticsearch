/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * Page-mapping operator that materializes deferred ("wide") columns for the rows surviving a
 * per-driver TopN.
 * <p>
 * Reads the {@code _rowPosition} channel from each incoming page — these are encoded row
 * references (extractor id packed with file-local position) written by the source factory. Calls
 * {@link SourceExtractors#materialize(long[], int, List, List, BlockFactory)} to materialize the
 * deferred columns, then assembles an output page that:
 * <ul>
 *     <li>Keeps every input block <em>except</em> the {@code _rowPosition} channel.</li>
 *     <li>Appends the deferred columns in the order requested at construction.</li>
 * </ul>
 * <p>
 * The registry is owned by the upstream {@code AsyncExternalSourceOperatorFactory} and resolved
 * per-driver via a {@code Function<DriverContext, SourceExtractors>} supplied at construction.
 * The source populates the registry as it opens files; this operator reads it after TopN
 * finishes. Single-threaded — same driver thread.
 * <p>
 * Implements {@link Operator} directly rather than extending {@code AbstractPageMappingOperator}
 * because the latter short-circuits zero-position pages — that would propagate the input shape
 * (including {@code _rowPosition}) instead of our declared output shape, breaking downstream
 * channel indexing.
 */
public class ExternalFieldExtractOperator implements Operator {

    /**
     * Builds {@link ExternalFieldExtractOperator}s for each driver. The {@code sourceExtractorsLookup}
     * resolves the per-driver {@link SourceExtractors} populated by the upstream source operator
     * factory (typically {@code AsyncExternalSourceOperatorFactory::sourceExtractorsFor}). Tests
     * may supply a pre-populated registry directly via {@code ignored -> registry}.
     */
    public static final class Factory implements Operator.OperatorFactory {
        private final int rowPositionChannel;
        private final List<Integer> passThroughChannels;
        private final List<String> deferredColumnNames;
        private final List<DataType> deferredColumnTypes;
        private final Function<DriverContext, SourceExtractors> sourceExtractorsLookup;

        /**
         * @param rowPositionChannel       channel index in the input page that holds {@code _rowPosition}
         * @param passThroughChannels      channel indices of the input page that should be copied
         *                                 to the output (in the order they appear in the output)
         * @param deferredColumnNames      names of the deferred columns to load, in output order
         * @param deferredColumnTypes      planner/declared type per deferred column, aligned with
         *                                 {@code deferredColumnNames}; extraction coerces the file's
         *                                 value to this type exactly like the eager decode paths
         *                                 ({@code DeclaredTypeCoercions})
         * @param sourceExtractorsLookup   per-driver registry resolver; must never return
         *                                 {@code null}
         */
        public Factory(
            int rowPositionChannel,
            List<Integer> passThroughChannels,
            List<String> deferredColumnNames,
            List<DataType> deferredColumnTypes,
            Function<DriverContext, SourceExtractors> sourceExtractorsLookup
        ) {
            if (rowPositionChannel < 0) {
                throw new IllegalArgumentException("rowPositionChannel must be non-negative, got [" + rowPositionChannel + "]");
            }
            if (passThroughChannels == null) {
                throw new IllegalArgumentException("passThroughChannels must not be null");
            }
            if (deferredColumnNames == null) {
                throw new IllegalArgumentException("deferredColumnNames must not be null");
            }
            if (deferredColumnTypes == null || deferredColumnTypes.size() != deferredColumnNames.size()) {
                throw new IllegalArgumentException(
                    "deferredColumnTypes must align with deferredColumnNames, got ["
                        + (deferredColumnTypes == null ? "null" : deferredColumnTypes.size())
                        + "] for ["
                        + deferredColumnNames.size()
                        + "] names"
                );
            }
            if (sourceExtractorsLookup == null) {
                throw new IllegalArgumentException("sourceExtractorsLookup must not be null");
            }
            this.rowPositionChannel = rowPositionChannel;
            this.passThroughChannels = List.copyOf(passThroughChannels);
            this.deferredColumnNames = List.copyOf(deferredColumnNames);
            this.deferredColumnTypes = List.copyOf(deferredColumnTypes);
            this.sourceExtractorsLookup = sourceExtractorsLookup;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            SourceExtractors registry = sourceExtractorsLookup.apply(driverContext);
            if (registry == null) {
                throw new IllegalStateException(
                    "sourceExtractorsLookup returned null for driverContext; deferred extraction is not wired correctly"
                );
            }
            return new ExternalFieldExtractOperator(
                rowPositionChannel,
                passThroughChannels,
                deferredColumnNames,
                deferredColumnTypes,
                registry,
                driverContext.blockFactory()
            );
        }

        @Override
        public String describe() {
            return "ExternalFieldExtractOperator[rowPositionChannel="
                + rowPositionChannel
                + ", passThrough="
                + passThroughChannels.size()
                + ", deferred="
                + deferredColumnNames
                + "]";
        }
    }

    private final int rowPositionChannel;
    private final List<Integer> passThroughChannels;
    private final List<String> deferredColumnNames;
    private final List<DataType> deferredColumnTypes;
    private final SourceExtractors registry;
    private final BlockFactory blockFactory;
    private final LongAdder pagesProcessed = new LongAdder();
    private final LongAdder rowsExtracted = new LongAdder();
    private final LongAdder extractNanos = new LongAdder();

    private Page prev;
    private boolean finished;

    ExternalFieldExtractOperator(
        int rowPositionChannel,
        List<Integer> passThroughChannels,
        List<String> deferredColumnNames,
        List<DataType> deferredColumnTypes,
        SourceExtractors registry,
        BlockFactory blockFactory
    ) {
        this.rowPositionChannel = rowPositionChannel;
        this.passThroughChannels = passThroughChannels;
        this.deferredColumnNames = deferredColumnNames;
        this.deferredColumnTypes = deferredColumnTypes;
        this.registry = registry;
        this.blockFactory = blockFactory;
    }

    @Override
    public boolean needsInput() {
        return prev == null && finished == false;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return prev != null;
    }

    @Override
    public void addInput(Page page) {
        assert prev == null : "has pending input page";
        prev = page;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && prev == null;
    }

    @Override
    public Page getOutput() {
        if (prev == null) {
            return null;
        }
        Page page = prev;
        prev = null;
        if (page.getPositionCount() == 0) {
            return reshapeEmpty(page);
        }
        long start = System.nanoTime();
        try {
            Page out = materialize(page);
            rowsExtracted.add(out.getPositionCount());
            return out;
        } finally {
            extractNanos.add(System.nanoTime() - start);
            pagesProcessed.increment();
        }
    }

    @Override
    public Status status() {
        return new Status(pagesProcessed.sum(), rowsExtracted.sum(), extractNanos.sum());
    }

    /**
     * For an empty input page, build a shape-correct empty output: drop {@code _rowPosition},
     * keep the pass-through blocks (incRef'd), and append empty placeholder blocks for the
     * deferred columns.
     */
    private Page reshapeEmpty(Page page) {
        Block[] outBlocks = new Block[passThroughChannels.size() + deferredColumnNames.size()];
        int idx = 0;
        try {
            for (int ch : passThroughChannels) {
                Block b = page.getBlock(ch);
                b.incRef();
                outBlocks[idx++] = b;
            }
            // Deferred columns: empty pages carry no _rowPosition values, so we can't go through
            // the registry. Emit constant-null blocks instead — downstream operators see the
            // right shape and treat them as nulls (which is consistent with there being no rows).
            for (int i = 0; i < deferredColumnNames.size(); i++) {
                outBlocks[idx++] = blockFactory.newConstantNullBlock(0);
            }
            page.releaseBlocks();
            return new Page(0, outBlocks);
        } catch (RuntimeException e) {
            for (int i = 0; i < idx; i++) {
                if (outBlocks[i] != null) {
                    outBlocks[i].close();
                }
            }
            page.releaseBlocks();
            throw e;
        }
    }

    /**
     * Hot path: extract deferred columns for the surviving positions and assemble the output
     * page. Pass-through blocks get an extra ref so the new page owns its own references; the
     * old page's references are released via {@link Page#releaseBlocks()}.
     */
    private Page materialize(Page page) {
        int positions = page.getPositionCount();
        Block rpBlock = page.getBlock(rowPositionChannel);
        if (rpBlock instanceof LongBlock == false) {
            page.releaseBlocks();
            throw new IllegalStateException(
                "_rowPosition channel [" + rowPositionChannel + "] expected LongBlock but was " + rpBlock.getClass().getSimpleName()
            );
        }
        LongBlock rp = (LongBlock) rpBlock;
        long[] refs = new long[positions];
        LongVector rpVector = rp.asVector();
        if (rpVector != null) {
            for (int i = 0; i < positions; i++) {
                refs[i] = rpVector.getLong(i);
            }
        } else {
            for (int i = 0; i < positions; i++) {
                if (rp.isNull(i) || rp.getValueCount(i) != 1) {
                    page.releaseBlocks();
                    throw new IllegalStateException(
                        "_rowPosition channel [" + rowPositionChannel + "] at position [" + i + "] must hold exactly one non-null value"
                    );
                }
                refs[i] = rp.getLong(rp.getFirstValueIndex(i));
            }
        }

        Block[] deferredBlocks = registry.materialize(refs, positions, deferredColumnNames, deferredColumnTypes, blockFactory);
        try {
            Block[] outBlocks = new Block[passThroughChannels.size() + deferredBlocks.length];
            int idx = 0;
            for (int ch : passThroughChannels) {
                Block b = page.getBlock(ch);
                b.incRef();
                outBlocks[idx++] = b;
            }
            for (Block d : deferredBlocks) {
                outBlocks[idx++] = d;
            }
            page.releaseBlocks();
            for (int i = 0; i < deferredBlocks.length; i++) {
                deferredBlocks[i] = null;
            }
            return new Page(positions, outBlocks);
        } catch (RuntimeException e) {
            Releasables.closeExpectNoException(deferredBlocks);
            page.releaseBlocks();
            throw e;
        }
    }

    @Override
    public String toString() {
        return "ExternalFieldExtractOperator[rowPositionChannel="
            + rowPositionChannel
            + ", passThrough="
            + passThroughChannels.size()
            + ", deferred="
            + deferredColumnNames
            + "]";
    }

    @Override
    public void close() {
        if (prev != null) {
            Page pending = prev;
            prev = null;
            Releasables.closeExpectNoException(pending::releaseBlocks);
        }
        // The registry is shared between source and this operator. The lifecycle is the driver:
        // when this operator closes (driver teardown), close the registry to release per-file
        // extractors and any held StorageObjects.
        registry.close();
    }

    /**
     * Per-driver counters for {@link ExternalFieldExtractOperator}, surfaced as the operator's
     * {@code status} in the profile: pages processed, rows whose deferred columns were materialized,
     * and wall time in {@code materialize(...)}. Wire-gated by {@code esql_external_source_profile}
     * so older nodes round-trip a zero-valued status.
     */
    public static class Status implements Operator.Status {

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "external_field_extract",
            Status::new
        );

        private static final TransportVersion ESQL_EXTERNAL_SOURCE_PROFILE = TransportVersion.fromName("esql_external_source_profile");

        private final long pagesProcessed;
        private final long rowsExtracted;
        private final long extractNanos;

        public Status(long pagesProcessed, long rowsExtracted, long extractNanos) {
            this.pagesProcessed = pagesProcessed;
            this.rowsExtracted = rowsExtracted;
            this.extractNanos = extractNanos;
        }

        Status(StreamInput in) throws IOException {
            // The operator + its Status only exist on nodes that support deferred extraction
            // (elasticsearch#149185), which landed alongside this PR. Pre-version nodes never
            // send this entry, but be defensive and accept either shape.
            if (in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
                pagesProcessed = in.readVLong();
                rowsExtracted = in.readVLong();
                extractNanos = in.readVLong();
            } else {
                pagesProcessed = 0L;
                rowsExtracted = 0L;
                extractNanos = 0L;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
                out.writeVLong(pagesProcessed);
                out.writeVLong(rowsExtracted);
                out.writeVLong(extractNanos);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public long pagesProcessed() {
            return pagesProcessed;
        }

        @Override
        public long rowsEmitted() {
            // Output rows mirror the input rows that survived TopN; counted here so this operator
            // contributes to the per-driver rowsEmitted rollup like any other source-ish stage.
            return rowsExtracted;
        }

        public long extractNanos() {
            return extractNanos;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, org.elasticsearch.xcontent.ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_extracted", rowsExtracted);
            builder.field("extract_nanos", extractNanos);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Status status = (Status) o;
            return pagesProcessed == status.pagesProcessed && rowsExtracted == status.rowsExtracted && extractNanos == status.extractNanos;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesProcessed, rowsExtracted, extractNanos);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }
    }
}
