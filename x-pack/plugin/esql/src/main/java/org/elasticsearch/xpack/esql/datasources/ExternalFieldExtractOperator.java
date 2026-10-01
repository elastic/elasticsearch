/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ThreadCpuTimer;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
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
 * finishes. Row-reference validation, empty-page reshaping, and output assembly run on the driver
 * thread. Non-empty materialization runs on the external read executor, one request at a time;
 * while it is pending the operator blocks the driver.
 * <p>
 * Extends {@link AsyncOperator} rather than {@code AbstractPageMappingOperator} because materialization
 * performs external I/O and because the latter short-circuits zero-position pages. That shortcut would
 * propagate the input shape (including {@code _rowPosition}) instead of the declared output shape.
 */
public class ExternalFieldExtractOperator extends AsyncOperator<ExternalFieldExtractOperator.Result> {

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
        private final Executor executor;

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
         * @param executor                 executor for non-empty materialization
         */
        public Factory(
            int rowPositionChannel,
            List<Integer> passThroughChannels,
            List<String> deferredColumnNames,
            List<DataType> deferredColumnTypes,
            Function<DriverContext, SourceExtractors> sourceExtractorsLookup,
            Executor executor
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
            if (executor == null) {
                throw new IllegalArgumentException("executor must not be null");
            }
            this.rowPositionChannel = rowPositionChannel;
            this.passThroughChannels = List.copyOf(passThroughChannels);
            this.deferredColumnNames = List.copyOf(deferredColumnNames);
            this.deferredColumnTypes = List.copyOf(deferredColumnTypes);
            this.sourceExtractorsLookup = sourceExtractorsLookup;
            this.executor = executor;
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
                driverContext,
                executor
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
    private final RefCountingRunnable registryRefs;
    private final BlockFactory blockFactory;
    private final Executor executor;
    private final LongAdder rowsExtracted = new LongAdder();
    private final LongAdder extractNanos = new LongAdder();
    private final LongAdder extractCpuNanos = new LongAdder();

    private volatile IsBlockedResult materializationBlocked = Operator.NOT_BLOCKED;

    static final class Result {
        private final Page inputPage;
        private final Block[] deferredBlocks;
        private final Page outputPage;
        private final Throwable failure;

        private Result(Page inputPage, Block[] deferredBlocks, Page outputPage, Throwable failure) {
            this.inputPage = inputPage;
            this.deferredBlocks = deferredBlocks;
            this.outputPage = outputPage;
            this.failure = failure;
        }

        static Result materialized(Page inputPage, Block[] deferredBlocks) {
            return new Result(inputPage, deferredBlocks, null, null);
        }

        static Result output(Page outputPage) {
            return new Result(null, null, outputPage, null);
        }

        static Result failure(Page inputPage, Throwable failure) {
            return new Result(inputPage, null, null, failure);
        }

        void releaseOnAnyThread() {
            if (inputPage != null) {
                releasePageOnAnyThread(inputPage);
            }
            if (outputPage != null) {
                releasePageOnAnyThread(outputPage);
            }
            if (deferredBlocks != null) {
                Releasables.closeExpectNoException(deferredBlocks);
            }
        }
    }

    ExternalFieldExtractOperator(
        int rowPositionChannel,
        List<Integer> passThroughChannels,
        List<String> deferredColumnNames,
        List<DataType> deferredColumnTypes,
        SourceExtractors registry,
        BlockFactory blockFactory
    ) {
        this(
            rowPositionChannel,
            passThroughChannels,
            deferredColumnNames,
            deferredColumnTypes,
            registry,
            new DriverContext(blockFactory.bigArrays(), blockFactory, null),
            Runnable::run
        );
    }

    ExternalFieldExtractOperator(
        int rowPositionChannel,
        List<Integer> passThroughChannels,
        List<String> deferredColumnNames,
        List<DataType> deferredColumnTypes,
        SourceExtractors registry,
        DriverContext driverContext,
        Executor executor
    ) {
        // Materialization does not produce response headers; AsyncOperator still requires a ThreadContext.
        super(driverContext, new ThreadContext(Settings.EMPTY), 1);
        this.rowPositionChannel = rowPositionChannel;
        this.passThroughChannels = passThroughChannels;
        this.deferredColumnNames = deferredColumnNames;
        this.deferredColumnTypes = deferredColumnTypes;
        this.registry = registry;
        this.registryRefs = new RefCountingRunnable(registry::close);
        this.blockFactory = driverContext.blockFactory();
        this.executor = executor;
    }

    @Override
    protected void performAsync(Page page, ActionListener<Result> listener) {
        if (page.getPositionCount() == 0) {
            Page output = null;
            Throwable failure = null;
            try {
                output = reshapeEmpty(page);
            } catch (Throwable t) {
                failure = t;
            } finally {
                Releasables.closeExpectNoException(page::releaseBlocks);
            }
            listener.onResponse(failure == null ? Result.output(output) : Result.failure(null, failure));
            return;
        }

        final long[] refs;
        try {
            refs = rowReferences(page);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        } catch (Error e) {
            releasePageOnAnyThread(page);
            throw e;
        }

        SubscribableListener<Void> ready = new SubscribableListener<>();
        materializationBlocked = new IsBlockedResult(ready, "external field materialization");
        var registryRef = Releasables.releaseOnce(registryRefs.acquire());
        ActionListener<Result> completion = ActionListener.releaseAfter(
            ActionListener.runAfter(listener, () -> ready.onResponse(null)),
            registryRef
        );
        try {
            executor.execute(() -> {
                long start = System.nanoTime();
                long cpuStart = ThreadCpuTimer.currentNanos();
                Result result;
                try {
                    driverContext().checkForEarlyTermination();
                    result = Result.materialized(
                        page,
                        registry.materialize(refs, refs.length, deferredColumnNames, deferredColumnTypes, blockFactory.parent())
                    );
                } catch (Throwable t) {
                    result = Result.failure(page, t);
                } finally {
                    extractNanos.add(System.nanoTime() - start);
                    if (cpuStart >= 0) {
                        extractCpuNanos.add(ThreadCpuTimer.elapsedNanos(cpuStart));
                    }
                }
                completion.onResponse(result);
            });
        } catch (RuntimeException e) {
            completion.onFailure(ExternalFailures.classify(e));
        } catch (Error e) {
            registryRef.close();
            ready.onResponse(null);
            releasePageOnAnyThread(page);
            throw e;
        }
    }

    @Override
    public Page getOutput() {
        Result result = fetchFromBuffer();
        if (result == null) {
            return null;
        }
        if (result.outputPage != null) {
            return result.outputPage;
        }
        if (result.failure != null) {
            try {
                throw ExternalFailures.classify(result.failure);
            } finally {
                if (result.inputPage != null) {
                    Releasables.closeExpectNoException(result.inputPage::releaseBlocks);
                }
            }
        }
        try {
            Page out = assemble(result.inputPage, result.deferredBlocks);
            rowsExtracted.add(out.getPositionCount());
            return out;
        } finally {
            Releasables.closeExpectNoException(result.inputPage::releaseBlocks);
        }
    }

    @Override
    protected Status status(long receivedPages, long completedPages, long processNanos) {
        return new Status(receivedPages, completedPages, processNanos, rowsExtracted.sum(), extractNanos.sum(), extractCpuNanos.sum());
    }

    @Override
    public IsBlockedResult isBlocked() {
        IsBlockedResult blocked = materializationBlocked;
        return blocked.listener().isDone() ? super.isBlocked() : blocked;
    }

    /**
     * For an empty input page, build a shape-correct empty output: drop {@code _rowPosition},
     * keep the pass-through blocks (incRef'd), and append empty placeholder blocks for the
     * deferred columns. The input page is released synchronously after this method returns;
     * the returned page owns the retained pass-through references.
     */
    private Page reshapeEmpty(Page page) {
        Block[] outBlocks = new Block[passThroughChannels.size() + deferredColumnNames.size()];
        try {
            int idx = 0;
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
            return new Page(0, outBlocks);
        } catch (Throwable e) {
            Releasables.closeExpectNoException(outBlocks);
            throw e;
        }
    }

    /** Validates and copies the encoded row references before materialization leaves the driver thread. */
    private long[] rowReferences(Page page) {
        int positions = page.getPositionCount();
        Block rpBlock = page.getBlock(rowPositionChannel);
        if (rpBlock instanceof LongBlock == false) {
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
                    throw new IllegalStateException(
                        "_rowPosition channel [" + rowPositionChannel + "] at position [" + i + "] must hold exactly one non-null value"
                    );
                }
                refs[i] = rp.getLong(rp.getFirstValueIndex(i));
            }
        }

        return refs;
    }

    private Page assemble(Page page, Block[] deferredBlocks) {
        int positions = page.getPositionCount();
        Block[] outBlocks = new Block[passThroughChannels.size() + deferredBlocks.length];
        try {
            int idx = 0;
            for (int ch : passThroughChannels) {
                Block b = page.getBlock(ch);
                b.incRef();
                outBlocks[idx++] = b;
            }
            for (int i = 0; i < deferredBlocks.length; i++) {
                outBlocks[idx++] = deferredBlocks[i];
                deferredBlocks[i] = null;
            }
            return new Page(positions, outBlocks);
        } catch (Throwable e) {
            Releasables.closeExpectNoException(outBlocks);
            Releasables.closeExpectNoException(deferredBlocks);
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
    protected void releaseFetchedOnAnyThread(Result result) {
        result.releaseOnAnyThread();
    }

    @Override
    protected void doClose() {
        // Do not tie registry closure to DriverContext.finish(): a later operator factory may fail
        // after constructing this operator, in which case the context is never finished. Pending
        // materializations retain their own refs and close the registry when the last one completes.
        registryRefs.close();
    }

    /**
     * Per-driver counters for {@link ExternalFieldExtractOperator}, surfaced as the operator's
     * {@code status} in the profile: pages processed, rows whose deferred columns were materialized,
     * and wall time in {@code materialize(...)}. Wire-gated by {@code esql_external_source_profile}
     * so older nodes round-trip a zero-valued status.
     */
    public static class Status extends AsyncOperator.Status {

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "external_field_extract",
            Status::new
        );

        private static final TransportVersion ESQL_EXTERNAL_SOURCE_PROFILE = TransportVersion.fromName("esql_external_source_profile");
        private static final TransportVersion ESQL_EXTRACT_CPU_NANOS = TransportVersion.fromName("esql_extract_cpu_nanos");

        private final long rowsExtracted;
        private final long extractNanos;
        private final long extractCpuNanos;

        public Status(long pagesProcessed, long rowsExtracted, long extractNanos, long extractCpuNanos) {
            this(pagesProcessed, pagesProcessed, extractNanos, rowsExtracted, extractNanos, extractCpuNanos);
        }

        private Status(
            long receivedPages,
            long completedPages,
            long processNanos,
            long rowsExtracted,
            long extractNanos,
            long extractCpuNanos
        ) {
            super(receivedPages, completedPages, processNanos);
            this.rowsExtracted = rowsExtracted;
            this.extractNanos = extractNanos;
            this.extractCpuNanos = extractCpuNanos;
        }

        Status(StreamInput in) throws IOException {
            this(readSerialized(in));
        }

        private Status(Serialized serialized) {
            this(
                serialized.pagesProcessed,
                serialized.pagesProcessed,
                serialized.extractNanos,
                serialized.rowsExtracted,
                serialized.extractNanos,
                serialized.extractCpuNanos
            );
        }

        private static Serialized readSerialized(StreamInput in) throws IOException {
            // The operator + its Status only exist on nodes that support deferred extraction
            // (elasticsearch#149185), which landed alongside this PR. Pre-version nodes never
            // send this entry, but be defensive and accept either shape.
            long pagesProcessed;
            long rowsExtracted;
            long extractNanos;
            if (in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
                pagesProcessed = in.readVLong();
                rowsExtracted = in.readVLong();
                extractNanos = in.readVLong();
            } else {
                pagesProcessed = 0L;
                rowsExtracted = 0L;
                extractNanos = 0L;
            }
            long extractCpuNanos = in.getTransportVersion().supports(ESQL_EXTRACT_CPU_NANOS) ? in.readVLong() : 0L;
            return new Serialized(pagesProcessed, rowsExtracted, extractNanos, extractCpuNanos);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // Keep the existing wire layout. The standard async counters are exact locally; after
            // transport they are reconstructed from these legacy counters.
            if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
                out.writeVLong(completedPages());
                out.writeVLong(rowsExtracted);
                out.writeVLong(extractNanos);
            }
            if (out.getTransportVersion().supports(ESQL_EXTRACT_CPU_NANOS)) {
                out.writeVLong(extractCpuNanos);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public long pagesProcessed() {
            return completedPages();
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
        public long readCpuNanos() {
            return extractCpuNanos;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, org.elasticsearch.xcontent.ToXContent.Params params) throws IOException {
            builder.startObject();
            innerToXContent(builder);
            builder.field("pages_processed", completedPages());
            builder.field("rows_extracted", rowsExtracted);
            builder.field("extract_nanos", extractNanos);
            builder.field("extract_cpu_nanos", extractCpuNanos);
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
            if (super.equals(o) == false) {
                return false;
            }
            Status status = (Status) o;
            return rowsExtracted == status.rowsExtracted
                && extractNanos == status.extractNanos
                && extractCpuNanos == status.extractCpuNanos;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), rowsExtracted, extractNanos, extractCpuNanos);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }

        private record Serialized(long pagesProcessed, long rowsExtracted, long extractNanos, long extractCpuNanos) {}
    }
}
