/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanReader;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanSpec;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanSpec.AggOp;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Source operator that drives an {@link AggregateScanReader}: per leaf {@link FileSplit},
 * opens a {@link CloseableIterator} of intermediate-state pages from the reader and forwards
 * one page per {@link #getOutput()} call. The reader chooses internally, per row group,
 * whether to derive the page from column statistics or by scanning row data.
 * <p>
 * Created by {@code LocalExecutionPlanner} from an {@code ExternalAggregatePushdownExec}.
 * Each driver instance pulls splits from a shared {@link ExternalSliceQueue}. The reader's
 * iterator covers exactly the row groups whose starting offset falls in the split's byte
 * range — Parquet's {@code withRange} convention — so disjoint splits over the same file
 * contribute disjoint sums and the FINAL reducer produces correct totals without any
 * cross-driver coordination.
 * <p>
 * Any failure — opening the file, or mid-iteration — is propagated to fail the query,
 * matching the standard scan path's behavior. Silently substituting an unseen page would
 * under-count {@code COUNT(*)} / {@code COUNT(field)} without any signal to the user, and
 * is inconsistent with how the same data source behaves under {@code FROM file | LIMIT n}.
 */
public final class AggregateScanOperator extends SourceOperator {

    private static final Logger logger = LogManager.getLogger(AggregateScanOperator.class);

    private final BlockFactory blockFactory;
    private final StorageProvider storageProvider;
    private final AggregateScanReader reader;
    private final ExternalSliceQueue sliceQueue;
    private final AggregateScanSpec spec;

    /**
     * Buffered children unwrapped from the most recently-claimed {@link CoalescedSplit}.
     * Each leaf {@link FileSplit} produces 1..N output pages (one per row group); a
     * coalesced split contributes pages from multiple files, drained one per
     * {@link #getOutput()} call to keep memory bounded and respect downstream backpressure.
     */
    private final ArrayDeque<FileSplit> pendingLeaves = new ArrayDeque<>();

    /**
     * Iterator over the currently-open file's per-row-group intermediate-state pages.
     * Held across {@link #getOutput()} calls until exhausted, then closed before opening
     * the next leaf.
     */
    private CloseableIterator<Page> currentIterator;
    private FileSplit currentLeaf;

    private boolean finished;

    public AggregateScanOperator(
        BlockFactory blockFactory,
        StorageProvider storageProvider,
        AggregateScanReader reader,
        ExternalSliceQueue sliceQueue,
        AggregateScanSpec spec
    ) {
        this.blockFactory = blockFactory;
        this.storageProvider = storageProvider;
        this.reader = reader;
        this.sliceQueue = sliceQueue;
        this.spec = spec;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public Page getOutput() {
        if (finished) {
            return null;
        }
        while (true) {
            // Drain the current iterator first.
            if (currentIterator != null) {
                try {
                    if (currentIterator.hasNext()) {
                        return currentIterator.next();
                    }
                } catch (RuntimeException e) {
                    // Earlier row-group pages from this file have already been forwarded to
                    // the reducer; emitting an unseen page now would silently truncate the
                    // aggregate. Fail the query instead. File-open failures (handled below)
                    // are still lenient because no pages have been emitted yet.
                    logger.warn(() -> Strings.format("aggregate scan failed mid-iteration for [%s]", currentLeaf.path()), e);
                    closeCurrentIteratorQuietly();
                    throw e;
                }
                closeCurrentIteratorQuietly();
            }
            // Iterator exhausted (or none open) — advance to the next leaf.
            FileSplit fs = nextLeaf();
            if (fs == null) {
                finished = true;
                return null;
            }
            currentLeaf = fs;
            try {
                StoragePath path = fs.path();
                long rangeStart = fs.offset();
                // length() can be Long.MAX_VALUE (unbounded); guard against overflow.
                long rangeEnd = (fs.length() >= Long.MAX_VALUE - rangeStart) ? Long.MAX_VALUE : rangeStart + fs.length();
                StorageObject object = storageProvider.newObject(path);
                currentIterator = reader.scanForAggregates(object, spec, blockFactory, rangeStart, rangeEnd);
            } catch (IOException e) {
                // Fail the query: silently substituting an unseen page would under-count the
                // aggregate. Matches the standard scan path (AsyncExternalSourceOperatorFactory).
                logger.warn(() -> Strings.format("aggregate scan failed to open [%s]", currentLeaf.path()), e);
                throw new UncheckedIOException(e);
            }
            // Loop back: pull the first page from the freshly-opened iterator.
        }
    }

    /**
     * Returns the next leaf {@link FileSplit}, transparently unwrapping {@link CoalescedSplit}
     * containers.
     */
    private FileSplit nextLeaf() {
        while (true) {
            FileSplit buffered = pendingLeaves.pollFirst();
            if (buffered != null) {
                return buffered;
            }
            ExternalSplit split = sliceQueue.nextSplit();
            if (split == null) {
                return null;
            }
            enqueueLeaves(split);
        }
    }

    private void enqueueLeaves(ExternalSplit split) {
        if (split instanceof FileSplit fs) {
            pendingLeaves.add(fs);
        } else if (split instanceof CoalescedSplit cs) {
            for (ExternalSplit child : cs.children()) {
                enqueueLeaves(child);
            }
        } else {
            throw new IllegalStateException("AggregateScanOperator received unsupported split: " + split.getClass().getName());
        }
    }

    private void closeCurrentIteratorQuietly() {
        IOUtils.closeWhileHandlingException(currentIterator);
        currentIterator = null;
    }

    @Override
    public void close() {
        closeCurrentIteratorQuietly();
    }

    @Override
    public String toString() {
        return "AggregateScanOperator[ops=" + spec.ops() + "]";
    }

    /**
     * Factory for {@link AggregateScanOperator}. Holds the shared {@link ExternalSliceQueue},
     * format reader, and storage provider; one operator instance per driver pulls splits from
     * the queue. The factory is also where {@code List<NamedExpression> aggregates} from the
     * plan node is lowered to a stable {@link AggregateScanSpec} once.
     */
    public static final class Factory implements SourceOperatorFactory {

        private final StorageProvider storageProvider;
        private final AggregateScanReader reader;
        private final ExternalSliceQueue sliceQueue;
        private final AggregateScanSpec spec;

        public Factory(
            StorageProvider storageProvider,
            AggregateScanReader reader,
            ExternalSliceQueue sliceQueue,
            List<NamedExpression> aggregates,
            List<Attribute> intermediateAttributes
        ) {
            this.storageProvider = storageProvider;
            this.reader = reader;
            this.sliceQueue = sliceQueue;
            this.spec = lower(aggregates, intermediateAttributes);
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new AggregateScanOperator(driverContext.blockFactory(), storageProvider, reader, sliceQueue, spec);
        }

        @Override
        public String describe() {
            return "AggregateScanOperator[splits=" + sliceQueue.totalSlices() + ", ops=" + spec.ops() + "]";
        }

        /**
         * Lower the plan node's aggregate expressions to a stable {@link AggregateScanSpec}.
         * The planner rule has already validated each aggregate is a supported shape (one of
         * {@code COUNT(*)}, {@code COUNT(field)}, {@code MIN(field)}, {@code MAX(field)} with
         * no filter); this method re-validates and throws on anything unexpected so a caller
         * that bypasses the planner cannot silently drop a filter.
         */
        static AggregateScanSpec lower(List<NamedExpression> aggregates, List<Attribute> intermediateAttributes) {
            List<AggOp> ops = new ArrayList<>(aggregates.size());
            for (NamedExpression ne : aggregates) {
                if (ne instanceof Alias alias && alias.child() instanceof Expression child) {
                    ops.add(toOp(child));
                } else {
                    throw new IllegalArgumentException("expected Alias(AggregateFunction), got: " + ne);
                }
            }
            return new AggregateScanSpec(ops, intermediateAttributes);
        }

        private static AggOp toOp(Expression child) {
            if (child instanceof Count count) {
                rejectFilter(count.hasFilter(), child);
                Expression target = count.field();
                if (target.foldable()) {
                    return new AggOp.CountStar();
                }
                if (target instanceof Attribute ref) {
                    return new AggOp.CountField(ref.name());
                }
                throw new IllegalArgumentException("unsupported Count target: " + target);
            }
            if (child instanceof Min min && min.field() instanceof Attribute ref) {
                rejectFilter(min.hasFilter(), child);
                return new AggOp.MinField(ref.name());
            }
            if (child instanceof Max max && max.field() instanceof Attribute ref) {
                rejectFilter(max.hasFilter(), child);
                return new AggOp.MaxField(ref.name());
            }
            throw new IllegalArgumentException("unsupported aggregate: " + child);
        }

        /**
         * {@link AggOp} carries no filter information, so a filtered aggregate ({@code .. WHERE ..})
         * cannot be lowered without silently dropping the filter and returning wrong results. The
         * planner rule excludes filtered aggregates upstream; throw loudly if anything bypasses
         * that contract (e.g. a future caller, or a plan-node round-trip).
         */
        private static void rejectFilter(boolean hasFilter, Expression aggFunction) {
            if (hasFilter) {
                throw new IllegalArgumentException("aggregate-scan operator does not support filtered aggregates: " + aggFunction);
            }
        }
    }
}
