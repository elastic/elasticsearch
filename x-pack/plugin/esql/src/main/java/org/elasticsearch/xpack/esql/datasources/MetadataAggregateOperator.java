/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.MetadataAggregateReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Source operator that emits one intermediate-shape aggregate page per file split, by asking
 * the format reader for per-split metadata (row count / null-count / min / max) instead of
 * scanning row data.
 * <p>
 * Created by {@code LocalExecutionPlanner} from an {@code ExternalMetadataAggregateExec}.
 * Each driver instance pulls splits from a shared {@link ExternalSliceQueue}; for each
 * {@link FileSplit} the operator opens a metadata-only {@link StorageObject} via the supplied
 * {@link StorageProvider} and calls
 * {@link MetadataAggregateReader#aggregateMetadata(StorageObject, List, long, long)} with the
 * split's byte range. The reader returns stats covering only the row groups whose starting
 * offset falls in that range, so disjoint splits over the same file contribute disjoint sums
 * and the FINAL reducer produces correct totals without any cross-driver coordination.
 * <p>
 * If a split's metadata is unavailable (reader returns {@code null}), the operator emits a
 * page of constant-null intermediate blocks with {@code seen=false}, which the reducer treats
 * as "no contribution" rather than silently dropping data.
 */
public final class MetadataAggregateOperator extends SourceOperator {

    private static final Logger logger = LogManager.getLogger(MetadataAggregateOperator.class);

    private final BlockFactory blockFactory;
    private final StorageProvider storageProvider;
    private final MetadataAggregateReader reader;
    private final ExternalSliceQueue sliceQueue;
    private final List<NamedExpression> aggregates;
    private final List<String> columnsToProbe;

    /**
     * Buffered children unwrapped from the most recently-claimed {@link CoalescedSplit}. Each
     * leaf {@link FileSplit} produces exactly one output page; a coalesced split contributes
     * multiple output pages, drained one per {@link #getOutput()} call to keep memory bounded
     * and respect downstream backpressure.
     */
    private final ArrayDeque<FileSplit> pendingLeaves = new ArrayDeque<>();

    private boolean finished;

    public MetadataAggregateOperator(
        BlockFactory blockFactory,
        StorageProvider storageProvider,
        MetadataAggregateReader reader,
        ExternalSliceQueue sliceQueue,
        List<NamedExpression> aggregates,
        List<String> columnsToProbe
    ) {
        this.blockFactory = blockFactory;
        this.storageProvider = storageProvider;
        this.reader = reader;
        this.sliceQueue = sliceQueue;
        this.aggregates = aggregates;
        this.columnsToProbe = columnsToProbe;
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
        FileSplit fs = nextLeaf();
        if (fs == null) {
            finished = true;
            return null;
        }
        StoragePath path = fs.path();
        long rangeStart = fs.offset();
        // length() can be Long.MAX_VALUE (unbounded); guard against overflow.
        long rangeEnd = (fs.length() >= Long.MAX_VALUE - rangeStart) ? Long.MAX_VALUE : rangeStart + fs.length();
        SplitStats stats;
        try {
            StorageObject object = storageProvider.newObject(path);
            stats = reader.aggregateMetadata(object, columnsToProbe, rangeStart, rangeEnd);
        } catch (IOException e) {
            // Metadata read failure: emit an empty (seen=false) page so the reducer sees no
            // contribution from this split. Preserves correctness over fail-fast since the
            // optimizer rule is conservative — but log loudly so operators can investigate.
            logger.warn(() -> Strings.format("metadata aggregate failed for [%s]", fs.path()), e);
            stats = null;
        }
        return buildIntermediatePage(stats);
    }

    /**
     * Returns the next leaf {@link FileSplit}, transparently unwrapping {@link CoalescedSplit}
     * containers. Children are buffered in {@link #pendingLeaves} so we emit exactly one page
     * per file (matching the shape of intermediate aggregation state).
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
            throw new IllegalStateException("MetadataAggregateOperator received unsupported split: " + split.getClass().getName());
        }
    }

    /**
     * Builds the intermediate-shape page for one split: per aggregate, two blocks — a typed
     * value block and a {@code seen} boolean. A {@code null} stats result yields a page where
     * every aggregate is unseen (constant-null value, {@code seen=false}).
     */
    private Page buildIntermediatePage(SplitStats stats) {
        Block[] blocks = new Block[aggregates.size() * 2];
        try {
            for (int i = 0; i < aggregates.size(); i++) {
                NamedExpression agg = aggregates.get(i);
                Expression child = agg instanceof Alias alias ? alias.child() : agg;
                Object value = stats != null ? resolveAggregateValue(child, stats) : null;
                DataType dataType = child instanceof AggregateFunction af ? af.dataType() : DataType.LONG;
                if (value == null) {
                    blocks[i * 2] = blockFactory.newConstantNullBlock(1);
                    blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(false, 1);
                } else {
                    blocks[i * 2] = buildValueBlock(value, dataType);
                    blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
                }
            }
            Page page = new Page(blocks);
            blocks = null; // ownership transferred
            return page;
        } finally {
            if (blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    /**
     * Resolves a single aggregate function against the per-file stats. Mirrors the planning-time
     * logic in {@code PushAggregatesToExternalSource#resolveFromStats} so SINGLE/INITIAL/FINAL
     * paths produce equivalent values.
     */
    private static Object resolveAggregateValue(Expression aggFunction, SplitStats stats) {
        if (aggFunction instanceof Count count) {
            if (count.hasFilter()) {
                return null;
            }
            Expression target = count.field();
            if (target.foldable()) {
                return stats.rowCount();
            }
            if (target instanceof Attribute ref) {
                long nc = stats.columnNullCount(ref.name());
                if (nc >= 0) {
                    return stats.rowCount() - nc;
                }
            }
            return null;
        }
        if (aggFunction instanceof Min min) {
            if (min.hasFilter()) {
                return null;
            }
            if (min.field() instanceof Attribute ref) {
                return stats.columnMin(ref.name());
            }
            return null;
        }
        if (aggFunction instanceof Max max) {
            if (max.hasFilter()) {
                return null;
            }
            if (max.field() instanceof Attribute ref) {
                return stats.columnMax(ref.name());
            }
            return null;
        }
        return null;
    }

    /**
     * Constructs a single-row constant value block matching the aggregate's ESQL data type.
     * Format readers may return wider Java types than the column's declared type
     * (e.g. {@code long} for an INT32 column), so we coerce.
     */
    private Block buildValueBlock(Object value, DataType dataType) {
        return switch (dataType) {
            case INTEGER -> blockFactory.newConstantIntBlockWith(((Number) value).intValue(), 1);
            case LONG, COUNTER_LONG, DATETIME -> blockFactory.newConstantLongBlockWith(((Number) value).longValue(), 1);
            case DOUBLE, COUNTER_DOUBLE -> blockFactory.newConstantDoubleBlockWith(((Number) value).doubleValue(), 1);
            case BOOLEAN -> blockFactory.newConstantBooleanBlockWith(
                value instanceof Boolean b ? b : Booleans.parseBoolean(value.toString()),
                1
            );
            case KEYWORD, TEXT -> blockFactory.newConstantBytesRefBlockWith(new BytesRef(value.toString()), 1);
            default -> {
                if (value instanceof Number n) {
                    yield blockFactory.newConstantLongBlockWith(n.longValue(), 1);
                }
                yield blockFactory.newConstantNullBlock(1);
            }
        };
    }

    @Override
    public void close() {
        // Nothing to release: StorageObject instances do not hold resources (see StorageObject
        // Javadoc); InputStreams opened by the reader are closed by the reader. The reader and
        // storage provider are owned by the factory.
    }

    @Override
    public String toString() {
        return "MetadataAggregateOperator[aggregates=" + aggregates.size() + ", probe=" + columnsToProbe + "]";
    }

    /**
     * Factory for {@link MetadataAggregateOperator}. Holds the shared {@link ExternalSliceQueue},
     * format reader and storage provider; one operator instance per driver pulls splits from
     * the queue.
     */
    public static final class Factory implements SourceOperatorFactory {

        private final StorageProvider storageProvider;
        private final MetadataAggregateReader reader;
        private final ExternalSliceQueue sliceQueue;
        private final List<NamedExpression> aggregates;
        private final List<String> columnsToProbe;

        public Factory(
            StorageProvider storageProvider,
            MetadataAggregateReader reader,
            ExternalSliceQueue sliceQueue,
            List<NamedExpression> aggregates,
            List<String> columnsToProbe
        ) {
            this.storageProvider = storageProvider;
            this.reader = reader;
            this.sliceQueue = sliceQueue;
            this.aggregates = List.copyOf(aggregates);
            this.columnsToProbe = columnsToProbe != null ? List.copyOf(columnsToProbe) : List.of();
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new MetadataAggregateOperator(
                driverContext.blockFactory(),
                storageProvider,
                reader,
                sliceQueue,
                aggregates,
                new ArrayList<>(columnsToProbe)
            );
        }

        @Override
        public String describe() {
            return "MetadataAggregateOperator[splits=" + sliceQueue.totalSlices() + ", aggregates=" + aggregates.size() + "]";
        }
    }
}
