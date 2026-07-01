/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.splitAnd;

/**
 * Walks the physical plan tree, discovers splits for each {@link ExternalSourceExec},
 * and replaces them with split-enriched copies via {@link ExternalSourceExec#withSplits}.
 *
 * <p>Filter expressions from {@link FilterExec} ancestors are collected per-source so that
 * each {@link ExternalSourceExec} only receives filters from its own ancestor chain, not
 * from unrelated branches of the plan tree.
 */
public final class SplitDiscoveryPhase {

    private SplitDiscoveryPhase() {}

    /**
     * Post-prune "scanned" accounting collected while resolving splits, surfaced at the root of the
     * query profile. The counts reflect what survived coordinator-side pruning and is handed to the
     * runtime, before any later split coalescing.
     *
     * @param plan          the split-enriched physical plan
     * @param filesScanned  distinct files contributing splits (file-based sources only; {@code 0} otherwise)
     * @param splitsScanned total number of discovered splits across all external sources
     * @param bytesScanned  sum of {@link ExternalSplit#estimatedSizeInBytes()} over the discovered splits,
     *                      ignoring splits that report an unknown ({@code < 0}) size
     */
    public record Result(PhysicalPlan plan, int filesScanned, int splitsScanned, long bytesScanned) {}

    /** Mutable accumulator threaded through the recursive walk. */
    private static final class ScanStats {
        private int filesScanned;
        private int splitsScanned;
        private long bytesScanned;
    }

    public static PhysicalPlan resolveExternalSplits(PhysicalPlan plan, Map<String, ExternalSourceFactory> sourceFactories) {
        return resolveExternalSplits(
            plan,
            sourceFactories,
            org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );
    }

    public static PhysicalPlan resolveExternalSplits(
        PhysicalPlan plan,
        Map<String, ExternalSourceFactory> sourceFactories,
        int maxRecordBytes
    ) {
        return resolveExternalSplitsWithStats(plan, sourceFactories, maxRecordBytes).plan();
    }

    /**
     * Like {@link #resolveExternalSplits}, but also returns the post-prune scanned counts aggregated
     * across every {@link ExternalSourceExec} in the plan.
     */
    public static Result resolveExternalSplitsWithStats(
        PhysicalPlan plan,
        Map<String, ExternalSourceFactory> sourceFactories,
        int maxRecordBytes
    ) {
        return resolveExternalSplitsWithStats(plan, sourceFactories, maxRecordBytes, () -> false);
    }

    /**
     * Like {@link #resolveExternalSplitsWithStats(PhysicalPlan, Map, int)}, but threads a cancellation
     * signal into each {@link SplitDiscoveryContext} so a long-running discovery (thousands of footer
     * reads) aborts promptly when the originating query is cancelled.
     */
    public static Result resolveExternalSplitsWithStats(
        PhysicalPlan plan,
        Map<String, ExternalSourceFactory> sourceFactories,
        int maxRecordBytes,
        BooleanSupplier isCancelled
    ) {
        ScanStats stats = new ScanStats();
        PhysicalPlan resolved = resolveRecursive(plan, List.of(), sourceFactories, maxRecordBytes, stats, isCancelled);
        return new Result(resolved, stats.filesScanned, stats.splitsScanned, stats.bytesScanned);
    }

    private static PhysicalPlan resolveRecursive(
        PhysicalPlan plan,
        List<Expression> ancestorFilters,
        Map<String, ExternalSourceFactory> sourceFactories,
        int maxRecordBytes,
        ScanStats stats,
        BooleanSupplier isCancelled
    ) {
        if (plan instanceof ExternalSourceExec exec) {
            return resolveExternalSource(exec, ancestorFilters, sourceFactories, maxRecordBytes, stats, isCancelled);
        }

        List<Expression> filtersForChildren = ancestorFilters;
        if (plan instanceof FilterExec filterExec) {
            List<Expression> extended = new ArrayList<>(ancestorFilters);
            for (Expression conjunction : splitAnd(filterExec.condition())) {
                extended.add(conjunction);
            }
            filtersForChildren = List.copyOf(extended);
        }

        List<PhysicalPlan> children = plan.children();
        if (children.isEmpty()) {
            return plan;
        }

        boolean changed = false;
        List<PhysicalPlan> newChildren = new ArrayList<>(children.size());
        for (PhysicalPlan child : children) {
            PhysicalPlan resolved = resolveRecursive(child, filtersForChildren, sourceFactories, maxRecordBytes, stats, isCancelled);
            if (resolved != child) {
                changed = true;
            }
            newChildren.add(resolved);
        }

        if (changed == false) {
            return plan;
        }

        if (plan instanceof UnaryExec unary && newChildren.size() == 1) {
            return unary.replaceChild(newChildren.get(0));
        }
        return plan.replaceChildren(newChildren);
    }

    private static PhysicalPlan resolveExternalSource(
        ExternalSourceExec exec,
        List<Expression> ancestorFilters,
        Map<String, ExternalSourceFactory> sourceFactories,
        int maxRecordBytes,
        ScanStats stats,
        BooleanSupplier isCancelled
    ) {
        ExternalSourceFactory factory = sourceFactories.get(exec.sourceType());
        SplitProvider splitProvider = factory != null ? factory.splitProvider() : SplitProvider.SINGLE;

        FileList fileList = exec.fileList();
        PartitionMetadata partitionInfo = fileList != null ? fileList.partitionMetadata() : null;

        List<Attribute> queryDataAttributes = new ArrayList<>(exec.output().size());
        for (Attribute attr : exec.output()) {
            if (attr instanceof MetadataAttribute == false) {
                queryDataAttributes.add(attr);
            }
        }
        ExternalSchema querySchema = new ExternalSchema(queryDataAttributes);

        SplitDiscoveryContext context = new SplitDiscoveryContext(
            null,
            fileList != null ? fileList : FileList.UNRESOLVED,
            exec.schemaMap(),
            exec.config(),
            partitionInfo,
            ancestorFilters,
            querySchema,
            exec.unifiedSchema(),
            maxRecordBytes,
            isCancelled
        );

        SplitDiscoveryResult result;
        try {
            result = splitProvider.discoverSplits(context);
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Exception e) {
            throw new ElasticsearchException(
                "failed to discover splits for external source [{}] of type [{}]",
                e,
                exec.sourcePath(),
                exec.sourceType()
            );
        }
        List<ExternalSplit> splits = result.splits();
        if (splits.isEmpty()) {
            return exec;
        }
        stats.filesScanned += result.filesScanned();
        stats.splitsScanned += splits.size();
        for (ExternalSplit split : splits) {
            long sizeInBytes = split.estimatedSizeInBytes();
            if (sizeInBytes > 0) {
                stats.bytesScanned += sizeInBytes;
            }
        }
        return exec.withSplits(splits);
    }
}
