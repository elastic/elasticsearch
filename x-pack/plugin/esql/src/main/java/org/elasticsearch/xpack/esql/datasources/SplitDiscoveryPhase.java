/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
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
 *
 * <p><b>Two trees, one rule.</b> Which filters guard a given {@link ExternalSourceExec} is decided here for both shapes
 * the relation can arrive in: the physical plan (walked by {@code resolveRecursive}, collecting {@link FilterExec}) and
 * a fragment's logical plan (walked by {@link #guardedRelations}, collecting {@link Filter}). Both exist because a
 * {@code FROM <dataset>} lowers through a {@code FragmentExec} whose body is still a {@link LogicalPlan}. Keeping the two
 * walks adjacent, sharing one rule, is what stops them drifting apart: a filter recovered on one path and not the other
 * would prune inconsistently, and on the fragment path the relation arrives stripped of the {@code Filter} that stood
 * above it, so the conjuncts have to be recovered here or the matcher never sees the predicate at all.
 *
 * <p>This decides which files are <em>read</em>. It is not the only place a partition filter is consulted:
 * {@link PartitionFilterHintExtractor} runs earlier, pre-resolution, and lets {@code GlobExpander} skip <em>listing</em>
 * whole folders. Nothing here can compensate for a file that was never listed, so both layers obey the rules in
 * {@link PartitionPruningRule} — the pre-resolution layer held to a stricter one, since it has only names to go on where
 * this side binds by {@code NameId}.
 */
public final class SplitDiscoveryPhase {

    private SplitDiscoveryPhase() {}

    /**
     * An {@link ExternalRelation} found inside a fragment, paired with the conjuncts of the {@link Filter} ancestors
     * that guard it — the seed for its partition pruning. Empty {@code filters} means the relation is unfiltered and
     * every file must be read.
     */
    public record GuardedRelation(ExternalRelation relation, List<Expression> filters) {
        public GuardedRelation {
            filters = List.copyOf(filters);
        }
    }

    /**
     * Pairs every {@link ExternalRelation} in a fragment's logical plan with the AND-split conjuncts of the
     * {@link Filter} ancestors above it.
     *
     * <p>Split discovery lowers each relation to a standalone {@link ExternalSourceExec} via
     * {@code ExternalRelation.toPhysicalExec()}, which drops the surrounding plan — and with it the {@code Filter} that
     * was sitting above the relation. Recovering the conjuncts here, before that lowering, is what lets partition
     * pruning see the predicate at all; without them {@code FileSplitProvider.matchesPartitionFilters} runs on an empty
     * filter set and every partition folder is read.
     *
     * <p>The conjuncts are <em>candidates</em>, not commands: {@link #resolveExternalSource} binds each one to the
     * relation's own output by {@code NameId} before it may prune, so a filter over a downstream-generated column that
     * merely shares a partition column's name cannot mis-prune.
     */
    public static List<GuardedRelation> guardedRelations(LogicalPlan fragment) {
        List<GuardedRelation> guarded = new ArrayList<>();
        collectGuardedRelations(fragment, List.of(), guarded);
        return guarded;
    }

    private static void collectGuardedRelations(LogicalPlan plan, List<Expression> ancestorFilters, List<GuardedRelation> guarded) {
        if (plan instanceof ExternalRelation external) {
            guarded.add(new GuardedRelation(external, ancestorFilters));
            return;
        }

        List<Expression> filtersForChildren = PartitionPruningRule.rowPreserving(plan) ? ancestorFilters : List.of();
        if (plan instanceof Filter filter) {
            List<Expression> extended = new ArrayList<>(filtersForChildren);
            extended.addAll(splitAnd(filter.condition()));
            filtersForChildren = List.copyOf(extended);
        }

        for (LogicalPlan child : plan.children()) {
            collectGuardedRelations(child, filtersForChildren, guarded);
        }
    }

    /**
     * Scan accounting collected while resolving splits, surfaced at the root of the query profile. The
     * counts reflect what survived coordinator-side pruning and is handed to the runtime, before any later
     * split coalescing.
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
     * signal into each {@link SplitDiscoveryContext} so a long-running discovery aborts promptly on cancel.
     */
    public static Result resolveExternalSplitsWithStats(
        PhysicalPlan plan,
        Map<String, ExternalSourceFactory> sourceFactories,
        int maxRecordBytes,
        BooleanSupplier isCancelled
    ) {
        return resolveExternalSplitsWithStats(plan, sourceFactories, maxRecordBytes, isCancelled, List.of());
    }

    /**
     * Like {@link #resolveExternalSplitsWithStats(PhysicalPlan, Map, int, BooleanSupplier)}, but seeds the recursive
     * walk with {@code seedFilters} — conjuncts that guard this sub-plan from <em>above</em> and so cannot be recovered
     * from the tree itself, as produced by {@link #guardedRelations}.
     *
     * <p>Needed because every {@code FROM <dataset>} is wrapped in a {@code FragmentExec}: the coordinator physical-plan
     * walk never reaches a top-level {@link ExternalSourceExec}, discovery runs on the fragment path, and the relation
     * arrives there stripped of its {@code Filter} ancestor. Without the seed, partition pruning would not fire on any
     * production query — single-node included, not only distributed ones.
     *
     * <p>The seed is not blindly trusted: {@link #resolveExternalSource} binds each conjunct to the relation's output by
     * {@link NameId} before it may prune, so a filter over a downstream-generated column that merely shares a partition
     * column's name cannot mis-prune.
     */
    public static Result resolveExternalSplitsWithStats(
        PhysicalPlan plan,
        Map<String, ExternalSourceFactory> sourceFactories,
        int maxRecordBytes,
        BooleanSupplier isCancelled,
        List<Expression> seedFilters
    ) {
        ScanStats stats = new ScanStats();
        PhysicalPlan resolved = resolveRecursive(plan, seedFilters, sourceFactories, maxRecordBytes, stats, isCancelled);
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

        List<Expression> filtersForChildren = PartitionPruningRule.rowPreserving(plan) ? ancestorFilters : List.of();
        if (plan instanceof FilterExec filterExec) {
            List<Expression> extended = new ArrayList<>(filtersForChildren);
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

        // Bind filter hints to the relation's output by NameId, not by name. A downstream EVAL/DISSECT/GROK/ENRICH can
        // introduce an attribute that SHARES A NAME with a partition column (e.g. `EVAL year = ...`) whose filter
        // `PushDownAndCombineFilters` may leave above the generating node and thus in ancestorFilters. Pruning by the
        // path partition value for such a row-derived column produces silently wrong answers. Keeping only conjuncts
        // whose every attribute reference resolves by id into exec.output() drops those shadowing filters; the split
        // provider's per-file matcher then sees only genuine partition/data-column predicates.
        List<Expression> boundFilters = filtersBoundToOutput(ancestorFilters, exec.output());

        SplitDiscoveryContext context = new SplitDiscoveryContext(
            null,
            fileList != null ? fileList : FileList.UNRESOLVED,
            exec.schemaMap(),
            exec.config(),
            partitionInfo,
            boundFilters,
            querySchema,
            exec.unifiedSchema(),
            maxRecordBytes,
            isCancelled,
            exec.declaredReadSpec()
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

    /**
     * Retains only those conjuncts whose every referenced attribute resolves by {@link NameId} into {@code output}.
     * {@link AttributeSet} keys on the attribute id (semantic equality), so a conjunct referencing an attribute that
     * merely shares a name with an output column — but was produced by a downstream {@code EVAL}/{@code DISSECT}/etc.
     * with a distinct id — is dropped. A conjunct that references an attribute genuinely in the relation's output
     * (a partition column or a data column) is kept.
     */
    private static List<Expression> filtersBoundToOutput(List<Expression> filters, List<Attribute> output) {
        if (filters.isEmpty()) {
            return filters;
        }
        AttributeSet outputSet = AttributeSet.of(output);
        List<Expression> bound = new ArrayList<>(filters.size());
        for (Expression filter : filters) {
            if (outputSet.containsAll(filter.references())) {
                bound.add(filter);
            }
        }
        return bound;
    }
}
