/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.datasources.ExternalMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SyntheticColumns;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.optimizer.ExternalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.ExternalFieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Inserts a deferred field-extraction step above a per-driver {@link TopNExec} that sits on top of
 * an {@link ExternalSourceExec}. The forward scan is narrowed to the columns the TopN actually
 * uses (sort keys and any predicate / eval inputs traversed between TopN and the source) plus a
 * synthetic {@code _rowPosition} key. The remaining ("wide") columns are loaded later, for the
 * at-most-LIMIT surviving rows, by an {@link ExternalFieldExtractExec} placed immediately above
 * the TopN.
 * <p>
 * Net effect: a TopN that previously pulled every projected column out of the file now only reads
 * the sort key column up front and pays the I/O for the wide columns only for the rows that
 * survive — analogous to Lucene's late materialization for {@code SORT … | LIMIT N}.
 * <p>
 * Bail-out conditions (rule returns the plan unchanged):
 * <ul>
 *     <li>No {@link ExternalSourceExec} reachable below the TopN through a {@link UnaryExec} spine.</li>
 *     <li>{@link #supportsDeferredExtraction(String, ExternalOptimizerContext)} returns
 *         {@code false} for the source's format — the underlying reader does not implement
 *         {@link ColumnExtractorAware}.</li>
 *     <li>The TopN's limit is unknown or exceeds {@link #TOPN_EXTRACT_LIMIT_MAX}.</li>
 *     <li>Fewer than {@link #DEFERRED_COLUMN_MIN} columns would actually be deferred.</li>
 *     <li>One of the source's columns is already named {@value #ROW_POSITION_NAME} — we refuse to
 *         shadow user data rather than try to rename.</li>
 * </ul>
 * <p>
 * Production filter invariant: any {@link ExternalSourceExec} reaching this rule with a
 * {@link ExternalSourceExec#pushedFilter()} also carries non-empty
 * {@link ExternalSourceExec#pushedExpressions()}, because the only production code path that
 * installs a pushed filter is {@code PushFiltersToSource}, which always populates both via
 * {@link ExternalSourceExec#withPushedFilterAndExpressions(Object, java.util.List)}. The rule
 * uses the expressions to keep filter-referenced columns eager so the narrowed projection
 * still has every input the filter reads.
 */
public class InsertExternalFieldExtraction extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    TopNExec,
    LocalPhysicalOptimizerContext> {

    public InsertExternalFieldExtraction() {
        // Transform bottom-up so an inner TopN gets the chance to defer extraction before any
        // enclosing TopN's subtree is reshaped (which would hide the source below the inserted
        // ExternalFieldExtractExec and silently disable the outer optimization).
        super(OptimizerRules.TransformDirection.UP);
    }

    /**
     * Minimum number of columns that need to be deferred before the optimization is worth applying.
     * Below this, the per-row extraction overhead (random access reads, page assembly) outweighs
     * the I/O saved by not loading the columns up front.
     */
    public static final int DEFERRED_COLUMN_MIN = 3;

    /**
     * Maximum TopN limit for which the optimization fires. Above this, the extraction phase risks
     * doing more random-access I/O than the streaming forward scan it replaces.
     */
    public static final int TOPN_EXTRACT_LIMIT_MAX = 10_000;

    static final String ROW_POSITION_NAME = ColumnExtractor.ROW_POSITION_COLUMN;

    @Override
    protected PhysicalPlan rule(TopNExec topN, LocalPhysicalOptimizerContext ctx) {
        Integer limit = limitOf(topN, ctx);
        if (limit == null || limit > TOPN_EXTRACT_LIMIT_MAX) {
            return topN;
        }

        ExternalSourceExec externalSource = findExternalSource(topN.child());
        if (externalSource == null) {
            return topN;
        }

        if (supportsDeferredExtraction(externalSource.sourceType(), ctx == null ? null : ctx.external()) == false) {
            return topN;
        }

        // The source carries the pushed filter as an opaque object — we can't introspect it. But
        // {@link ExternalSourceExec#pushedExpressions()} keeps the original ESQL {@link Expression}
        // tree the optimizer pushed down, and its {@code references()} expose the exact set of
        // source columns the filter reads. We use those to keep the narrowed projection sound:
        // every column the filter touches stays eager. The class-level invariant guarantees a
        // pushed filter on an {@link ExternalSourceExec} always comes with non-empty pushed
        // expressions (production filters flow through {@code PushFiltersToSource}, which always
        // calls {@code withPushedFilterAndExpressions}); a non-production assignment that violates
        // it would silently disable the optimization, so we assert instead.
        List<Expression> pushedExpressions = externalSource.pushedExpressions();
        boolean hasPushedFilter = externalSource.pushedFilter() != null;
        assert hasPushedFilter == false || (pushedExpressions != null && pushedExpressions.isEmpty() == false)
            : "ExternalSourceExec ["
                + externalSource.sourceType()
                + "] has a pushed filter without pushed expressions; production filters must flow through PushFiltersToSource";

        // Refuse to fire if a user column already collides with our synthetic name. Renaming is
        // possible but adds complexity for a vanishingly rare case; bailing out preserves
        // correctness without surprises.
        for (Attribute a : externalSource.output()) {
            if (ROW_POSITION_NAME.equals(a.name())) {
                return topN;
            }
        }

        // Eager set: every attribute referenced anywhere between TopN (inclusive) and the source,
        // plus everything the source-level pushed filter reads (only its expressions, which we
        // can enumerate). We start from the TopN's own references (sort key expressions), union
        // in references from every intermediate UnaryExec node down to the source, then add the
        // pushed-filter expression refs. Anything in the source output not in this set is safe
        // to defer.
        AttributeSet eagerRefs = AttributeSet.builder().addAll(topN.references()).build();
        eagerRefs = unionWithIntermediateRefs(topN.child(), externalSource, eagerRefs);
        if (hasPushedFilter) {
            AttributeSet.Builder withFilterRefs = AttributeSet.builder().addAll(eagerRefs);
            for (Expression e : pushedExpressions) {
                withFilterRefs.addAll(e.references());
            }
            eagerRefs = withFilterRefs.build();
        }

        List<Attribute> sourceOutput = externalSource.output();
        // When `_source` is projected, its synthesizer needs every file-resident data column at
        // compose time. Deferring those columns would render `{}`. Pin every data attribute as
        // eager in that case; the synthesizer runs on the producer thread before the TopN gate.
        // Side effect: with `_source` projected the deferred set is empty by construction —
        // the {@link VirtualAttribute} arm catches every metadata attribute ({@link
        // ExternalMetadataAttribute} implements {@link VirtualAttribute}) and the data-column pin
        // arm catches everything else. {@link #DEFERRED_COLUMN_MIN} then bails out, so TopN
        // late-materialisation is disabled for `_source` queries (correctness preserves over the
        // I/O optimisation).
        boolean sourceProjected = false;
        for (Attribute a : sourceOutput) {
            if (a instanceof ExternalMetadataAttribute && ExternalMetadataColumns.SOURCE.equals(a.name())) {
                sourceProjected = true;
                break;
            }
        }
        // The second non-file-resident family: hive-style partition columns. Unlike {@code _file.*}
        // they carry no {@link VirtualAttribute} marker — they are surfaced as plain
        // {@link org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute}s on purpose, so
        // they stay user-addressable (WHERE / STATS BY / EVAL) and prunable by partition-filter
        // pushdown. Their names travel out-of-band in {@code sourceMetadata} under
        // {@code PARTITION_COLUMNS_KEY}; read them through the node-safe accessor (never the
        // coordinator-only fileList) so we can pin them eager below.
        Set<String> partitionColumns = externalSource.partitionColumnNames();
        List<Attribute> eagerColumns = new ArrayList<>(sourceOutput.size());
        List<Attribute> deferredColumns = new ArrayList<>(sourceOutput.size());
        for (Attribute a : sourceOutput) {
            // Non-file-resident columns are materialised on the producer thread by
            // {@link org.elasticsearch.xpack.esql.datasources.VirtualColumnIterator} from per-file
            // metadata, not by the format reader. They must stay in the source's narrowed output
            // (so the iterator still injects them and downstream operators see them) and they
            // must not be deferred (the {@link ColumnExtractor} positional read path cannot
            // produce values that don't exist in the file). Two families qualify: every
            // {@link VirtualAttribute} (today: {@code _file.*}, relying on the marker rather than a
            // specific subclass keeps future virtual attributes correct by construction) and every
            // hive partition column (a plain ReferenceAttribute, matched by name against the
            // {@code PARTITION_COLUMNS_KEY} stamp).
            if (a instanceof VirtualAttribute || eagerRefs.contains(a) || partitionColumns.contains(a.name())) {
                eagerColumns.add(a);
            } else if (sourceProjected && a instanceof ExternalMetadataAttribute == false) {
                // File-resident data column under `_source` projection — must be eager.
                eagerColumns.add(a);
            } else {
                deferredColumns.add(a);
            }
        }

        if (deferredColumns.size() < DEFERRED_COLUMN_MIN) {
            return topN;
        }

        MetadataAttribute rowPositionAttribute = SyntheticColumns.newRowPositionMetadataAttribute(externalSource.source());

        List<Attribute> narrowedAttributes = new ArrayList<>(eagerColumns.size() + 1);
        narrowedAttributes.addAll(eagerColumns);
        narrowedAttributes.add(rowPositionAttribute);

        // withDeferredExtraction is the operator factory's signal that this exec is paired with the
        // ExternalFieldExtractExec built below — _rowPosition presence alone is ambiguous, since
        // InjectRowPositionForExternalId also injects it for plain _id composition.
        ExternalSourceExec narrowedSource = externalSource.withAttributes(narrowedAttributes).withDeferredExtraction();
        // Rebuild the (TopN, …, ExternalSourceExec) spine with the narrowed source at the bottom.
        // Every intermediate node's children are unchanged except for the source-replacement at the
        // leaf of the spine, so the recursive copy here rewires correctly.
        TopNExec rewrittenTopN = (TopNExec) replaceSource(topN, externalSource, narrowedSource);

        return new ExternalFieldExtractExec(topN.source(), rewrittenTopN, List.copyOf(deferredColumns), rowPositionAttribute);
    }

    /**
     * Whether the configured external-source reader for the given {@code sourceType} can serve
     * positional column reads after the forward scan — the precondition for inserting an
     * {@link ExternalFieldExtractExec} above a TopN. Returns {@code false} when the rule has no
     * way to verify the capability (no external context, no registry, source type unregistered),
     * causing the rule to bail out and leave the plan unchanged.
     * <p>
     * Encapsulating the lookup here keeps the rule body free of {@link FormatReaderRegistry} and
     * {@link FormatReader} mechanics; the rule expresses the plan-shape conditions and delegates
     * the runtime-capability question to this single, replaceable predicate.
     */
    static boolean supportsDeferredExtraction(String sourceType, ExternalOptimizerContext external) {
        if (external == null) {
            return false;
        }
        FormatReaderRegistry registry = external.formatReaderRegistry();
        if (registry == null || sourceType == null) {
            return false;
        }
        FormatReader reader = registry.findByName(sourceType);
        return reader instanceof ColumnExtractorAware;
    }

    private static Integer limitOf(TopNExec topN, LocalPhysicalOptimizerContext ctx) {
        if (topN.limit().foldable() == false) {
            return null;
        }
        Object folded = topN.limit().fold(ctx != null ? ctx.foldCtx() : null);
        if (folded instanceof Integer i) {
            return i;
        }
        if (folded instanceof Number n) {
            long l = n.longValue();
            if (l < 0 || l > Integer.MAX_VALUE) {
                return null;
            }
            return (int) l;
        }
        return null;
    }

    /**
     * Walk down the spine of {@code UnaryExec} nodes between {@code start} and the leaf source,
     * returning the first {@link ExternalSourceExec} reached, or {@code null} if the spine forks
     * or ends elsewhere.
     */
    static ExternalSourceExec findExternalSource(PhysicalPlan start) {
        PhysicalPlan p = start;
        while (true) {
            if (p instanceof ExternalSourceExec es) {
                return es;
            }
            if (p instanceof UnaryExec u) {
                p = u.child();
                continue;
            }
            return null;
        }
    }

    /**
     * Walk the spine between {@code start} and {@code source} and union every node's
     * {@link PhysicalPlan#references()} into {@code initial}. This collects the attributes used by
     * filters, evals, projections etc. that sit between TopN and the source — all of which must
     * remain eagerly available.
     */
    private static AttributeSet unionWithIntermediateRefs(PhysicalPlan start, ExternalSourceExec source, AttributeSet initial) {
        AttributeSet.Builder builder = AttributeSet.builder().addAll(initial);
        PhysicalPlan p = start;
        while (p != source) {
            builder.addAll(p.references());
            if (p instanceof UnaryExec u) {
                p = u.child();
            } else {
                break;
            }
        }
        return builder.build();
    }

    /**
     * Replace {@code oldSource} with {@code newSource} inside the spine rooted at {@code root}.
     * The spine is a single UnaryExec chain; we copy nodes from the top down, swapping the source
     * at the leaf.
     */
    private static PhysicalPlan replaceSource(PhysicalPlan root, ExternalSourceExec oldSource, ExternalSourceExec newSource) {
        if (root == oldSource) {
            return newSource;
        }
        if (root instanceof UnaryExec u) {
            PhysicalPlan newChild = replaceSource(u.child(), oldSource, newSource);
            return u.replaceChild(newChild);
        }
        return root;
    }
}
