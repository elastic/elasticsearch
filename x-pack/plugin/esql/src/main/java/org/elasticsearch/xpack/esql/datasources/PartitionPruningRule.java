/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Streaming;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.RowCountPreserving;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * When a {@code WHERE} above an external relation is allowed to decide which of that relation's files are touched.
 *
 * <p>Two layers act on that decision, and both must obey these rules — so the rules live here instead of being restated
 * in each. {@link PartitionFilterHintExtractor} rewrites the glob, so non-matching folders are never even
 * <em>listed</em>; {@link SplitDiscoveryPhase} prunes files that were listed. The two run at opposite ends of planning
 * (one before resolution, one after) and nothing downstream can recover a file the listing layer skipped. Keeping the
 * rules apart from the walks that apply them is deliberate: a rule restated in two places drifts, and a filter trusted
 * where it should not be does not scan too much — it returns the wrong rows.
 */
final class PartitionPruningRule {

    private PartitionPruningRule() {}

    /**
     * Whether a filter above {@code plan} may still prune the files beneath it.
     *
     * <p>Pruning is licensed by this argument: if no row of a file can satisfy the filter, that file cannot contribute
     * to the result, so not reading it changes nothing. The argument holds only while every node between the filter and
     * the source leaves the row <em>count</em> alone. As soon as one selects rows by cardinality it collapses —
     * {@code FROM ds | SORT id | LIMIT 4 | WHERE year == 2025} must take four rows and filter them <em>afterwards</em>,
     * so pruning the non-2025 files first refills the {@code LIMIT} window from the survivors and returns rows the query
     * never asked for. {@code SAMPLE}, {@code STATS}, {@code MV_EXPAND} and joins break it the same way.
     *
     * <p>{@link Streaming} already means "does not add or remove rows", so it carries the rule and keeps carrying it as
     * commands are added. Two more qualify: {@link Filter}, which only ever removes rows — pruning yet more of them
     * below is still sound — and {@link OrderBy}, which reorders but never drops. Everything else fails closed to "do
     * not prune": a full scan, never an invented answer.
     *
     * <p>Safe here, and <em>only</em> here, because this side runs on a resolved plan where {@link SplitDiscoveryPhase}
     * binds each conjunct to the relation's output by {@code NameId}. That binding is what makes it harmless to let a
     * conjunct cross a node like {@code ENRICH} whose new columns are not knowable by name. The listing layer has no
     * such backstop — see {@link #hintTransparent}.
     */
    static boolean rowPreserving(LogicalPlan plan) {
        return plan instanceof Streaming || plan instanceof Filter || plan instanceof OrderBy;
    }

    /**
     * The physical twin of {@link #rowPreserving(LogicalPlan)} — same rule, other tree. Where the logical side leans on
     * the existing {@link Streaming} marker, the physical side has its own marker, {@link RowCountPreserving}, carried by
     * every exec that maps rows one-to-one ({@code FilterExec}, {@code EvalExec}, {@code ProjectExec} — the lowered form
     * of {@code Keep}/{@code Drop}/{@code Rename} — {@code RegexExtractExec} for {@code DISSECT}/{@code
     * GROK}, and {@code EnrichExec}). A cardinality-sensitive exec such as {@code LimitExec} or {@code TopNExec} does not
     * carry it, so the seed stops there.
     *
     * <p>The marker is opt-in and fails closed: an exec without it is treated as cardinality-sensitive, so a new
     * row-preserving node that forgets the marker forfeits an optimization across itself, never correctness.
     */
    static boolean rowPreserving(PhysicalPlan plan) {
        return plan instanceof RowCountPreserving;
    }

    /**
     * Whether a <em>pre-resolution</em> partition hint may cross {@code plan} — the listing layer's stricter counterpart
     * to {@link #rowPreserving(LogicalPlan)}.
     *
     * <p>It must satisfy everything {@code rowPreserving} does, and one thing more: every column the node introduces has
     * to be knowable <em>by name</em>, because before resolution a name is all a hint has. That rules out
     * {@link Streaming} as the test. {@link Enrich} is the counterexample that forces this: it is {@code Streaming} and a
     * {@link GeneratingPlan}, but until the analyzer loads the policy its {@code enrichFields} are empty unless the query
     * spelled out a {@code WITH} clause. So {@code FROM ds | ENRICH policy ON k | WHERE year == 615}, with a policy that
     * contributes a {@code year} field, would look like it shadows nothing and would prune folders by the path's year.
     * The other {@code Streaming} nodes are left out on the same principle: whatever their columns turn out to be, this is
     * not the layer that can prove it.
     *
     * <p>So this is an explicit allowlist of nodes whose name changes are fully visible in the parsed tree, and it fails
     * closed: an unrecognized node stops the hint, the glob is not rewritten, and the folders are listed. That costs a
     * wider listing. Getting it wrong costs the user rows.
     */
    static boolean hintTransparent(LogicalPlan plan) {
        return plan instanceof Filter
            || plan instanceof OrderBy
            || plan instanceof Eval          // its aliases carry names even unresolved
            || plan instanceof RegexExtract  // DISSECT, GROK — extracted names come from the parser
            || plan instanceof Rename
            || plan instanceof Project       // column selection; KEEP extends Project
            || plan instanceof Drop;
    }

    /**
     * The column names {@code plan} introduces or redefines, which therefore no longer mean what the storage path means.
     * {@code EVAL year = id % 10000} turns {@code year} into a row-derived value with nothing to do with the
     * {@code year=2024/} folder the row came from, so a {@code WHERE year == 615} above it must not prune a single folder.
     *
     * <p>Only the listing layer needs this. Once a plan is resolved, names stop being identity —
     * {@link SplitDiscoveryPhase} binds conjuncts by {@code NameId}, which is strictly stronger and needs none of it.
     * Before resolution there are no ids, so shadowing has to be spotted structurally.
     *
     * <p>For {@link Rename} the <em>new</em> name is the dangerous one: {@code RENAME id AS year} makes {@code year}
     * row-derived. The old name disappearing is harmless — a filter still referencing it fails analysis, which is an
     * error, never a wrong row.
     */
    static Set<String> shadowedNames(LogicalPlan plan) {
        if (plan instanceof GeneratingPlan<?> generating) {
            return generating.generatedAttributes().stream().map(Attribute::name).collect(Collectors.toSet());
        }
        if (plan instanceof Rename rename) {
            return rename.renamings().stream().map(Alias::name).collect(Collectors.toSet());
        }
        return Set.of();
    }
}
