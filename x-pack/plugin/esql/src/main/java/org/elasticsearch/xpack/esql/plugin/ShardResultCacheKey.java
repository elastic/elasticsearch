/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Builds the opaque key ES|QL hands to the shard request cache. Everything that can change the rows a given shard
 * reader produces has to be in it; the shard copy, its reader version and its mapping version are separate components
 * of the cache's own key and are not repeated here.
 * <p>
 * The key is built in two stages. {@link #queryPart} digests everything that is the same for every shard of one node
 * request: the canonicalized wire plan, the node flags that drive its local replan, and a default-deny slice of
 * {@link Configuration}. {@link #forShard} then appends the parts that are per shard: the resharding summary, the
 * relation of each lifted time predicate against that shard's own data, and the security differentiator.
 * <p>
 * Additions to {@link Configuration} default to being <em>in</em> the key. A field that lands in the key without
 * needing to be there costs hit rate; one left out by accident returns wrong rows.
 */
final class ShardResultCacheKey {

    /**
     * Bumped whenever the layout below changes in a way that would let two different queries produce the same key. It
     * does not need to be bumped for changes that only affect which entries exist, because entries never outlive the
     * node process that wrote them.
     */
    private static final int FORMAT_VERSION = 1;

    /**
     * The part of the key that is common to every shard of one node request, plus the time predicates that were pulled
     * out of the digest and have to be resolved per shard instead, plus the fields the plan reads, whose mapping on a
     * given shard decides whether that shard may participate at all.
     */
    record QueryPart(byte[] digest, List<LiftedTimeRange> liftedRanges, Set<String> fieldNames) {}

    private ShardResultCacheKey() {}

    static QueryPart queryPart(DataNodeRequest request, EsqlFlags flags) throws IOException {
        List<LiftedTimeRange> liftedRanges = new ArrayList<>();
        PhysicalPlan canonicalPlan = liftTimeRanges(request.plan(), liftedRanges);
        Configuration configuration = request.configuration();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            out.writeVInt(FORMAT_VERSION);
            writeFlags(out, flags);
            /*
             * Deliberately not keyed: runNodeLevelReduction, reductionLateMaterialization and retainSearchContexts.
             * They pick how the request is split, and the caller passes the data-node half of that split as
             * request.plan(), so their effect is already in the plan bytes below. Keying them as well would make the
             * same shard's result a different entry depending on whether the coordinator happens to be this node.
             */
            out.writeString(request.clusterAlias());
            writeAliasFilters(out, request.aliasFilters());
            writeConfiguration(out, configuration);
            for (LiftedTimeRange range : liftedRanges) {
                // Only the field, so that two windows over the same field can share a key when both resolve to the
                // same relation. The bounds come back per shard, in forShard.
                out.writeString(range.fieldName());
            }
            out.writeVInt(liftedRanges.size());
            /*
             * PlanStreamOutput is the canonical encoding DataNodeRequest itself uses, so hashing it hashes the bytes
             * that were actually sent rather than a rendering of them. NameIds are normalized because they come from a
             * global counter and are reallocated again on deserialize, so the raw ids differ run to run.
             */
            new PlanStreamOutput(out, configuration, true).writeNamedWriteable(canonicalPlan);
            return new QueryPart(
                MessageDigests.digest(out.bytes(), MessageDigests.sha256()),
                List.copyOf(liftedRanges),
                fieldNames(request.plan())
            );
        }
    }

    /**
     * Completes the key for one shard.
     *
     * @return the key, or {@code null} when a lifted time predicate could not be resolved against this shard, in which
     *         case the shard must not use the cache
     */
    @Nullable
    static BytesReference forShard(
        QueryPart queryPart,
        DataNodeRequest.Shard shard,
        SearchContext searchContext,
        @Nullable CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> differentiator
    ) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            out.writeBytes(queryPart.digest());
            // Resharding changes which documents a copy owns, and the summary travels per shard, so folding it into the
            // shared digest would let two states of the same physical shard collide.
            shard.splitShardCountSummary().writeTo(out);
            if (LiftedTimeRange.writeResidue(queryPart.liftedRanges(), searchContext, out) == false) {
                return null;
            }
            if (differentiator != null) {
                // The hook the DSL path uses, not a mirror of it: it reads only the index name and the security thread
                // context, so the minimal per-shard request ES|QL already builds is enough to feed it. Reimplementing
                // it is the likely source of a cross-user leak.
                differentiator.accept(searchContext.request(), out);
            }
            return new BytesArray(MessageDigests.digest(out.bytes(), MessageDigests.sha256()));
        }
    }

    /**
     * The fields the plan reads. A field name means the same thing on every shard of a request, but what it is mapped
     * to does not, which is why this is resolved per shard rather than folded into the digest.
     */
    private static Set<String> fieldNames(PhysicalPlan plan) {
        Set<String> names = new HashSet<>();
        plan.forEachExpressionDown(FieldAttribute.class, field -> names.add(field.fieldName().string()));
        // A fragment is a field of FragmentExec rather than a child of it, so the walk above does not enter it, and
        // the fields a data node request reads are nearly all in there.
        plan.forEachDown(
            FragmentExec.class,
            fragmentExec -> fragmentExec.fragment()
                .forEachExpressionDown(FieldAttribute.class, field -> names.add(field.fieldName().string()))
        );
        return Set.copyOf(names);
    }

    /**
     * The wire plan is planned again on the data node, with local physical optimization driven by these node-level
     * flags, so two nodes with different flags can produce different rows from the same plan bytes.
     * {@code stringLikeOnIndex} changes what {@code _index LIKE} matches, and {@code roundToPushdownThreshold} decides
     * whether {@code ReplaceRoundToWithQueryAndTags} rewrites a {@code RoundTo} into range queries with tags.
     * <p>
     * {@code PlannerSettings} is deliberately not here. Its members pick slice boundaries and buffer sizes, which
     * change how rows are produced rather than which, or bound shapes the verifier refuses outright: the TopN limit and
     * the local relation size caps.
     */
    private static void writeFlags(StreamOutput out, EsqlFlags flags) throws IOException {
        out.writeBoolean(flags.stringLikeOnIndex());
        // Not a vint: the threshold's minimum is -1, which means no pushdown at all.
        out.writeInt(flags.roundToPushdownThreshold());
    }

    /**
     * Digests the fields of {@link Configuration} that can change a shard's rows. Deliberate exclusions:
     * <ul>
     *     <li>{@code now} and {@code queryStartTimeNanos} - wall clock. {@code NOW()} still reaches the key, as the
     *     folded literal it becomes in the plan, so two queries a minute apart cannot collide.</li>
     *     <li>{@code profile} and {@code explainOnly} - those requests are refused by
     *     {@link ShardResultCacheVerifier} and so can never reach a key.</li>
     *     <li>{@code query} - the text. What the shard produces is a function of the plan, which is keyed below; two
     *     texts that plan to the same fragment may share an entry, and the same text arriving as a prepared statement
     *     rather than as text must not miss.</li>
     * </ul>
     * Everything else is included, including all pragmas. Several pragmas that look like pure parallelism knobs do
     * change slice boundaries and page framing, and proving harmlessness per pragma buys nothing: pragmas are an expert
     * debug knob that production queries do not set, so keying them costs no hit rate.
     */
    private static void writeConfiguration(StreamOutput out, Configuration configuration) throws IOException {
        out.writeOptionalString(configuration.clusterName());
        out.writeOptionalString(configuration.username());
        out.writeString(configuration.locale().toLanguageTag());
        configuration.pragmas().writeTo(out);
        out.writeVInt(configuration.resultTruncationMaxSize(false));
        out.writeVInt(configuration.resultTruncationDefaultSize(false));
        out.writeVInt(configuration.resultTruncationMaxSize(true));
        out.writeVInt(configuration.resultTruncationDefaultSize(true));
        out.writeBoolean(configuration.allowPartialResults());
        Map<String, Map<String, Column>> tables = new TreeMap<>(configuration.tables());
        out.writeMap(tables, (tableOut, columns) -> tableOut.writeMap(new TreeMap<>(columns), StreamOutput::writeWriteable));
        configuration.resolvedSettings().writeTo(out);
        out.writeMap(new TreeMap<>(configuration.viewQueries()), StreamOutput::writeString);
    }

    /** Sorted by index UUID so that two runs of the same query cannot differ only in map iteration order. */
    private static void writeAliasFilters(StreamOutput out, Map<Index, AliasFilter> aliasFilters) throws IOException {
        Map<String, AliasFilter> sorted = new TreeMap<>();
        for (Map.Entry<Index, AliasFilter> entry : aliasFilters.entrySet()) {
            sorted.put(entry.getKey().getUUID(), entry.getValue());
        }
        out.writeMap(sorted, StreamOutput::writeWriteable);
    }

    /**
     * Replaces the top-level date range conjuncts of every filter directly above a relation with nothing, collecting
     * them into {@code liftedRanges} instead. Execution is never affected: only the key is built from the rewritten
     * plan, which makes this strictly safer than the DSL rewrite it mirrors, where the rewritten query is what runs.
     * <p>
     * Default deny. Only conjuncts of a top-level AND, only a single {@code date} field compared against a literal, and
     * only one lower and one upper bound per field. Anything else stays in the digest verbatim, which costs hit rate
     * and never correctness.
     */
    private static PhysicalPlan liftTimeRanges(PhysicalPlan plan, List<LiftedTimeRange> liftedRanges) {
        return plan.transformUp(
            FragmentExec.class,
            fragmentExec -> fragmentExec.withFragment(liftFromFragment(fragmentExec.fragment(), liftedRanges))
        );
    }

    private static LogicalPlan liftFromFragment(LogicalPlan fragment, List<LiftedTimeRange> liftedRanges) {
        return fragment.transformUp(Filter.class, filter -> {
            if (filter.child() instanceof EsRelation == false) {
                return filter;
            }
            List<Expression> remaining = new ArrayList<>();
            // Insertion ordered so that the digest and the per-shard residue agree on the order of the lifted ranges.
            Map<String, Bounds> byField = new LinkedHashMap<>();
            for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                if (collectBound(conjunct, byField) == false) {
                    remaining.add(conjunct);
                }
            }
            List<LiftedTimeRange> lifted = new ArrayList<>(byField.size());
            for (Map.Entry<String, Bounds> entry : byField.entrySet()) {
                Bounds bounds = entry.getValue();
                if (bounds.usable()) {
                    lifted.add(new LiftedTimeRange(entry.getKey(), bounds.from, bounds.includeFrom, bounds.to, bounds.includeTo));
                } else {
                    remaining.addAll(bounds.conjuncts);
                }
            }
            if (lifted.isEmpty()) {
                return filter;
            }
            liftedRanges.addAll(lifted);
            Expression condition = remaining.isEmpty() ? Literal.TRUE : Predicates.combineAnd(remaining);
            return new Filter(filter.source(), filter.child(), condition);
        });
    }

    /**
     * Records {@code conjunct} as a bound if it is a date range comparison against a literal.
     *
     * @return true when the conjunct was consumed, so the caller must not keep it in the residual condition
     */
    private static boolean collectBound(Expression conjunct, Map<String, Bounds> byField) {
        boolean lowerBound = conjunct instanceof GreaterThan || conjunct instanceof GreaterThanOrEqual;
        boolean upperBound = conjunct instanceof LessThan || conjunct instanceof LessThanOrEqual;
        if (lowerBound == false && upperBound == false) {
            return false;
        }
        EsqlBinaryComparison comparison = (EsqlBinaryComparison) conjunct;
        if (comparison.left() instanceof FieldAttribute field
            && field.dataType() == DataType.DATETIME
            && comparison.right() instanceof Literal literal
            && literal.value() instanceof Long value) {
            boolean inclusive = conjunct instanceof GreaterThanOrEqual || conjunct instanceof LessThanOrEqual;
            Bounds bounds = byField.computeIfAbsent(field.fieldName().string(), name -> new Bounds());
            bounds.conjuncts.add(conjunct);
            if (lowerBound) {
                bounds.lower(value, inclusive);
            } else {
                bounds.upper(value, inclusive);
            }
            return true;
        }
        return false;
    }

    /** Accumulates at most one lower and one upper bound for a single field. */
    private static final class Bounds {
        private final List<Expression> conjuncts = new ArrayList<>();
        private Long from;
        private boolean includeFrom;
        private Long to;
        private boolean includeTo;
        private boolean rejected;

        void lower(long value, boolean inclusive) {
            if (from != null) {
                rejected = true;
                return;
            }
            from = value;
            includeFrom = inclusive;
        }

        void upper(long value, boolean inclusive) {
            if (to != null) {
                rejected = true;
                return;
            }
            to = value;
            includeTo = inclusive;
        }

        boolean usable() {
            return rejected == false && (from != null || to != null);
        }
    }
}
