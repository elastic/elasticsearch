/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDifference;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSlice;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.GraphExpand;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Coordinator-side visited-set BFS for a research snapshot of {@link GraphExpand}.
 * Each hop is a real ES|QL subplan because the compute engine has no iteration.
 * The plan is not serialized (same stance as {@link org.elasticsearch.xpack.esql.plan.logical.MMR}).
 */
public final class GraphExpandDriver {

    private static final Logger LOGGER = LogManager.getLogger(GraphExpandDriver.class);

    public static final int DEFAULT_MAX_HOPS = 3;

    /** Column carrying a frontier document's pointer count through a hop subplan, for the hub guard. */
    private static final String HUB_DEGREE_COLUMN = "__degree";
    /** Column holding the ids an inbound hub guard kept for one frontier node, before they are exploded. */
    private static final String HUB_REACHED_COLUMN = "__reached";

    private static final Set<String> ALLOWED_OPTIONS = Set.of(
        "max_hops",
        "direction",
        "closing_edges",
        "hub_degree",
        "max_edges_per_node",
        "max_edges_per_hop",
        "hop_fields",
        "max_nodes",
        "max_frontier",
        "exclude"
    );

    /** Walk orientation for one hop leg. */
    enum Leg {
        OUT,
        IN
    }

    private final GraphExpand graphExpand;
    private final BlockFactory blockFactory;
    private final int maxHops;
    private final String direction;
    private final Attribute matchField;
    private final List<Attribute> targetFields;
    private final boolean multiField;
    /**
     * The {@code hop_fields} option: the subset of {@link #targetFields} each hop walks, one
     * entry per hop with the last entry reused for later hops; empty means every hop walks
     * every TO field. A hop only issues subplans for its own fields.
     */
    private final List<List<Attribute>> hopFields;
    private final List<Attribute> resultAttributes;
    private final DataType nodeType;
    private final int nodeFromIdx;
    private final int nodeToIdx;
    private final int nodeReachedIdx;
    private final int hopIdx;

    /** Nodes already admitted (including seeds). */
    private final Set<Object> visited = new HashSet<>();
    /** Nodes the walk never admits; an edge touching one is dropped as soon as the hop returns it. */
    private final Set<Object> excluded;
    /** Nodes to expand on the next hop. */
    private List<Object> frontier = new ArrayList<>();
    /** Depth of the next hop to run (1-based). */
    private int nextHop = 1;
    /** What the hop subplan in flight walks, for the log line its result completes; null when none is in flight. */
    @Nullable
    private String inFlightHop;
    /** {@link System#nanoTime()} when the hop subplan in flight was handed out. */
    private long inFlightHopStartedNanos;
    /** Hop subplans handed out so far, and the time their results took to come back. */
    private int hopSubPlans;
    private long hopSubPlanNanos;
    private final long walkStartedNanos = System.nanoTime();
    /** Admitted edge rows matching {@link #resultAttributes}. */
    private final List<List<Object>> admittedRows = new ArrayList<>();
    private boolean finished;

    /**
     * Next leg to execute for the current target field. For {@code out}/{@code in}
     * this stays on that single leg; for {@code both} it advances OUT → IN within
     * the field.
     */
    private Leg nextLeg;
    /** Index into {@link #targetFields} for the field leg currently in flight. */
    /** Outbound rows buffered while the inbound leg of {@code direction: both} runs. */
    /** Set when the walk had no frontier to start from and an empty hop result is in flight. */
    private boolean emptyStart;
    /**
     * Rows from completed TO-field legs of the current hop, waiting for remaining
     * fields before SORT / caps. {@code null} when no multi-field buffering is active.
     */

    /**
     * Sorted hop rows waiting for an {@code UNTIL} {@link Filter} subplan, or
     * {@code null} when no until-filter is in flight.
     */
    private List<List<Object>> pendingUntilRows;
    /** True while {@link #firstSubPlan} should return the UNTIL Filter over {@link #pendingUntilRows}. */
    private boolean awaitingUntilFilter;

    /**
     * Mutable stop condition. Starts as {@link GraphExpand#until()}; when that holds an
     * {@link InSubquery}, the driver resolves the subquery once before hop 1 and replaces
     * this with an ordinary {@link In} of literals.
     */
    private Expression until;
    /** True while {@link #firstSubPlan} should return the UNTIL subquery plan. */
    private boolean awaitingUntilSubquery;
    /** True after the UNTIL subquery (if any) has been resolved to literals. */
    private boolean untilSubqueryDone;

    private GraphExpandDriver(
        GraphExpand graphExpand,
        BlockFactory blockFactory,
        int maxHops,
        String direction,
        Attribute matchField,
        List<Attribute> targetFields,
        List<Attribute> resultAttributes,
        List<Object> seeds
    ) {
        this.graphExpand = graphExpand;
        this.blockFactory = blockFactory;
        this.maxHops = maxHops;
        this.direction = direction;
        this.matchField = matchField;
        this.targetFields = targetFields;
        this.multiField = targetFields.size() > 1;
        this.hopFields = hopFields(graphExpand, targetFields);
        this.resultAttributes = resultAttributes;
        this.nodeType = targetFields.get(0).dataType();
        this.nodeFromIdx = indexOf(resultAttributes, "node_from");
        this.nodeToIdx = indexOf(resultAttributes, "node_to");
        this.nodeReachedIdx = indexOf(resultAttributes, "node_reached");
        this.hopIdx = indexOf(resultAttributes, "hop");
        this.nextLeg = initialLeg();
        this.until = graphExpand.until();
        this.untilSubqueryDone = until == null || untilContainsInSubquery(until) == false;
        this.excluded = excludedNodes(graphExpand, nodeType);
        for (Object seed : seeds) {
            if (excluded.contains(seed)) {
                continue;
            }
            visited.add(seed);
            frontier.add(seed);
        }
    }

    /**
     * The {@code exclude} option as node values: keyword ids as stored bytes, ip ids in the
     * encoding the ip column carries, so a row's endpoint compares equal to them.
     */
    private static Set<Object> excludedNodes(GraphExpand ge, DataType nodeType) {
        MapExpression options = ge.options();
        if (options == null) {
            return Set.of();
        }
        Expression value = options.keyFoldedMap().get("exclude");
        if (value == null) {
            return Set.of();
        }
        Object folded = value.fold(FoldContext.small());
        List<?> values = folded instanceof List<?> list ? list : folded == null ? List.of() : List.of(folded);
        Set<Object> result = new HashSet<>();
        for (Object v : values) {
            if (v == null) {
                continue;
            }
            result.add(nodeType == DataType.IP ? canonicalIp(v) : v instanceof BytesRef ? v : new BytesRef(v.toString()));
        }
        return result;
    }

    /**
     * The {@code hop_fields} option as TO attributes per hop. Each entry names the fields of
     * one hop, comma separated; every name must be a TO field.
     */
    private static List<List<Attribute>> hopFields(GraphExpand ge, List<Attribute> targetFields) {
        MapExpression options = ge.options();
        if (options == null) {
            return List.of();
        }
        Expression value = options.keyFoldedMap().get("hop_fields");
        if (value == null) {
            return List.of();
        }
        Object folded = value.fold(FoldContext.small());
        List<?> entries = folded instanceof List<?> list ? list : folded == null ? List.of() : List.of(folded);
        List<List<Attribute>> result = new ArrayList<>(entries.size());
        for (Object entry : entries) {
            List<Attribute> fields = new ArrayList<>();
            for (String name : BytesRefs.toString(entry).split(",")) {
                String trimmed = name.strip();
                Attribute field = targetFields.stream().filter(a -> a.name().equals(trimmed)).findFirst().orElse(null);
                if (field == null) {
                    throw new IllegalArgumentException("GRAPH EXPAND hop_fields names [" + trimmed + "], which is not a TO field");
                }
                fields.add(field);
            }
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("GRAPH EXPAND hop_fields has an entry with no fields");
            }
            result.add(fields);
        }
        return result;
    }

    /** The TO fields the hop about to run walks: its {@code hop_fields} entry, else every TO field. */
    private List<Attribute> currentTargetFields() {
        if (hopFields.isEmpty()) {
            return targetFields;
        }
        return hopFields.get(Math.min(nextHop, hopFields.size()) - 1);
    }

    private Leg initialLeg() {
        return "in".equals(direction) ? Leg.IN : Leg.OUT;
    }

    /**
     * Builds a driver for the first {@link GraphExpand} in {@code plan}, or {@code null}
     * if none. Validates the executed subset and reads seeds from the expand's
     * {@link LocalRelation} child.
     */
    public static GraphExpandDriver create(LogicalPlan plan, BlockFactory blockFactory) {
        GraphExpand ge = findGraphExpand(plan);
        if (ge == null) {
            return null;
        }
        validateSubset(ge);
        if ((ge.child() instanceof LocalRelation seedRelation) == false) {
            throw new IllegalArgumentException(
                "GRAPH EXPAND seed input must be a local relation after optimization, got [" + ge.child().getClass().getSimpleName() + "]"
            );
        }
        List<Attribute> resultAttributes = ge.resultAttributes();
        if (resultAttributes == null || resultAttributes.size() < 4) {
            throw new IllegalStateException("GRAPH EXPAND result attributes were not resolved during analysis");
        }
        if (ge.targetFields().isEmpty()) {
            throw new IllegalStateException("GRAPH EXPAND requires at least one TO field");
        }
        List<Object> seeds = readColumnValues((LocalRelation) ge.child(), ge.seedColumn());
        if (seeds.isEmpty()) {
            throw new IllegalArgumentException("GRAPH EXPAND seed column [" + ge.seedColumn().name() + "] produced no values");
        }
        return new GraphExpandDriver(
            ge,
            blockFactory,
            maxHops(ge),
            direction(ge),
            ge.matchField(),
            ge.targetFields(),
            resultAttributes,
            seeds
        );
    }

    public static GraphExpand findGraphExpand(LogicalPlan plan) {
        var holder = new org.elasticsearch.xpack.esql.core.util.Holder<GraphExpand>();
        plan.forEachUp(GraphExpand.class, ge -> {
            if (holder.get() == null) {
                holder.set(ge);
            }
        });
        return holder.get();
    }

    /**
     * Next hop (or hop-leg / field-leg) plan to execute, the in-flight {@code UNTIL}
     * Filter, the pre-walk UNTIL subquery, or {@code null} when the walk is done.
     */
    public LogicalPlan firstSubPlan() {
        if (finished) {
            return null;
        }
        if (awaitingUntilFilter) {
            LogicalPlan untilPlan = buildUntilFilterPlan(pendingUntilRows);
            untilPlan.setOptimized();
            return untilPlan;
        }
        // Resolve UNTIL InSubquery once, before hop 1 — not per hop / per row.
        if (untilSubqueryDone == false) {
            awaitingUntilSubquery = true;
            LogicalPlan subqueryPlan = untilSubqueryPlan(until);
            subqueryPlan.setOptimized();
            return subqueryPlan;
        }
        if (frontier.isEmpty() || nextHop > maxHops) {
            if (admittedRows.isEmpty() && nextHop == 1 && emptyStart == false) {
                // Nothing to walk: one empty hop result lets newMainPlan finish and replace the command.
                emptyStart = true;
                return emptyHopPlan();
            }
            finished = true;
            return null;
        }
        // One subplan per hop. Each leg of a `both` walk and each TO field is its own plan;
        // with more than one they become the branches of a FORK, which the compute service runs
        // concurrently (up to branch_parallel_degree) and concatenates, so a hop's latency is
        // that of its slowest leg and field, not the sum.
        List<Attribute> fields = currentTargetFields();
        List<Leg> legs = "both".equals(direction) ? List.of(Leg.OUT, Leg.IN) : List.of(initialLeg());
        List<LogicalPlan> branches = new ArrayList<>(legs.size() * fields.size());
        for (Leg leg : legs) {
            for (Attribute field : fields) {
                branches.add(buildHopPlan(nextHop, frontier, leg, field));
            }
        }
        LogicalPlan hopPlan;
        if (branches.size() == 1) {
            hopPlan = branches.get(0);
        } else {
            branches.forEach(LogicalPlan::setOptimized);
            hopPlan = new Fork(graphExpand.source(), branches, resultAttributes);
        }
        hopPlan.setOptimized();
        inFlightHop = "hop "
            + nextHop
            + " "
            + ("both".equals(direction) ? "both" : nextLeg.name().toLowerCase(Locale.ROOT))
            + " "
            + fields.stream().map(Attribute::name).collect(Collectors.joining(","));
        inFlightHopStartedNanos = System.nanoTime();
        hopSubPlans++;
        LOGGER.info("GRAPH EXPAND {}: {} frontier values, subplan:\n{}", inFlightHop, frontier.size(), hopPlan);
        return hopPlan;
    }

    /**
     * Consumes a hop {@link Result} (or an {@code UNTIL} Filter / subquery result), updates
     * visited/frontier, and either keeps {@code mainPlan} for another hop (or
     * the inbound leg of {@code both}, or the next TO field, or the UNTIL Filter)
     * or replaces {@link GraphExpand} with the accumulated admission rows.
     */
    public LogicalPlan newMainPlan(LogicalPlan mainPlan, Result hopResult) {
        if (awaitingUntilSubquery) {
            return finishUntilSubquery(mainPlan, hopResult);
        }
        if (awaitingUntilFilter) {
            return finishUntilFilter(mainPlan, hopResult);
        }

        List<List<Object>> rows = extractRows(hopResult);
        if (inFlightHop != null) {
            long elapsedNanos = System.nanoTime() - inFlightHopStartedNanos;
            hopSubPlanNanos += elapsedNanos;
            LOGGER.info("GRAPH EXPAND {}: {} rows in {} ms", inFlightHop, rows.size(), TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
            inFlightHop = null;
        }

        if (emptyStart) {
            // The empty start result: finish at once, whatever the direction or TO list.
            return admitAndAdvance(mainPlan, rows, false);
        }

        // FORK does not promise branch order, so put the legs (out before in) and the TO fields
        // back in written order, rows within a branch keeping theirs, before SORT/caps run once
        // over the hop. A `both` walk also drops the inbound copy of a self-loop.
        if ("both".equals(direction)) {
            rows = combineBothLegs(rows);
        }
        if (multiField) {
            orderByField(rows);
        }

        // SORT → hub_degree → caps → UNTIL → admission — UNTIL is not a post-filter after all hops.
        sortHopRows(rows);
        applyHubDegree(rows);
        applyCaps(rows);
        // IP stop-set members and the admitted node have to be the same bytes.
        // An ip field can come back IPv4 or IPv4-mapped; both encode to one form.
        canonicalizeUntilIps(rows);
        if (until != null && rows.isEmpty() == false) {
            pendingUntilRows = rows;
            awaitingUntilFilter = true;
            return mainPlan;
        }
        return admitAndAdvance(mainPlan, rows, false);
    }

    /** Stable sort of a multi-field hop's rows by their {@code relation} field's position in TO. */
    private void orderByField(List<List<Object>> rows) {
        int relationIdx = indexOf(resultAttributes, "relation");
        if (relationIdx < 0) {
            return;
        }
        Map<String, Integer> position = new HashMap<>();
        for (int i = 0; i < targetFields.size(); i++) {
            position.put(targetFields.get(i).name(), i);
        }
        rows.sort(Comparator.comparingInt(row -> {
            Object relation = row.get(relationIdx);
            Integer index = relation == null ? null : position.get(BytesRefs.toString(relation));
            return index == null ? Integer.MAX_VALUE : index;
        }));
    }

    /**
     * Ordinary ES|QL {@link Filter} over the hop's emitted columns (not edge
     * documents). Matching rows identify where {@code UNTIL} fires; the driver
     * still walks the sorted hop in order and cuts off after the first match.
     */
    private LogicalPlan buildUntilFilterPlan(List<List<Object>> rows) {
        Source source = graphExpand.source();
        LocalRelation local = rowsAsRelation(source, rows);
        return new Filter(source, local, until);
    }

    /**
     * Consumes the pre-walk UNTIL subquery {@link Result}: one column of stop-set
     * values (nulls dropped). Rewrites {@link #until} to an ordinary {@link In} of
     * literals on the same left-hand side, then the walk proceeds with hop 1.
     */
    private LogicalPlan finishUntilSubquery(LogicalPlan mainPlan, Result subqueryResult) {
        awaitingUntilSubquery = false;
        untilSubqueryDone = true;
        InSubquery inSub = findUntilInSubquery(until);
        if (inSub == null) {
            throw new IllegalStateException("GRAPH EXPAND expected an UNTIL InSubquery to resolve");
        }
        List<Attribute> schema = subqueryResult.schema();
        if (schema.size() != 1) {
            throw new IllegalArgumentException("GRAPH EXPAND UNTIL subquery must return exactly one column, got [" + schema.size() + "]");
        }
        DataType valueType = schema.get(0).dataType();
        List<Expression> literals = new ArrayList<>();
        for (Page page : subqueryResult.pages()) {
            Block block = page.getBlock(0);
            for (int i = 0; i < page.getPositionCount(); i++) {
                Object v = BlockUtils.toJavaObject(block, i);
                if (v != null) {
                    if (valueType == DataType.IP) {
                        v = canonicalIp(v);
                    }
                    literals.add(new Literal(graphExpand.source(), v, valueType));
                }
            }
        }
        if (literals.isEmpty()) {
            throw new IllegalArgumentException("GRAPH EXPAND UNTIL subquery returned no values, so the walk would stop at nothing");
        }
        // Preserve the user's LHS (usually node_reached); do not hard-code the name.
        until = new In(graphExpand.source(), inSub.value(), literals);
        return mainPlan;
    }

    private LogicalPlan finishUntilFilter(LogicalPlan mainPlan, Result filterResult) {
        awaitingUntilFilter = false;
        List<List<Object>> matched = extractRows(filterResult);
        Set<List<Object>> matchedSet = new HashSet<>(matched);
        List<List<Object>> sorted = pendingUntilRows;
        pendingUntilRows = null;

        List<List<Object>> kept = new ArrayList<>();
        boolean untilMatched = false;
        for (List<Object> row : sorted) {
            kept.add(row);
            if (matchedSet.contains(row)) {
                untilMatched = true;
                break;
            }
        }
        return admitAndAdvance(mainPlan, kept, untilMatched);
    }

    private LogicalPlan admitAndAdvance(LogicalPlan mainPlan, List<List<Object>> rows, boolean untilMatched) {
        List<Object> newlyAdmitted = admitRows(rows);
        nextHop++;
        frontier = newlyAdmitted;
        nextLeg = initialLeg();
        if (untilMatched || frontier.isEmpty() || nextHop > maxHops) {
            finished = true;
            LOGGER.info(
                "GRAPH EXPAND finished: {} hops, {} subplans taking {} ms of {} ms, {} rows admitted, {} nodes visited, stop: {}",
                nextHop - 1,
                hopSubPlans,
                TimeUnit.NANOSECONDS.toMillis(hopSubPlanNanos),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - walkStartedNanos),
                admittedRows.size(),
                visited.size(),
                untilMatched ? "until matched" : frontier.isEmpty() ? "frontier exhausted" : "max hops"
            );
            LocalRelation results = resultsRelation();
            LogicalPlan replaced = mainPlan.transformUp(GraphExpand.class, ge -> results);
            replaced.setOptimized();
            return replaced;
        }
        return mainPlan;
    }

    private LocalRelation rowsAsRelation(Source source, List<List<Object>> rows) {
        if (rows.isEmpty()) {
            Block[] empty = new Block[resultAttributes.size()];
            for (int i = 0; i < resultAttributes.size(); i++) {
                empty[i] = blockFactory.newConstantNullBlock(0);
            }
            return new LocalRelation(source, resultAttributes, LocalSupplier.of(new Page(empty)));
        }
        Block[] blocks = BlockUtils.fromList(blockFactory, rows);
        return new LocalRelation(source, resultAttributes, LocalSupplier.of(new Page(blocks)));
    }

    public boolean finished() {
        return finished;
    }

    /**
     * A walk that ends before its first hop (every seed excluded, or no hop budget) still has
     * to leave the main plan without the command: this is the empty hop result to run through
     * {@link #newMainPlan} so the usual finishing path replaces {@link GraphExpand}.
     */
    public LogicalPlan emptyHopPlan() {
        LogicalPlan empty = rowsAsRelation(graphExpand.source(), List.of());
        empty.setOptimized();
        return empty;
    }

    // --- hop plan ----------------------------------------------------------------

    private LogicalPlan buildHopPlan(int hop, List<Object> frontierValues, Leg leg, Attribute targetField) {
        Source source = graphExpand.source();
        List<Expression> literals = new ArrayList<>(frontierValues.size());
        for (Object value : frontierValues) {
            literals.add(new Literal(source, value, nodeType));
        }
        // out: frontier matches ON (stored source). in: frontier matches TO (stored target).
        Attribute frontierField = leg == Leg.OUT ? matchField : targetField;
        // Explode the matched field before the IN. A compute-layer IN on a
        // multi-valued keyword misses the document (the value is inside the
        // array). After MV_EXPAND the value is one keyword and the IN matches,
        // which is what an inbound pointer walk needs. A single-valued field
        // expands to itself. The IN references the expanded attribute, so the
        // optimizer leaves it above the expand; other predicates can still
        // push to the index.
        Attribute expandedFrontier = new ReferenceAttribute(
            frontierField.source(),
            frontierField.qualifier(),
            frontierField.name(),
            frontierField.dataType(),
            frontierField.nullable(),
            null,
            false
        );
        // Match at the index first, so Lucene answers the frontier with a terms query before any
        // document is read, and the pointer being non-null with an exists query. On an outbound
        // leg the frontier field is the node id, a single value, and a plain IN does it. On an
        // inbound leg the frontier field is the multi-valued pointer, where IN and == carry
        // single-value semantics and miss a value held inside the array; MV_INTERSECTS against
        // the frontier list translates to the same terms query without that restriction. The IN
        // kept above the expands still trims per value either way.
        Attribute pointerField = leg == Leg.OUT ? targetField : matchField;
        // Targets this hop never admits: excluded ids always, and with closing_edges false the
        // visited ids too. Edges onto them are removed inside the query rather than after the
        // fetch, so they are neither grouped nor shipped. On an inbound leg the reached node is
        // the document's own id, single-valued, so a NOT IN on it is a Lucene filter; on an
        // outbound leg they are taken out of the pointer array below, before it is exploded.
        List<Object> skipped = new ArrayList<>(excluded);
        if (closingEdges() == false) {
            skipped.addAll(visited);
        }
        List<Expression> indexPredicates = new ArrayList<>(3);
        if (leg == Leg.OUT) {
            indexPredicates.add(new In(source, frontierField, literals));
        } else {
            Literal frontierList = new Literal(source, List.copyOf(frontierValues), nodeType);
            indexPredicates.add(new MvIntersects(source, frontierField, frontierList));
            if (skipped.isEmpty() == false) {
                List<Expression> skippedLiterals = new ArrayList<>(skipped.size());
                for (Object value : skipped) {
                    skippedLiterals.add(new Literal(source, value, nodeType));
                }
                indexPredicates.add(new Not(source, new In(source, pointerField, skippedLiterals)));
            }
        }
        indexPredicates.add(new IsNotNull(source, pointerField));
        LogicalPlan edges = new Filter(source, graphExpand.edgeRelation(), Predicates.combineAnd(indexPredicates));
        // Outbound pointer array, rewritten on the matched document before it is exploded.
        // Skipped targets are taken out first (MV_DIFFERENCE), so what follows only sees edges
        // the hop can admit. Hub guard: a frontier node with more remaining pointers than
        // hub_degree is refused after the fetch; counting the array here and slicing it to
        // hub_degree + 1 values keeps a hub to just enough rows for that refusal to see it,
        // instead of every pointer it holds being expanded, joined and shipped. The count rides
        // along in `dropped` so the refusal stub reports it; applyHubDegree clears it on kept rows.
        Integer hubDegree = optionInt("hub_degree");
        Attribute degreeAttr = null;
        Attribute expandSource = pointerField;
        LogicalPlan beforeExpand = edges;
        if (leg == Leg.OUT && (hubDegree != null || skipped.isEmpty() == false)) {
            Expression pointers = skipped.isEmpty()
                ? pointerField
                : new MvDifference(source, pointerField, new Literal(source, List.copyOf(skipped), nodeType));
            List<Alias> rewritten = new ArrayList<>(2);
            Expression kept = pointers;
            if (hubDegree != null) {
                Alias degree = new Alias(source, HUB_DEGREE_COLUMN, new MvCount(source, pointers));
                rewritten.add(degree);
                degreeAttr = degree.toAttribute();
                // MV_SLICE bounds are inclusive: 0..hub_degree is hub_degree + 1 values.
                kept = new MvSlice(
                    source,
                    pointers,
                    new Literal(source, 0, DataType.INTEGER),
                    new Literal(source, hubDegree, DataType.INTEGER)
                );
            }
            Alias capped = new Alias(source, pointerField.name(), kept);
            rewritten.add(capped);
            beforeExpand = new Eval(source, edges, rewritten);
            expandSource = capped.toAttribute();
        }
        LogicalPlan scanned = new MvExpand(source, beforeExpand, frontierField, expandedFrontier);
        // The other side of the edge is exploded too, so a multi-valued pointer (an entity's
        // list of relationship targets) yields one edge per value on an outbound leg as well,
        // and UNTIL compares against a single reached id rather than a list. Without STATS
        // nothing else would split the values.
        Attribute expandedPointer = new ReferenceAttribute(
            pointerField.source(),
            pointerField.qualifier(),
            pointerField.name(),
            pointerField.dataType(),
            pointerField.nullable(),
            null,
            false
        );
        scanned = new MvExpand(source, scanned, expandSource, expandedPointer);
        In inPredicate = new In(source, expandedFrontier, literals);
        // Null pointer values emit no row (TO IS NOT NULL on the scanned documents).
        Expression notNullPointer = new IsNotNull(
            source,
            retarget(retarget(targetField, frontierField, expandedFrontier), pointerField, expandedPointer)
        );
        Expression documentFilter = graphExpand.documentFilter() == null
            ? null
            : retarget(retarget(graphExpand.documentFilter(), frontierField, expandedFrontier), pointerField, expandedPointer);
        Expression edgePredicate = Predicates.combineAnd(
            documentFilter != null ? List.of(inPredicate, notNullPointer, documentFilter) : List.of(inPredicate, notNullPointer)
        );
        LogicalPlan hopChild = new Filter(source, scanned, edgePredicate);

        Attribute nodeFrom = resultAttributes.get(nodeFromIdx);
        Attribute nodeTo = resultAttributes.get(nodeToIdx);
        Attribute nodeReached = resultAttributes.get(nodeReachedIdx);
        Attribute hopAttr = resultAttributes.get(hopIdx);

        Attribute seenMatch = retarget(retarget(matchField, frontierField, expandedFrontier), pointerField, expandedPointer);
        Attribute seenTarget = retarget(retarget(targetField, frontierField, expandedFrontier), pointerField, expandedPointer);
        Attribute evalFrom = seenMatch;
        Attribute evalTo = seenTarget;
        boolean inboundGuard = leg == Leg.IN && hubDegree != null && countOnlyAggregates();
        if (inboundGuard) {
            // Inbound hub guard. The node reached is the document holding the pointer, one row per
            // document, so there is no array to slice; instead the rows are grouped by the frontier
            // value they point at, counted, and at most hub_degree + 1 of their ids kept with TOP.
            // A hub thus ships just enough rows for the refusal after the fetch to see it, with its
            // true in-degree in `dropped`, and max_edges_per_hop no longer cuts its neighbours' edges.
            // Per-edge STATS values cannot survive the regrouping, so this form runs only when the
            // statement has none or counts rows; each row here is one holder document, so COUNT(*)
            // is 1 (Kibana's statement).
            Alias degree = new Alias(source, HUB_DEGREE_COLUMN, new Count(source, Literal.keyword(source, "*")));
            Alias reachedTop = new Alias(
                source,
                HUB_REACHED_COLUMN,
                new Top(source, expandedPointer, new Literal(source, hubDegree + 1, DataType.INTEGER), Literal.keyword(source, "asc"), null)
            );
            Aggregate perFrontier = new Aggregate(
                source,
                hopChild,
                List.of(expandedFrontier),
                List.<NamedExpression>of(degree, reachedTop, expandedFrontier)
            );
            Attribute reachedExpanded = new ReferenceAttribute(source, null, HUB_REACHED_COLUMN, nodeType, Nullability.TRUE, null, false);
            hopChild = new MvExpand(source, perFrontier, reachedTop.toAttribute(), reachedExpanded);
            evalFrom = reachedExpanded;
            evalTo = expandedFrontier;
            degreeAttr = degree.toAttribute();
        } else if (graphExpand.aggregates() != null) {
            Aggregate aggregate = buildHopAggregate(
                source,
                hopChild,
                seenMatch,
                seenTarget,
                frontierField,
                expandedFrontier,
                pointerField,
                expandedPointer,
                degreeAttr
            );
            hopChild = aggregate;
            // Aggregate WHERE filters collapsed edges after STATS, before admission.
            if (graphExpand.aggregateFilter() != null) {
                hopChild = new Filter(source, aggregate, graphExpand.aggregateFilter());
            }
            evalFrom = attributeByName(hopChild.output(), matchField.name());
            evalTo = attributeByName(hopChild.output(), targetField.name());
        }

        Attribute reachedExpr = leg == Leg.OUT ? evalTo : evalFrom;
        List<Alias> evalFields = new ArrayList<>(6);
        evalFields.add(new Alias(source, nodeFrom.name(), evalFrom, nodeFrom.id(), false));
        evalFields.add(new Alias(source, nodeTo.name(), evalTo, nodeTo.id(), false));
        evalFields.add(new Alias(source, nodeReached.name(), reachedExpr, nodeReached.id(), false));
        evalFields.add(new Alias(source, hopAttr.name(), new Literal(source, hop, DataType.INTEGER), hopAttr.id(), false));
        if (inboundGuard && graphExpand.aggregates() != null) {
            for (NamedExpression aggregate : graphExpand.aggregates()) {
                evalFields.add(new Alias(source, aggregate.name(), new Literal(source, 1L, DataType.LONG), aggregate.id(), false));
            }
        }
        // Multi-field TO only: relation = the pointer field name that fired this leg.
        Attribute relationAttr = findNamed(resultAttributes, "relation");
        if (relationAttr != null) {
            evalFields.add(new Alias(source, relationAttr.name(), Literal.keyword(source, targetField.name()), relationAttr.id(), false));
        }
        // Optional dropped: null on every hop row; hub stubs overwrite with the refused degree.
        // Must live on Eval (Literal) — Project only accepts Attribute children.
        Attribute droppedAttr = findNamed(resultAttributes, "dropped");
        if (droppedAttr != null) {
            Expression droppedValue = new Literal(source, null, DataType.INTEGER);
            if (degreeAttr != null) {
                // The guard counted the pointers: a count past hub_degree is the refused degree.
                Expression degree = graphExpand.aggregates() != null ? attributeByName(hopChild.output(), HUB_DEGREE_COLUMN) : degreeAttr;
                if (inboundGuard) {
                    // COUNT(*) is a long; `dropped` is an integer column.
                    degree = new ToInteger(source, degree);
                }
                droppedValue = new Case(
                    source,
                    new GreaterThan(source, degree, new Literal(source, hubDegree, DataType.INTEGER)),
                    List.of(degree, new Literal(source, null, DataType.INTEGER))
                );
            }
            evalFields.add(new Alias(source, droppedAttr.name(), droppedValue, droppedAttr.id(), false));
        }
        Eval eval = new Eval(source, hopChild, evalFields);

        List<NamedExpression> projections = new ArrayList<>(resultAttributes.size());
        for (int i = 0; i < 4; i++) {
            projections.add(evalFields.get(i).toAttribute());
        }
        // relation / STATS payload / optional dropped — lookup by name on Eval output.
        for (int i = 4; i < resultAttributes.size(); i++) {
            projections.add(attributeByName(eval.output(), resultAttributes.get(i).name()));
        }
        LogicalPlan hopPlan = new Project(source, eval, projections);
        // Rows a hop could never admit are not read: the cap is the smaller of max_edges_per_hop
        // and what the engine would hold anyway, applied per subplan before the rows travel.
        Integer maxEdgesPerHop = optionInt("max_edges_per_hop");
        if (maxEdgesPerHop != null) {
            hopPlan = new Limit(source, new Literal(source, maxEdgesPerHop, DataType.INTEGER), hopPlan);
        }
        return hopPlan;
    }

    /** True when the statement has no STATS, or only unfiltered, ungrouped row counts (`COUNT(*)`). */
    private boolean countOnlyAggregates() {
        List<? extends NamedExpression> aggregates = graphExpand.aggregates();
        if (aggregates == null) {
            return true;
        }
        if ((graphExpand.groupings() != null && graphExpand.groupings().isEmpty() == false) || graphExpand.aggregateFilter() != null) {
            return false;
        }
        for (NamedExpression aggregate : aggregates) {
            boolean rowCount = aggregate instanceof Alias alias
                && alias.child() instanceof Count count
                && count.hasFilter() == false
                && count.field().foldable();
            if (rowCount == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Ordinary ES|QL {@link Aggregate} over hop documents. Grouping is always the
     * endpoint pair ({@code match}, {@code TO}); a user {@code BY} refines that
     * pair. Aggregate expressions are those already resolved on {@link GraphExpand}.
     */
    private Aggregate buildHopAggregate(
        Source source,
        LogicalPlan filteredEdges,
        Attribute seenMatch,
        Attribute seenTarget,
        Attribute frontierField,
        Attribute expandedFrontier,
        Attribute pointerField,
        Attribute expandedPointer,
        @Nullable Attribute degreeAttr
    ) {
        List<? extends NamedExpression> userAggregates = graphExpand.aggregates();
        List<Expression> userGroupings = graphExpand.groupings() != null ? graphExpand.groupings() : List.of();

        List<Expression> groupings = new ArrayList<>(2 + userGroupings.size());
        groupings.add(seenMatch);
        groupings.add(seenTarget);
        for (Expression grouping : userGroupings) {
            groupings.add(retarget(retarget(grouping, frontierField, expandedFrontier), pointerField, expandedPointer));
        }

        // Same shape as ParserUtils.buildStats: user aggs first, then grouping keys
        // so Aggregate.output() carries the pair (for Eval) and any user BY columns.
        List<NamedExpression> aggregates = new ArrayList<>(userAggregates.size() + groupings.size());
        for (NamedExpression aggregate : userAggregates) {
            Expression rewritten = retarget(retarget(aggregate, frontierField, expandedFrontier), pointerField, expandedPointer);
            if (rewritten instanceof NamedExpression named) {
                aggregates.add(named);
            } else {
                throw new IllegalStateException(
                    "GRAPH EXPAND could not retarget aggregate [" + aggregate.sourceText() + "] onto the expanded frontier field"
                );
            }
        }
        for (Expression grouping : groupings) {
            Attribute attr = Expressions.attribute(grouping);
            if (attr == null) {
                throw new IllegalStateException(
                    "GRAPH EXPAND STATS grouping [" + grouping.sourceText() + "] did not resolve to an attribute"
                );
            }
            aggregates.add(attr);
        }
        if (degreeAttr != null) {
            // Every row of an edge pair comes from one frontier document, so its count is the same
            // on each; MAX keeps it through the collapse for the `dropped` column.
            aggregates.add(new Alias(source, HUB_DEGREE_COLUMN, new Max(source, degreeAttr)));
        }
        return new Aggregate(source, filteredEdges, groupings, aggregates);
    }

    /** Replace references to the pre-expand field with the attribute MV_EXPAND emits. */
    private static Expression retarget(Expression expression, Attribute from, Attribute to) {
        return expression.transformUp(Attribute.class, attribute -> attribute.semanticEquals(from) ? to : attribute);
    }

    private static Attribute retarget(Attribute attribute, Attribute from, Attribute to) {
        return attribute.semanticEquals(from) ? to : attribute;
    }

    /**
     * Rewrite an ip column in the hop rows to the Lucene 16-byte form, so an
     * {@code UNTIL} literal built from a watchlist compares equal to the node
     * the walk admitted. IPv4 and IPv4-mapped IPv6 are one address.
     */
    private void canonicalizeUntilIps(List<List<Object>> rows) {
        if (until instanceof In inExpr && inExpr.value() instanceof Attribute lhs && lhs.dataType() == DataType.IP) {
            int idx = -1;
            for (int i = 0; i < resultAttributes.size(); i++) {
                if (resultAttributes.get(i).name().equals(lhs.name())) {
                    idx = i;
                    break;
                }
            }
            if (idx < 0) {
                return;
            }
            for (List<Object> row : rows) {
                if (idx < row.size()) {
                    row.set(idx, canonicalIp(row.get(idx)));
                }
            }
        }
    }

    /**
     * One comparable form for an ip value. A 16-byte Lucene encoding is
     * decoded and re-encoded, which folds IPv4-mapped IPv6 back to IPv4.
     * A string is parsed the same way and returned as that encoding too,
     * because {@link Literal} of type {@code ip} stores the encoded bytes.
     */
    private static Object canonicalIp(Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof BytesRef bytes && bytes.length == 16) {
                byte[] raw = new byte[16];
                System.arraycopy(bytes.bytes, bytes.offset, raw, 0, 16);
                return new BytesRef(InetAddressPoint.encode(InetAddressPoint.decode(raw)));
            }
            String text = value instanceof BytesRef bytes ? bytes.utf8ToString() : value.toString();
            return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(text)));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid IP value [" + value + "]", e);
        }
    }

    // --- combine / sort / admission ----------------------------------------------

    /**
     * Concatenate outbound then inbound rows. A self-loop (stored source equals
     * target) is kept from the outbound leg only.
     */
    /**
     * Rows of a {@code both} hop, outbound ones (reached is {@code node_to}) first, then inbound.
     * A self-loop reaches itself from either side and both branches emit the same row; one copy
     * is kept, as the inbound copy was dropped when the legs ran separately.
     */
    private List<List<Object>> combineBothLegs(List<List<Object>> rows) {
        List<List<Object>> outRows = new ArrayList<>(rows.size());
        List<List<Object>> inRows = new ArrayList<>();
        Set<List<Object>> selfLoops = new HashSet<>();
        for (List<Object> row : rows) {
            Object from = row.get(nodeFromIdx);
            Object to = row.get(nodeToIdx);
            if (Objects.equals(from, to)) {
                if (selfLoops.add(row)) {
                    outRows.add(row);
                }
            } else if (Objects.equals(row.get(nodeReachedIdx), to)) {
                outRows.add(row);
            } else {
                inRows.add(row);
            }
        }
        outRows.addAll(inRows);
        return outRows;
    }

    private List<List<Object>> extractRows(Result hopResult) {
        List<Attribute> schema = hopResult.schema();
        int[] channels = new int[resultAttributes.size()];
        for (int c = 0; c < resultAttributes.size(); c++) {
            channels[c] = indexOf(schema, resultAttributes.get(c).name());
        }
        List<List<Object>> rows = new ArrayList<>();
        for (Page page : hopResult.pages()) {
            int positions = page.getPositionCount();
            for (int i = 0; i < positions; i++) {
                List<Object> row = new ArrayList<>(resultAttributes.size());
                for (int channel : channels) {
                    row.add(BlockUtils.toJavaObject(page.getBlock(channel), i));
                }
                // An edge touching an excluded node leaves before SORT, hub_degree, caps and
                // UNTIL see it, so the node is never admitted and nothing is walked beyond it.
                if (excluded.isEmpty() == false && (excluded.contains(row.get(nodeFromIdx)) || excluded.contains(row.get(nodeToIdx)))) {
                    continue;
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * Orders hop rows by the in-command {@code SORT} keys (if any), then always by
     * {@code node_reached} ascending so hop output is deterministic.
     */
    private void sortHopRows(List<List<Object>> rows) {
        if (rows.size() < 2) {
            return;
        }
        Comparator<List<Object>> comparator = hopRowComparator();
        rows.sort(comparator);
    }

    private Comparator<List<Object>> hopRowComparator() {
        Comparator<List<Object>> comparator = null;
        List<Order> sorts = graphExpand.sorts();
        if (sorts != null) {
            for (Order order : sorts) {
                String name = Expressions.attribute(order.child()).name();
                int channel = indexOf(resultAttributes, name);
                Comparator<List<Object>> key = comparingChannel(channel, order.direction(), order.nullsPosition());
                comparator = comparator == null ? key : comparator.thenComparing(key);
            }
        }
        Comparator<List<Object>> tieBreak = comparingChannel(nodeReachedIdx, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return comparator == null ? tieBreak : comparator.thenComparing(tieBreak);
    }

    private static Comparator<List<Object>> comparingChannel(int channel, Order.OrderDirection direction, Order.NullsPosition nulls) {
        return (left, right) -> {
            Object a = left.get(channel);
            Object b = right.get(channel);
            if (a == null || b == null) {
                if (a == null && b == null) {
                    return 0;
                }
                boolean nullFirst = nulls == Order.NullsPosition.FIRST;
                if (a == null) {
                    return nullFirst ? -1 : 1;
                }
                return nullFirst ? 1 : -1;
            }
            int cmp = compareSortValues(a, b);
            return direction == Order.OrderDirection.DESC ? -cmp : cmp;
        };
    }

    private static int compareSortValues(Object a, Object b) {
        if (a instanceof BytesRef aBytes && b instanceof BytesRef bBytes) {
            return aBytes.compareTo(bBytes);
        }
        if (a.getClass() == b.getClass() && a instanceof Comparable<?>) {
            // Same runtime class: safe to compare as Comparable<Object>.
            @SuppressWarnings("unchecked")
            Comparable<Object> comparable = (Comparable<Object>) a;
            return comparable.compareTo(b);
        }
        throw new IllegalArgumentException(
            "GRAPH EXPAND cannot compare sort values of [" + a.getClass().getName() + "] and [" + b.getClass().getName() + "]"
        );
    }

    /**
     * Refuses frontier nodes whose distinct {@code node_reached} count is
     * strictly greater than {@code hub_degree}. Runs after SORT and before the
     * three budget caps, while closing-edge targets still carry their
     * {@code node_reached} value. A refused node loses every edge and emits one
     * stub ({@code node_to}/{@code node_reached} null, {@code dropped} = degree)
     * that admits nobody and spends no budget. Degree equal to the cap keeps
     * every edge.
     */
    private void applyHubDegree(List<List<Object>> rows) {
        Integer hubDegree = optionInt("hub_degree");
        if (hubDegree == null || rows.isEmpty()) {
            return;
        }
        Map<Object, Set<Object>> distinctReached = new HashMap<>();
        for (List<Object> row : rows) {
            Object reached = row.get(nodeReachedIdx);
            if (reached == null) {
                continue;
            }
            Object via = frontierNode(row);
            distinctReached.computeIfAbsent(via, k -> new HashSet<>()).add(reached);
        }
        Set<Object> refused = new HashSet<>();
        for (Map.Entry<Object, Set<Object>> entry : distinctReached.entrySet()) {
            if (entry.getValue().size() > hubDegree) {
                refused.add(entry.getKey());
            }
        }
        int droppedIdx = indexOf(resultAttributes, "dropped");
        // The hub guard in the subplan reports a document's remaining pointer count in `dropped`
        // on every row. The stub of a refused node carries that count rather than the sliced
        // one; rows of a node that is not refused get it cleared, since only a stub reports a
        // degree.
        Map<Object, Integer> reportedDegree = new HashMap<>();
        if (droppedIdx >= 0) {
            for (List<Object> row : rows) {
                if (row.get(droppedIdx) instanceof Number reported) {
                    Object via = frontierNode(row);
                    if (refused.contains(via)) {
                        reportedDegree.merge(via, reported.intValue(), Math::max);
                    } else {
                        row.set(droppedIdx, null);
                    }
                }
            }
        }
        if (refused.isEmpty()) {
            return;
        }
        List<List<Object>> kept = new ArrayList<>(rows.size());
        Set<Object> stubEmitted = new HashSet<>();
        for (List<Object> row : rows) {
            Object via = frontierNode(row);
            if (refused.contains(via) == false) {
                kept.add(row);
                continue;
            }
            if (stubEmitted.add(via)) {
                int degree = Math.max(distinctReached.get(via).size(), reportedDegree.getOrDefault(via, 0));
                kept.add(hubStub(via, row, degree, droppedIdx));
            }
        }
        rows.clear();
        rows.addAll(kept);
    }

    /**
     * One refusal stub for a frontier node: admits nobody, carries the degree in
     * {@code dropped}, nulls STATS payload columns.
     */
    private List<Object> hubStub(Object frontierNodeId, List<Object> template, int degree, int droppedIdx) {
        List<Object> stub = new ArrayList<>(resultAttributes.size());
        for (int i = 0; i < resultAttributes.size(); i++) {
            stub.add(null);
        }
        stub.set(nodeFromIdx, frontierNodeId);
        // node_to / node_reached stay null — stub admits nobody and spends no budget
        stub.set(hopIdx, template.get(hopIdx));
        stub.set(droppedIdx, degree);
        return stub;
    }

    /**
     * Applies optional walk budgets after SORT/{@code hub_degree} and before
     * UNTIL. Closing edges (target already in {@link #visited} from an earlier
     * hop) and hub stubs ({@code node_reached} null) are kept and do not spend
     * budget. Rows removed by a cap are simply absent.
     */
    private void applyCaps(List<List<Object>> rows) {
        Integer maxEdgesPerNode = optionInt("max_edges_per_node");
        Integer maxFrontier = optionInt("max_frontier");
        Integer maxNodes = optionInt("max_nodes");
        if (maxEdgesPerNode == null && maxFrontier == null && maxNodes == null) {
            return;
        }
        // Snapshot at hop start: the seed (and prior admits) already count toward max_nodes.
        Set<Object> reachedBefore = Set.copyOf(visited);
        List<List<Object>> current = rows;
        if (maxEdgesPerNode != null) {
            current = applyFanOutCap(current, maxEdgesPerNode, reachedBefore);
        }
        if (maxFrontier != null) {
            current = applyNewNodeCap(current, maxFrontier, reachedBefore);
        }
        if (maxNodes != null) {
            int remaining = maxNodes - reachedBefore.size();
            current = applyNewNodeCap(current, Math.max(remaining, 0), reachedBefore);
        }
        if (current != rows) {
            rows.clear();
            rows.addAll(current);
        }
    }

    /**
     * Per frontier node, keep at most {@code maxEdgesPerNode} edges that would
     * admit a new node, in the already sorted order. Closing edges and hub stubs
     * are free. The frontier end is {@code node_from} on an outbound leg and
     * {@code node_to} on an inbound leg.
     */
    private List<List<Object>> applyFanOutCap(List<List<Object>> rows, int maxEdgesPerNode, Set<Object> reachedBefore) {
        Map<Object, Integer> spent = new HashMap<>();
        List<List<Object>> kept = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            Object reached = row.get(nodeReachedIdx);
            if (reached == null) {
                kept.add(row);
                continue;
            }
            if (reachedBefore.contains(reached)) {
                kept.add(row);
                continue;
            }
            Object via = frontierNode(row);
            int count = spent.getOrDefault(via, 0);
            if (count >= maxEdgesPerNode) {
                continue;
            }
            spent.put(via, count + 1);
            kept.add(row);
        }
        return kept;
    }

    /**
     * Keep edges for at most {@code limit} distinct newly reached nodes (in
     * current order). Edges to nodes past that cut are dropped; closing edges,
     * hub stubs, and further edges onto an already-kept new node are kept.
     * {@code limit} may be 0 (admit no new nodes).
     */
    private List<List<Object>> applyNewNodeCap(List<List<Object>> rows, int limit, Set<Object> reachedBefore) {
        LinkedHashSet<Object> keptNew = new LinkedHashSet<>();
        List<List<Object>> kept = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            Object reached = row.get(nodeReachedIdx);
            if (reached == null) {
                kept.add(row);
                continue;
            }
            if (reachedBefore.contains(reached)) {
                kept.add(row);
                continue;
            }
            if (keptNew.contains(reached)) {
                kept.add(row);
                continue;
            }
            if (keptNew.size() >= limit) {
                continue;
            }
            keptNew.add(reached);
            kept.add(row);
        }
        return kept;
    }

    /**
     * Endpoint that matched the frontier for this hop leg: stored source on an
     * outbound walk ({@code node_reached == node_to}), stored target on inbound.
     */
    private Object frontierNode(List<Object> row) {
        Object from = row.get(nodeFromIdx);
        Object to = row.get(nodeToIdx);
        Object reached = row.get(nodeReachedIdx);
        if (Objects.equals(reached, to)) {
            return from;
        }
        if (Objects.equals(reached, from)) {
            return to;
        }
        return from;
    }

    /**
     * {@code closing_edges}, default true: whether an edge onto a node admitted on an earlier hop
     * is reported (with {@code node_reached} null). False drops such edges inside the hop query.
     */
    private boolean closingEdges() {
        MapExpression options = graphExpand.options();
        if (options == null) {
            return true;
        }
        Expression value = options.keyFoldedMap().get("closing_edges");
        return value == null || Boolean.TRUE.equals(value.fold(FoldContext.small()));
    }

    private Integer optionInt(String key) {
        MapExpression options = graphExpand.options();
        if (options == null) {
            return null;
        }
        Expression value = options.keyFoldedMap().get(key);
        if (value == null) {
            return null;
        }
        return ((Number) value.fold(FoldContext.small())).intValue();
    }

    private List<Object> admitRows(List<List<Object>> rows) {
        int hopStart = admittedRows.size();
        LinkedHashSet<Object> newlyAdmitted = new LinkedHashSet<>();
        for (List<Object> row : rows) {
            Object reached = row.get(nodeReachedIdx);
            if (reached == null) {
                // Hub stub: emit as-is, admit nobody, do not frontier.
                admittedRows.add(row);
                continue;
            }
            // Target admitted on an earlier hop (not this hop): closing edge.
            // Emit with node_reached null; do not re-frontier the node.
            if (visited.contains(reached) && newlyAdmitted.contains(reached) == false) {
                List<Object> closing = new ArrayList<>(row);
                closing.set(nodeReachedIdx, null);
                admittedRows.add(closing);
                continue;
            }
            boolean firstTimeThisHop = newlyAdmitted.add(reached);
            if (firstTimeThisHop) {
                visited.add(reached);
            }
            // Emit every aggregated row for a node newly admitted this hop (BY may
            // produce several rows that share one node_reached). Frontier gets the
            // node once via newlyAdmitted.
            admittedRows.add(row);
        }
        // Closing edges null node_reached after the pre-admit SORT; re-apply the
        // hop comparator so nulls land last among this hop's emitted rows.
        if (admittedRows.size() - hopStart >= 2) {
            admittedRows.subList(hopStart, admittedRows.size()).sort(hopRowComparator());
        }
        return new ArrayList<>(newlyAdmitted);
    }

    private LocalRelation resultsRelation() {
        Source source = graphExpand.source();
        if (admittedRows.isEmpty()) {
            Block[] empty = new Block[resultAttributes.size()];
            for (int i = 0; i < resultAttributes.size(); i++) {
                empty[i] = blockFactory.newConstantNullBlock(0);
            }
            return new LocalRelation(source, resultAttributes, LocalSupplier.of(new Page(empty)));
        }
        Block[] blocks = BlockUtils.fromList(blockFactory, admittedRows);
        return new LocalRelation(source, resultAttributes, LocalSupplier.of(new Page(blocks)));
    }

    // --- subset / options --------------------------------------------------------

    static void validateSubset(GraphExpand ge) {
        if (ge.aggregateFilter() != null && ge.aggregates() == null) {
            throw new IllegalArgumentException("GRAPH EXPAND aggregate WHERE requires STATS");
        }
        // Single-column uncorrelated InSubquery is resolved by the driver before hop 1.
        // Multi-column form stays refused (analysis should already have caught it).
        if (ge.until() != null && ge.until().anyMatch(e -> e instanceof MultiColumnInSubquery)) {
            throw new IllegalArgumentException("GRAPH EXPAND UNTIL subquery form is not supported yet");
        }
        MapExpression options = ge.options();
        if (options == null) {
            return;
        }
        options.keyFoldedMap().forEach((key, value) -> {
            if (ALLOWED_OPTIONS.contains(key) == false) {
                throw new IllegalArgumentException("GRAPH EXPAND option [" + key + "] is not supported in this build");
            }
            if (key.equals("direction")) {
                String direction = BytesRefs.toString(value.fold(FoldContext.small())).toLowerCase(Locale.ROOT);
                if (direction.equals("out") == false && direction.equals("in") == false && direction.equals("both") == false) {
                    throw new IllegalArgumentException("GRAPH EXPAND direction [" + direction + "] is not supported in this build");
                }
            }
        });
    }

    private static boolean untilContainsInSubquery(Expression until) {
        return until.anyMatch(e -> e instanceof InSubquery);
    }

    private static LogicalPlan untilSubqueryPlan(Expression until) {
        InSubquery inSub = findUntilInSubquery(until);
        if (inSub == null) {
            throw new IllegalStateException("GRAPH EXPAND expected an UNTIL InSubquery plan");
        }
        return inSub.subquery();
    }

    private static InSubquery findUntilInSubquery(Expression until) {
        var holder = new org.elasticsearch.xpack.esql.core.util.Holder<InSubquery>();
        until.forEachDown(InSubquery.class, inSub -> {
            if (holder.get() == null) {
                holder.set(inSub);
            }
        });
        return holder.get();
    }

    private static int maxHops(GraphExpand ge) {
        MapExpression options = ge.options();
        if (options == null) {
            return DEFAULT_MAX_HOPS;
        }
        Expression value = options.keyFoldedMap().get("max_hops");
        if (value == null) {
            return DEFAULT_MAX_HOPS;
        }
        Number n = (Number) value.fold(FoldContext.small());
        return n.intValue();
    }

    private static String direction(GraphExpand ge) {
        MapExpression options = ge.options();
        if (options == null) {
            return "out";
        }
        Expression value = options.keyFoldedMap().get("direction");
        if (value == null) {
            return "out";
        }
        return BytesRefs.toString(value.fold(FoldContext.small())).toLowerCase(Locale.ROOT);
    }

    private static List<Object> readColumnValues(LocalRelation relation, Attribute column) {
        List<Attribute> schema = relation.output();
        int channel = -1;
        for (int i = 0; i < schema.size(); i++) {
            if (Objects.equals(schema.get(i).id(), column.id()) || schema.get(i).name().equals(column.name())) {
                channel = i;
                break;
            }
        }
        if (channel < 0) {
            throw new IllegalArgumentException("GRAPH EXPAND seed column [" + column.name() + "] not found in seed relation");
        }
        Page page = relation.supplier().get();
        if (page == null || page.getPositionCount() == 0) {
            return List.of();
        }
        Block block = page.getBlock(channel);
        List<Object> values = new ArrayList<>(page.getPositionCount());
        for (int i = 0; i < page.getPositionCount(); i++) {
            Object v = BlockUtils.toJavaObject(block, i);
            if (v != null) {
                values.add(v);
            }
        }
        return values;
    }

    private static int indexOf(List<Attribute> schema, String name) {
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalStateException("hop result missing column [" + name + "]");
    }

    private static Attribute findNamed(List<Attribute> schema, String name) {
        for (Attribute attribute : schema) {
            if (attribute.name().equals(name)) {
                return attribute;
            }
        }
        return null;
    }

    private static Attribute attributeByName(List<Attribute> schema, String name) {
        for (Attribute attribute : schema) {
            if (attribute.name().equals(name)) {
                return attribute;
            }
        }
        throw new IllegalStateException("expected column [" + name + "] in hop intermediate output");
    }

    /** Builds the four walk output attributes once analysis has resolved the TO field type. */
    public static List<Attribute> buildResultAttributes(Source source, DataType nodeType) {
        return buildResultAttributes(source, nodeType, null, null, false, false);
    }

    /**
     * Walk columns {@code node_from}, {@code node_to}, {@code node_reached}, {@code hop},
     * then STATS output columns in written order (aggregates, then user {@code BY}),
     * then optional {@code dropped} when {@code hub_degree} is set.
     */
    public static List<Attribute> buildResultAttributes(
        Source source,
        DataType nodeType,
        @Nullable List<? extends NamedExpression> aggregates,
        @Nullable List<Expression> groupings
    ) {
        return buildResultAttributes(source, nodeType, aggregates, groupings, false, false);
    }

    /**
     * Walk columns {@code node_from}, {@code node_to}, {@code node_reached}, {@code hop},
     * optional {@code relation} when {@code includeRelation} is true (multi-field TO),
     * then STATS output columns in written order (aggregates, then user {@code BY}).
     * When {@code includeDropped} is true (query set {@code hub_degree}), appends
     * {@code dropped} after those columns.
     */
    public static List<Attribute> buildResultAttributes(
        Source source,
        DataType nodeType,
        @Nullable List<? extends NamedExpression> aggregates,
        @Nullable List<Expression> groupings,
        boolean includeDropped
    ) {
        return buildResultAttributes(source, nodeType, aggregates, groupings, includeDropped, false);
    }

    /**
     * Walk columns {@code node_from}, {@code node_to}, {@code node_reached}, {@code hop},
     * optional {@code relation} when {@code includeRelation} is true (multi-field TO),
     * then STATS output columns in written order (aggregates, then user {@code BY}).
     * When {@code includeDropped} is true (query set {@code hub_degree}), appends
     * {@code dropped} after those columns.
     */
    public static List<Attribute> buildResultAttributes(
        Source source,
        DataType nodeType,
        @Nullable List<? extends NamedExpression> aggregates,
        @Nullable List<Expression> groupings,
        boolean includeDropped,
        boolean includeRelation
    ) {
        // Not synthetic: Analyzer.planWithoutSyntheticAttributes would strip them from the
        // query output (leaving an empty Project) if they were marked synthetic.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new ReferenceAttribute(source, null, "node_from", nodeType, Nullability.TRUE, null, false));
        attributes.add(new ReferenceAttribute(source, null, "node_to", nodeType, Nullability.TRUE, null, false));
        attributes.add(new ReferenceAttribute(source, null, "node_reached", nodeType, Nullability.TRUE, null, false));
        attributes.add(new ReferenceAttribute(source, null, "hop", DataType.INTEGER, Nullability.TRUE, null, false));
        if (includeRelation) {
            attributes.add(new ReferenceAttribute(source, null, "relation", DataType.KEYWORD, Nullability.TRUE, null, false));
        }
        if (aggregates != null) {
            for (NamedExpression aggregate : aggregates) {
                attributes.add(aggregate.toAttribute());
            }
        }
        if (groupings != null) {
            for (Expression grouping : groupings) {
                Attribute attr = Expressions.attribute(grouping);
                if (attr != null) {
                    attributes.add(attr);
                } else if (grouping instanceof NamedExpression named) {
                    attributes.add(named.toAttribute());
                } else {
                    attributes.add(
                        new ReferenceAttribute(
                            grouping.source(),
                            null,
                            grouping.sourceText(),
                            grouping.dataType(),
                            Nullability.TRUE,
                            null,
                            false
                        )
                    );
                }
            }
        }
        if (includeDropped) {
            attributes.add(new ReferenceAttribute(source, null, "dropped", DataType.INTEGER, Nullability.TRUE, null, false));
        }
        return List.copyOf(attributes);
    }

    /** True when the expand options map names {@code hub_degree}. */
    public static boolean hasHubDegree(@Nullable MapExpression options) {
        return options != null && options.keyFoldedMap().containsKey("hub_degree");
    }

    /** True when {@code TO} lists more than one target field (emits {@code relation}). */
    public static boolean isMultiFieldTo(List<Attribute> targetFields) {
        return targetFields != null && targetFields.size() > 1;
    }
}
