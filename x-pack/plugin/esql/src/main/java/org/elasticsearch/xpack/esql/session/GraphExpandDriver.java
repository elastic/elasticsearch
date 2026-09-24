/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.GraphExpand;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
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

/**
 * Coordinator-side visited-set BFS for {@link GraphExpand}.
 * <p>
 * Each hop is a full plan/dispatch round through {@link EsqlSession#executeSubPlan}.
 * Without STATS the hop is {@code Filter → Eval → Project}. With STATS an
 * {@link Aggregate} sits on the filtered edge scan (pair grouping always, user
 * {@code BY} refining it) before Eval/Project. Document {@code WHERE} is ANDed
 * into the edge {@link Filter}; aggregate {@code WHERE} is a {@link Filter} on
 * the Aggregate output before admission. {@code direction: both} runs outbound
 * then inbound as separate hop plans and combines pages before SORT and
 * admission. A multi-field {@code TO (f1, f2, …)} runs the same leg(s) once per
 * target field in written order, concatenates those field legs (each row stamped
 * with {@code relation}), then applies SORT / hub_degree / caps / UNTIL /
 * admission once on the union — caps therefore bind to the combined rows, not
 * per field. In-command {@code SORT} (plus an always-on {@code node_reached}
 * ascending tie-break) orders each hop's rows, then {@code hub_degree} may
 * refuse a frontier node (stub row, no budget spend) before the three caps
 * {@code max_edges_per_node}, {@code max_frontier}, {@code max_nodes} — a row
 * removed by a cap cannot satisfy {@code UNTIL} and is not admitted. When
 * {@code UNTIL} holds an {@link InSubquery}, that subquery is executed once
 * before hop 1 and rewritten to an ordinary {@link In} of literals; the hop
 * walk then uses the same Filter-over-hop-rows path as a literal UNTIL. When
 * {@code UNTIL} is present, the remaining hop rows are filtered with that
 * boolean expression (ordinary {@link Filter} over the emit columns); the first
 * matching row and every row before it are admitted, later rows are dropped,
 * and the walk stops. An edge onto a node admitted on an earlier hop is a
 * closing edge: it is emitted with {@code node_reached} null, does not
 * re-enter the frontier, and does not spend any of the three budgets.
 * Same-hop duplicates of a newly admitted {@code node_reached} keep that value
 * and enter the frontier once. Stage subset — see {@link #validateSubset}.
 */
public final class GraphExpandDriver {

    public static final int DEFAULT_MAX_HOPS = 3;

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
    private final List<Attribute> resultAttributes;
    private final DataType nodeType;

    /** Nodes already admitted (including seeds). */
    private final Set<Object> visited = new HashSet<>();
    /** Nodes to expand on the next hop. */
    private List<Object> frontier = new ArrayList<>();
    /** Depth of the next hop to run (1-based). */
    private int nextHop = 1;
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
    private int nextFieldIndex;
    /** Outbound rows buffered while the inbound leg of {@code direction: both} runs. */
    private List<List<Object>> pendingOutRows;
    /**
     * Rows from completed TO-field legs of the current hop, waiting for remaining
     * fields before SORT / caps. {@code null} when no multi-field buffering is active.
     */
    private List<List<Object>> pendingFieldRows;

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
        this.resultAttributes = resultAttributes;
        this.nodeType = targetFields.get(0).dataType();
        this.nextLeg = initialLeg();
        this.nextFieldIndex = 0;
        this.until = graphExpand.until();
        this.untilSubqueryDone = until == null || untilContainsInSubquery(until) == false;
        for (Object seed : seeds) {
            visited.add(seed);
            frontier.add(seed);
        }
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
                "GRAPH EXPAND seed input must be a local relation after optimization, got ["
                    + ge.child().getClass().getSimpleName()
                    + "]"
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
            finished = true;
            return null;
        }
        LogicalPlan hopPlan = buildHopPlan(nextHop, frontier, nextLeg, targetFields.get(nextFieldIndex));
        hopPlan.setOptimized();
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

        if ("both".equals(direction) && nextLeg == Leg.OUT) {
            pendingOutRows = rows;
            nextLeg = Leg.IN;
            return mainPlan;
        }

        if ("both".equals(direction) && nextLeg == Leg.IN) {
            rows = combineBothLegs(pendingOutRows, rows);
            pendingOutRows = null;
            nextLeg = initialLeg();
        }

        // Multi-field TO: concatenate field legs in written order, then SORT/caps once.
        if (multiField) {
            if (pendingFieldRows == null) {
                pendingFieldRows = new ArrayList<>();
            }
            pendingFieldRows.addAll(rows);
            nextFieldIndex++;
            if (nextFieldIndex < targetFields.size()) {
                nextLeg = initialLeg();
                return mainPlan;
            }
            rows = pendingFieldRows;
            pendingFieldRows = null;
            nextFieldIndex = 0;
        }

        // SORT → hub_degree → caps → UNTIL → admission — UNTIL is not a post-filter after all hops.
        sortHopRows(rows);
        applyHubDegree(rows);
        applyCaps(rows);
        if (until != null && rows.isEmpty() == false) {
            pendingUntilRows = rows;
            awaitingUntilFilter = true;
            return mainPlan;
        }
        return admitAndAdvance(mainPlan, rows, false);
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
            throw new IllegalArgumentException(
                "GRAPH EXPAND UNTIL subquery must return exactly one column, got [" + schema.size() + "]"
            );
        }
        DataType valueType = schema.get(0).dataType();
        List<Expression> literals = new ArrayList<>();
        for (Page page : subqueryResult.pages()) {
            Block block = page.getBlock(0);
            for (int i = 0; i < page.getPositionCount(); i++) {
                Object v = BlockUtils.toJavaObject(block, i);
                if (v != null) {
                    literals.add(new Literal(graphExpand.source(), v, valueType));
                }
            }
        }
        if (literals.isEmpty()) {
            throw new IllegalArgumentException(
                "GRAPH EXPAND UNTIL subquery returned no values, so the walk would stop at nothing"
            );
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
        nextFieldIndex = 0;
        nextLeg = initialLeg();
        if (untilMatched || frontier.isEmpty() || nextHop > maxHops) {
            finished = true;
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

    // --- hop plan ----------------------------------------------------------------

    private LogicalPlan buildHopPlan(int hop, List<Object> frontierValues, Leg leg, Attribute targetField) {
        Source source = graphExpand.source();
        List<Expression> literals = new ArrayList<>(frontierValues.size());
        for (Object value : frontierValues) {
            literals.add(new Literal(source, value, nodeType));
        }
        // out: frontier matches ON (stored source). in: frontier matches TO (stored target).
        Attribute frontierField = leg == Leg.OUT ? matchField : targetField;
        In inPredicate = new In(source, frontierField, literals);
        // Null pointer values emit no row (TO IS NOT NULL on the scanned documents).
        Expression notNullPointer = new IsNotNull(source, targetField);
        Expression edgePredicate = Predicates.combineAnd(
            graphExpand.documentFilter() != null
                ? List.of(inPredicate, notNullPointer, graphExpand.documentFilter())
                : List.of(inPredicate, notNullPointer)
        );
        LogicalPlan hopChild = new Filter(source, graphExpand.edgeRelation(), edgePredicate);

        Attribute nodeFrom = resultAttributes.get(0);
        Attribute nodeTo = resultAttributes.get(1);
        Attribute nodeReached = resultAttributes.get(2);
        Attribute hopAttr = resultAttributes.get(3);

        Attribute evalFrom = matchField;
        Attribute evalTo = targetField;
        if (graphExpand.aggregates() != null) {
            Aggregate aggregate = buildHopAggregate(source, hopChild, targetField);
            hopChild = aggregate;
            // Aggregate WHERE filters collapsed edges after STATS, before admission.
            if (graphExpand.aggregateFilter() != null) {
                hopChild = new Filter(source, aggregate, graphExpand.aggregateFilter());
            }
            evalFrom = attributeByName(hopChild.output(), matchField.name());
            evalTo = attributeByName(hopChild.output(), targetField.name());
        }

        // node_from / node_to keep stored orientation; node_reached follows the walk leg.
        Attribute reachedExpr = leg == Leg.OUT ? evalTo : evalFrom;
        List<Alias> evalFields = new ArrayList<>(6);
        evalFields.add(new Alias(source, nodeFrom.name(), evalFrom, nodeFrom.id(), false));
        evalFields.add(new Alias(source, nodeTo.name(), evalTo, nodeTo.id(), false));
        evalFields.add(new Alias(source, nodeReached.name(), reachedExpr, nodeReached.id(), false));
        evalFields.add(new Alias(source, hopAttr.name(), new Literal(source, hop, DataType.INTEGER), hopAttr.id(), false));
        // Multi-field TO only: relation = the pointer field name that fired this leg.
        Attribute relationAttr = findNamed(resultAttributes, "relation");
        if (relationAttr != null) {
            evalFields.add(
                new Alias(source, relationAttr.name(), Literal.keyword(source, targetField.name()), relationAttr.id(), false)
            );
        }
        // Optional dropped: null on every hop row; hub stubs overwrite with the refused degree.
        // Must live on Eval (Literal) — Project only accepts Attribute children.
        Attribute droppedAttr = findNamed(resultAttributes, "dropped");
        if (droppedAttr != null) {
            evalFields.add(
                new Alias(source, droppedAttr.name(), new Literal(source, null, DataType.INTEGER), droppedAttr.id(), false)
            );
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
        return new Project(source, eval, projections);
    }

    /**
     * Ordinary ES|QL {@link Aggregate} over hop documents. Grouping is always the
     * endpoint pair ({@code match}, {@code TO}); a user {@code BY} refines that
     * pair. Aggregate expressions are those already resolved on {@link GraphExpand}.
     */
    private Aggregate buildHopAggregate(Source source, LogicalPlan filteredEdges, Attribute targetField) {
        List<? extends NamedExpression> userAggregates = graphExpand.aggregates();
        List<Expression> userGroupings = graphExpand.groupings() != null ? graphExpand.groupings() : List.of();

        List<Expression> groupings = new ArrayList<>(2 + userGroupings.size());
        groupings.add(matchField);
        groupings.add(targetField);
        groupings.addAll(userGroupings);

        // Same shape as ParserUtils.buildStats: user aggs first, then grouping keys
        // so Aggregate.output() carries the pair (for Eval) and any user BY columns.
        List<NamedExpression> aggregates = new ArrayList<>(userAggregates.size() + groupings.size());
        aggregates.addAll(userAggregates);
        for (Expression grouping : groupings) {
            Attribute attr = Expressions.attribute(grouping);
            if (attr == null) {
                throw new IllegalStateException(
                    "GRAPH EXPAND STATS grouping [" + grouping.sourceText() + "] did not resolve to an attribute"
                );
            }
            aggregates.add(attr);
        }
        return new Aggregate(source, filteredEdges, groupings, aggregates);
    }

    // --- combine / sort / admission ----------------------------------------------

    /**
     * Concatenate outbound then inbound rows. A self-loop (stored source equals
     * target) is kept from the outbound leg only.
     */
    private static List<List<Object>> combineBothLegs(List<List<Object>> outRows, List<List<Object>> inRows) {
        List<List<Object>> combined = new ArrayList<>(outRows.size() + inRows.size());
        combined.addAll(outRows);
        for (List<Object> row : inRows) {
            Object from = row.get(0);
            Object to = row.get(1);
            if (Objects.equals(from, to)) {
                continue;
            }
            combined.add(row);
        }
        return combined;
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
        // Always-on tie-break: node_reached ascending (channel 2).
        Comparator<List<Object>> tieBreak = comparingChannel(2, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return comparator == null ? tieBreak : comparator.thenComparing(tieBreak);
    }

    private static Comparator<List<Object>> comparingChannel(
        int channel,
        Order.OrderDirection direction,
        Order.NullsPosition nulls
    ) {
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static int compareSortValues(Object a, Object b) {
        if (a instanceof Comparable comparable && a.getClass().isInstance(b)) {
            return comparable.compareTo(b);
        }
        if (b instanceof Comparable comparable && b.getClass().isInstance(a)) {
            return -comparable.compareTo(a);
        }
        return a.toString().compareTo(b.toString());
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
            Object reached = row.get(2);
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
        if (refused.isEmpty()) {
            return;
        }
        int droppedIdx = indexOf(resultAttributes, "dropped");
        List<List<Object>> kept = new ArrayList<>(rows.size());
        Set<Object> stubEmitted = new HashSet<>();
        for (List<Object> row : rows) {
            Object via = frontierNode(row);
            if (refused.contains(via) == false) {
                kept.add(row);
                continue;
            }
            if (stubEmitted.add(via)) {
                kept.add(hubStub(via, row, distinctReached.get(via).size(), droppedIdx));
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
        stub.set(0, frontierNodeId); // node_from
        // node_to / node_reached stay null
        stub.set(3, template.get(3)); // hop
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
    private static List<List<Object>> applyFanOutCap(List<List<Object>> rows, int maxEdgesPerNode, Set<Object> reachedBefore) {
        Map<Object, Integer> spent = new HashMap<>();
        List<List<Object>> kept = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            Object reached = row.get(2);
            if (reached == null) {
                // Hub stub — keep, no budget.
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
    private static List<List<Object>> applyNewNodeCap(List<List<Object>> rows, int limit, Set<Object> reachedBefore) {
        LinkedHashSet<Object> keptNew = new LinkedHashSet<>();
        List<List<Object>> kept = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            Object reached = row.get(2);
            if (reached == null) {
                // Hub stub — keep, no budget.
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
    private static Object frontierNode(List<Object> row) {
        Object from = row.get(0);
        Object to = row.get(1);
        Object reached = row.get(2);
        if (Objects.equals(reached, to)) {
            return from;
        }
        if (Objects.equals(reached, from)) {
            return to;
        }
        return from;
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
        int reachedIdx = 2; // node_reached
        int hopStart = admittedRows.size();
        LinkedHashSet<Object> newlyAdmitted = new LinkedHashSet<>();
        for (List<Object> row : rows) {
            Object reached = row.get(reachedIdx);
            if (reached == null) {
                // Hub stub: emit as-is, admit nobody, do not frontier.
                admittedRows.add(row);
                continue;
            }
            // Target admitted on an earlier hop (not this hop): closing edge.
            // Emit with node_reached null; do not re-frontier the node.
            if (visited.contains(reached) && newlyAdmitted.contains(reached) == false) {
                List<Object> closing = new ArrayList<>(row);
                closing.set(reachedIdx, null);
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
            switch (key) {
                case "max_hops" -> {
                    // allowed
                }
                case "direction" -> {
                    String direction = BytesRefs.toString(value.fold(FoldContext.small())).toLowerCase(Locale.ROOT);
                    if (direction.equals("out") == false && direction.equals("in") == false && direction.equals("both") == false) {
                        throw new IllegalArgumentException(
                            "GRAPH EXPAND direction [" + direction + "] is not supported in this build"
                        );
                    }
                }
                case "hub_degree" -> {
                    // allowed — applied after SORT and before the three budget caps
                }
                case "max_edges_per_node", "max_nodes", "max_frontier" -> {
                    // allowed — applied after SORT/hub_degree and before UNTIL in applyCaps
                }
                default -> throw new IllegalArgumentException("GRAPH EXPAND option [" + key + "] is not supported in this build");
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
                        new ReferenceAttribute(grouping.source(), null, grouping.sourceText(), grouping.dataType(), Nullability.TRUE, null, false)
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
