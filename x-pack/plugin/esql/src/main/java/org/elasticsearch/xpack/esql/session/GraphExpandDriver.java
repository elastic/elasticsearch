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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
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
 * admission. In-command {@code SORT} (plus an always-on {@code node_reached}
 * ascending tie-break) orders each hop's rows before admission. When
 * {@code UNTIL} is present, the sorted hop rows are then filtered with that
 * boolean expression (ordinary {@link Filter} over the emit columns); the first
 * matching row and every row before it are admitted, later rows are dropped,
 * and the walk stops. Admitted nodes are remembered here so a later hop does
 * not re-admit them. Stage subset — see {@link #validateSubset}.
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
    private final Attribute targetField;
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
     * Next leg to execute for the current hop. For {@code out}/{@code in} this
     * stays on that single leg; for {@code both} it advances OUT → IN within the hop.
     */
    private Leg nextLeg;
    /** Outbound rows buffered while the inbound leg of {@code direction: both} runs. */
    private List<List<Object>> pendingOutRows;

    /**
     * Sorted hop rows waiting for an {@code UNTIL} {@link Filter} subplan, or
     * {@code null} when no until-filter is in flight.
     */
    private List<List<Object>> pendingUntilRows;
    /** True while {@link #firstSubPlan} should return the UNTIL Filter over {@link #pendingUntilRows}. */
    private boolean awaitingUntilFilter;

    private GraphExpandDriver(
        GraphExpand graphExpand,
        BlockFactory blockFactory,
        int maxHops,
        String direction,
        Attribute matchField,
        Attribute targetField,
        List<Attribute> resultAttributes,
        List<Object> seeds
    ) {
        this.graphExpand = graphExpand;
        this.blockFactory = blockFactory;
        this.maxHops = maxHops;
        this.direction = direction;
        this.matchField = matchField;
        this.targetField = targetField;
        this.resultAttributes = resultAttributes;
        this.nodeType = targetField.dataType();
        this.nextLeg = "in".equals(direction) ? Leg.IN : Leg.OUT;
        for (Object seed : seeds) {
            visited.add(seed);
            frontier.add(seed);
        }
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
            ge.targetFields().get(0),
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
     * Next hop (or hop-leg) plan to execute, the in-flight {@code UNTIL} Filter,
     * or {@code null} when the walk is done.
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
        if (frontier.isEmpty() || nextHop > maxHops) {
            finished = true;
            return null;
        }
        LogicalPlan hopPlan = buildHopPlan(nextHop, frontier, nextLeg);
        hopPlan.setOptimized();
        return hopPlan;
    }

    /**
     * Consumes a hop {@link Result} (or an {@code UNTIL} Filter result), updates
     * visited/frontier, and either keeps {@code mainPlan} for another hop (or
     * the inbound leg of {@code both}, or the UNTIL Filter) or replaces
     * {@link GraphExpand} with the accumulated admission rows.
     */
    public LogicalPlan newMainPlan(LogicalPlan mainPlan, Result hopResult) {
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
            nextLeg = Leg.OUT;
        }

        // SORT then UNTIL then admission — UNTIL is not a post-filter after all hops.
        sortHopRows(rows);
        if (graphExpand.until() != null && rows.isEmpty() == false) {
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
        return new Filter(source, local, graphExpand.until());
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

    private LogicalPlan buildHopPlan(int hop, List<Object> frontierValues, Leg leg) {
        Source source = graphExpand.source();
        List<Expression> literals = new ArrayList<>(frontierValues.size());
        for (Object value : frontierValues) {
            literals.add(new Literal(source, value, nodeType));
        }
        // out: frontier matches ON (stored source). in: frontier matches TO (stored target).
        Attribute frontierField = leg == Leg.OUT ? matchField : targetField;
        In inPredicate = new In(source, frontierField, literals);
        // Document WHERE filters hop documents before STATS (and before emit when there is no STATS).
        Expression edgePredicate = graphExpand.documentFilter() != null
            ? Predicates.combineAnd(List.of(inPredicate, graphExpand.documentFilter()))
            : inPredicate;
        LogicalPlan hopChild = new Filter(source, graphExpand.edgeRelation(), edgePredicate);

        Attribute nodeFrom = resultAttributes.get(0);
        Attribute nodeTo = resultAttributes.get(1);
        Attribute nodeReached = resultAttributes.get(2);
        Attribute hopAttr = resultAttributes.get(3);

        Attribute evalFrom = matchField;
        Attribute evalTo = targetField;
        if (graphExpand.aggregates() != null) {
            Aggregate aggregate = buildHopAggregate(source, hopChild);
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
        List<Alias> evalFields = List.of(
            new Alias(source, nodeFrom.name(), evalFrom, nodeFrom.id(), false),
            new Alias(source, nodeTo.name(), evalTo, nodeTo.id(), false),
            new Alias(source, nodeReached.name(), reachedExpr, nodeReached.id(), false),
            new Alias(source, hopAttr.name(), new Literal(source, hop, DataType.INTEGER), hopAttr.id(), false)
        );
        Eval eval = new Eval(source, hopChild, evalFields);

        List<NamedExpression> projections = new ArrayList<>(resultAttributes.size());
        for (Alias alias : evalFields) {
            projections.add(alias.toAttribute());
        }
        // STATS payload columns after hop (aggregates then user BY), in written order.
        for (int i = 4; i < resultAttributes.size(); i++) {
            Attribute wanted = resultAttributes.get(i);
            projections.add(attributeByName(eval.output(), wanted.name()));
        }
        return new Project(source, eval, projections);
    }

    /**
     * Ordinary ES|QL {@link Aggregate} over hop documents. Grouping is always the
     * endpoint pair ({@code match}, {@code TO}); a user {@code BY} refines that
     * pair. Aggregate expressions are those already resolved on {@link GraphExpand}.
     */
    private Aggregate buildHopAggregate(Source source, LogicalPlan filteredEdges) {
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

    private List<Object> admitRows(List<List<Object>> rows) {
        int reachedIdx = 2; // node_reached
        LinkedHashSet<Object> newlyAdmitted = new LinkedHashSet<>();
        for (List<Object> row : rows) {
            Object reached = row.get(reachedIdx);
            if (reached == null) {
                continue;
            }
            // Target admitted on an earlier hop: closing edge — drop the row.
            if (visited.contains(reached) && newlyAdmitted.contains(reached) == false) {
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
        if (ge.until() != null && untilContainsSubquery(ge.until())) {
            throw new IllegalArgumentException("GRAPH EXPAND UNTIL subquery form is not supported yet");
        }
        if (ge.targetFields().size() != 1) {
            throw new IllegalArgumentException(
                "GRAPH EXPAND multi-field TO is not supported in this build, got "
                    + ge.targetFields().size()
                    + " target fields"
            );
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
                case "hub_degree" -> throw new IllegalArgumentException("GRAPH EXPAND hub_degree is not supported in this build");
                case "max_edges_per_node" -> throw new IllegalArgumentException(
                    "GRAPH EXPAND max_edges_per_node is not supported in this build"
                );
                case "max_nodes" -> throw new IllegalArgumentException("GRAPH EXPAND max_nodes is not supported in this build");
                case "max_frontier" -> throw new IllegalArgumentException("GRAPH EXPAND max_frontier is not supported in this build");
                default -> throw new IllegalArgumentException("GRAPH EXPAND option [" + key + "] is not supported in this build");
            }
        });
    }

    private static boolean untilContainsSubquery(Expression until) {
        return until.anyMatch(e -> e instanceof InSubquery || e instanceof MultiColumnInSubquery);
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
        return buildResultAttributes(source, nodeType, null, null);
    }

    /**
     * Walk columns {@code node_from}, {@code node_to}, {@code node_reached}, {@code hop},
     * then STATS output columns in written order (aggregates, then user {@code BY}).
     */
    public static List<Attribute> buildResultAttributes(
        Source source,
        DataType nodeType,
        @Nullable List<? extends NamedExpression> aggregates,
        @Nullable List<Expression> groupings
    ) {
        // Not synthetic: Analyzer.planWithoutSyntheticAttributes would strip them from the
        // query output (leaving an empty Project) if they were marked synthetic.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new ReferenceAttribute(source, null, "node_from", nodeType, Nullability.TRUE, null, false));
        attributes.add(new ReferenceAttribute(source, null, "node_to", nodeType, Nullability.TRUE, null, false));
        attributes.add(new ReferenceAttribute(source, null, "node_reached", nodeType, Nullability.TRUE, null, false));
        attributes.add(new ReferenceAttribute(source, null, "hop", DataType.INTEGER, Nullability.TRUE, null, false));
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
        return List.copyOf(attributes);
    }
}
