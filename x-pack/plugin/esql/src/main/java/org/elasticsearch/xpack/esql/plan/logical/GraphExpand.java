/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Logical plan for the {@code GRAPH EXPAND} command (snapshot/dev only).
 * Holds the clauses the user typed. The visited-set walk runs in
 * {@code EsqlSession} via {@code GraphExpandDriver}.
 * <p>
 * {@link #edgeRelation()} starts as an {@link UnresolvedRelation} and is
 * resolved to an {@link EsRelation} during analysis (same shape as FROM).
 */
public class GraphExpand extends UnaryPlan implements PostAnalysisVerificationAware, TelemetryAware, ExecutesOn.Coordinator {

    private static final Set<String> ALLOWED_OPTIONS = Set.of(
        "max_hops",
        "max_edges_per_node",
        "hub_degree",
        "max_nodes",
        "max_frontier",
        "direction",
        "exclude"
    );

    private static final Set<String> INTEGER_CAP_OPTIONS = Set.of(
        "max_hops",
        "max_edges_per_node",
        "hub_degree",
        "max_nodes",
        "max_frontier"
    );

    private static final Set<String> DIRECTION_VALUES = Set.of("in", "out", "both");

    /**
     * Edge side of the expand: an {@link UnresolvedRelation} (index pattern form) or a
     * subquery plan whose leaves are unresolved relations before analysis; an
     * {@link EsRelation} or a resolved subquery plan after. Not a plan child — resolution
     * happens in {@code Analyzer.ResolveRefs#resolveGraphExpand}.
     */
    private final LogicalPlan edgeRelation;
    private final Attribute seedColumn;
    private final Attribute matchField;
    private final List<Attribute> targetFields;
    private final Expression documentFilter;
    private final List<? extends NamedExpression> aggregates;
    private final List<Expression> groupings;
    private final Expression aggregateFilter;
    private final List<Order> sorts;
    private final Expression until;
    private final MapExpression options;
    /**
     * Output columns {@code node_from}, {@code node_to}, {@code node_reached},
     * {@code hop}, optional {@code relation} when {@code TO} lists more than one
     * field, optional STATS columns, and optional {@code dropped} when
     * {@code hub_degree} is set — built during analysis once the TO field type
     * is known.
     */
    private final List<Attribute> resultAttributes;

    public GraphExpand(
        Source source,
        LogicalPlan child,
        LogicalPlan edgeRelation,
        Attribute seedColumn,
        Attribute matchField,
        List<Attribute> targetFields,
        @Nullable Expression documentFilter,
        @Nullable List<? extends NamedExpression> aggregates,
        @Nullable List<Expression> groupings,
        @Nullable Expression aggregateFilter,
        @Nullable List<Order> sorts,
        @Nullable Expression until,
        @Nullable MapExpression options
    ) {
        this(
            source,
            child,
            edgeRelation,
            seedColumn,
            matchField,
            targetFields,
            documentFilter,
            aggregates,
            groupings,
            aggregateFilter,
            sorts,
            until,
            options,
            null
        );
    }

    public GraphExpand(
        Source source,
        LogicalPlan child,
        LogicalPlan edgeRelation,
        Attribute seedColumn,
        Attribute matchField,
        List<Attribute> targetFields,
        @Nullable Expression documentFilter,
        @Nullable List<? extends NamedExpression> aggregates,
        @Nullable List<Expression> groupings,
        @Nullable Expression aggregateFilter,
        @Nullable List<Order> sorts,
        @Nullable Expression until,
        @Nullable MapExpression options,
        @Nullable List<Attribute> resultAttributes
    ) {
        super(source, child);
        this.edgeRelation = edgeRelation;
        this.seedColumn = seedColumn;
        this.matchField = matchField;
        this.targetFields = targetFields;
        this.documentFilter = documentFilter;
        this.aggregates = aggregates;
        this.groupings = groupings;
        this.aggregateFilter = aggregateFilter;
        this.sorts = sorts;
        this.until = until;
        this.options = options;
        this.resultAttributes = resultAttributes;
    }

    /**
     * Index pattern written after {@code GRAPH EXPAND}, derived from
     * {@link #edgeRelation()}. For a subquery edge, the first relation the subquery reads,
     * or {@code null} when it reads none (a {@code ROW} subquery).
     */
    public IndexPattern indexPattern() {
        Holder<IndexPattern> first = new Holder<>();
        edgeRelation.forEachUp(p -> {
            if (first.get() != null) {
                return;
            }
            switch (p) {
                case UnresolvedRelation ur -> first.set(ur.indexPattern());
                case EsRelation er -> first.set(new IndexPattern(er.source(), er.indexPattern()));
                default -> {
                }
            }
        });
        return first.get();
    }

    public LogicalPlan edgeRelation() {
        return edgeRelation;
    }

    public Attribute seedColumn() {
        return seedColumn;
    }

    public Attribute matchField() {
        return matchField;
    }

    public List<Attribute> targetFields() {
        return targetFields;
    }

    public Expression documentFilter() {
        return documentFilter;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Expression aggregateFilter() {
        return aggregateFilter;
    }

    public List<Order> sorts() {
        return sorts;
    }

    public Expression until() {
        return until;
    }

    public MapExpression options() {
        return options;
    }

    public List<Attribute> resultAttributes() {
        return resultAttributes;
    }

    @Override
    public List<Attribute> output() {
        return resultAttributes != null ? resultAttributes : child().output();
    }

    /**
     * Only the seed column must come from the child. Match/TO fields live on the
     * edge {@link #edgeRelation()}, and {@link #resultAttributes()} are generated.
     */
    @Override
    protected AttributeSet computeReferences() {
        return seedColumn.references();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new GraphExpand(
            source(),
            newChild,
            edgeRelation,
            seedColumn,
            matchField,
            targetFields,
            documentFilter,
            aggregates,
            groupings,
            aggregateFilter,
            sorts,
            until,
            options,
            resultAttributes
        );
    }

    @Override
    public boolean expressionsResolved() {
        // Edge index + ON/TO + result attrs + optional STATS + both WHERE slots.
        // Aggregate WHERE without STATS never resolves (see postAnalysisVerification).
        // UNTIL InSubquery plans are analyzed inside the expression (not plan children).
        return edgeRelation.resolved()
            && seedColumn.resolved()
            && matchField.resolved()
            && targetFields.stream().allMatch(Attribute::resolved)
            && resultAttributes != null
            && (aggregates == null || Resolvables.resolved(aggregates))
            && (groupings == null || Resolvables.resolved(groupings))
            && (documentFilter == null || documentFilter.resolved())
            && (aggregateFilter == null || (aggregates != null && aggregateFilter.resolved()))
            && (sorts == null || Resolvables.resolved(sorts))
            && (until == null || (until.resolved() && untilSubqueryPlansResolved(until)));
    }

    /** {@link InSubquery#subquery()} is not an expression child — require it resolved too. */
    private static boolean untilSubqueryPlansResolved(Expression until) {
        Holder<Boolean> holder = new Holder<>(Boolean.TRUE);
        until.forEachDown(InSubquery.class, inSub -> {
            if (inSub.subquery().resolved() == false) {
                holder.set(Boolean.FALSE);
            }
        });
        until.forEachDown(MultiColumnInSubquery.class, mcs -> {
            if (mcs.subquery().resolved() == false) {
                holder.set(Boolean.FALSE);
            }
        });
        return holder.get();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            GraphExpand::new,
            child(),
            edgeRelation,
            seedColumn,
            matchField,
            targetFields,
            documentFilter,
            aggregates,
            groupings,
            aggregateFilter,
            sorts,
            until,
            options,
            resultAttributes
        );
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (aggregateFilter != null && aggregates == null) {
            failures.add(fail(this, "GRAPH EXPAND aggregate WHERE requires STATS"));
        }
        if (until != null) {
            until.forEachDown(e -> {
                if (e instanceof MultiColumnInSubquery) {
                    failures.add(fail(this, "GRAPH EXPAND UNTIL subquery form is not supported yet"));
                } else if (e instanceof InSubquery inSub && untilSubqueryIsCorrelated(inSub)) {
                    failures.add(fail(this, "GRAPH EXPAND UNTIL subquery form is not supported yet"));
                }
            });
        }
        postAnalysisOptionsVerification(failures);
    }

    /**
     * True when the UNTIL subquery references a column from the expand output or seed
     * input (correlated). The driver only supports an uncorrelated stop-set subquery.
     */
    private boolean untilSubqueryIsCorrelated(InSubquery inSub) {
        Set<String> outerNames = new HashSet<>();
        outerNames.add(seedColumn.name());
        for (Attribute a : child().output()) {
            outerNames.add(a.name());
        }
        if (resultAttributes != null) {
            for (Attribute a : resultAttributes) {
                outerNames.add(a.name());
            }
        }
        Holder<Boolean> correlated = new Holder<>(Boolean.FALSE);
        inSub.subquery().forEachExpressionDown(UnresolvedAttribute.class, ua -> {
            if (outerNames.contains(ua.name())) {
                correlated.set(Boolean.TRUE);
            }
        });
        return correlated.get();
    }

    private void postAnalysisOptionsVerification(Failures failures) {
        if (options == null) {
            return;
        }
        options.keyFoldedMap().forEach((key, value) -> {
            if (ALLOWED_OPTIONS.contains(key) == false) {
                failures.add(fail(this, "Invalid option [" + key + "] in [" + this.sourceText() + "]"));
                return;
            }
            if (key.equals("direction")) {
                verifyDirection(failures, value);
            } else if (key.equals("exclude")) {
                verifyExclude(failures, value);
            } else if (INTEGER_CAP_OPTIONS.contains(key)) {
                verifyIntegerCap(failures, key, value);
            }
        });
    }

    /**
     * Node ids the walk never admits: an edge that touches one is dropped before sorting,
     * caps and UNTIL, so nothing beyond an excluded node is visited and no budget is spent
     * on it. A string or a list of strings.
     */
    private void verifyExclude(Failures failures, Expression value) {
        if ((value instanceof Literal) == false) {
            failures.add(fail(this, "expected exclude to be a literal, got [" + value.sourceText() + "]"));
            return;
        }
        if (value.dataType() != DataType.KEYWORD && value.dataType() != DataType.TEXT) {
            failures.add(fail(this, "GRAPH EXPAND exclude must be a string or a list of strings, got [" + value.sourceText() + "]"));
        }
    }

    private void verifyDirection(Failures failures, Expression value) {
        if ((value instanceof Literal) == false) {
            failures.add(fail(this, "expected direction to be a literal, got [" + value.sourceText() + "]"));
            return;
        }
        Object folded = value.fold(FoldContext.small());
        if (folded == null) {
            failures.add(fail(this, "GRAPH EXPAND direction must be one of [in, out, both], got [null]"));
            return;
        }
        String direction = BytesRefs.toString(folded).toLowerCase(Locale.ROOT);
        if (DIRECTION_VALUES.contains(direction) == false) {
            failures.add(fail(this, "GRAPH EXPAND direction must be one of [in, out, both], got [" + value.sourceText() + "]"));
        }
    }

    private void verifyIntegerCap(Failures failures, String key, Expression value) {
        if ((value instanceof Literal) == false) {
            failures.add(fail(this, "expected " + key + " to be a literal, got [" + value.sourceText() + "]"));
            return;
        }
        if (value.dataType().isWholeNumber() == false) {
            failures.add(fail(this, "expected " + key + " to be an integer, got [" + value.sourceText() + "]"));
            return;
        }
        Number numericValue = (Number) value.fold(FoldContext.small());
        if (numericValue == null || numericValue.longValue() < 1) {
            failures.add(fail(this, "GRAPH EXPAND option [" + key + "] must be an integer >= 1, got [" + value.sourceText() + "]"));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        GraphExpand that = (GraphExpand) o;
        return Objects.equals(edgeRelation, that.edgeRelation)
            && Objects.equals(seedColumn, that.seedColumn)
            && Objects.equals(matchField, that.matchField)
            && Objects.equals(targetFields, that.targetFields)
            && Objects.equals(documentFilter, that.documentFilter)
            && Objects.equals(aggregates, that.aggregates)
            && Objects.equals(groupings, that.groupings)
            && Objects.equals(aggregateFilter, that.aggregateFilter)
            && Objects.equals(sorts, that.sorts)
            && Objects.equals(until, that.until)
            && Objects.equals(options, that.options)
            && Objects.equals(resultAttributes, that.resultAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            edgeRelation,
            seedColumn,
            matchField,
            targetFields,
            documentFilter,
            aggregates,
            groupings,
            aggregateFilter,
            sorts,
            until,
            options,
            resultAttributes
        );
    }
}
