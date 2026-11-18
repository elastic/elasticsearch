/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.AbstractTimeSeriesMetadataFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Splitting the label container of a translated PromQL plan, on plans built the way translation builds them: a time-series
 * aggregate keyed by the source's labels, and operations over its container above it.
 */
public class ScalarizeTimeSeriesMetadataPathsTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    private final FieldAttribute pod = dimension("pod");
    private final FieldAttribute value = new FieldAttribute(SOURCE, "value", new EsField("value", DataType.DOUBLE, Map.of(), true, null));
    private final EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(pod, value));

    /**
     * A label read by name above the time-series aggregate is one more key of it. The aggregate returns what it groups by, so the
     * container stays its key, whole, though nothing above uses it.
     */
    public void testLabelReadByNameIsAKeyNextToTheContainer() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Alias read = new Alias(SOURCE, "pod", new TimeSeriesMetadataPath(SOURCE, labels.toAttribute(), Literal.keyword(SOURCE, "pod")));
        LogicalPlan plan = pushDown(new Eval(SOURCE, aggregate, List.of(read)));

        TimeSeriesAggregate pushed = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertEquals(List.of(MetadataAttribute.TIMESERIES, "pod"), names(pushed.groupings()));
        Expression container = Alias.unwrap(pushed.groupings().getFirst());
        assertThat(as(container, TimeSeriesWithout.class).children(), empty());
        assertThat(container.dataType(), sameInstance(DataType.TIME_SERIES_METADATA_DATA_TYPE));
        assertEquals(pod, Alias.unwrap(pushed.groupings().getLast()));
        assertThat(expressions(plan, AbstractTimeSeriesMetadataFunction.class), empty());
        Eval eval = as(plan, Eval.class);
        assertThat(Alias.unwrap(eval.fields().getFirst()), instanceOf(Attribute.class));
    }

    /** A container rendered without a label is loaded without it. */
    public void testRenderedContainerIsLoadedWithoutWhatItLeavesOut() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Expression without = new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), true);
        Alias rendered = new Alias(SOURCE, MetadataAttribute.TIMESERIES, new ToString(SOURCE, without, EsqlTestUtils.TEST_CFG));
        LogicalPlan plan = pushDown(new Eval(SOURCE, aggregate, List.of(rendered)));

        List<Expression> loads = expressions(plan, TimeSeriesWithout.class);
        assertThat(loads, hasSize(1));
        assertEquals(List.of(pod), loads.getFirst().children());
        assertThat(expressions(plan, AbstractTimeSeriesMetadataFunction.class), empty());
    }

    /** A label the source has no field for is in no series: the load leaves out no field for it. */
    public void testLabelWithoutFieldIsNotLeftOutOfTheLoad() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Expression without = new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "__name__")), true);
        Alias rendered = new Alias(SOURCE, MetadataAttribute.TIMESERIES, new ToString(SOURCE, without, EsqlTestUtils.TEST_CFG));
        LogicalPlan plan = pushDown(new Eval(SOURCE, aggregate, List.of(rendered)));

        List<Expression> loads = expressions(plan, TimeSeriesWithout.class);
        assertThat(loads, hasSize(1));
        assertThat(loads.getFirst().children(), empty());
    }

    /**
     * An aggregate keyed by a typed label container keys by the container whole: one value per row, the same for the same label
     * set, so nothing is split or packed and a label of several values keys as a whole.
     */
    public void testAggregateKeysByTheWholeContainer() {
        Aggregate pushed = regroupedByPod(pushDown(regroupByPod()));
        assertThat(pushed.groupings(), hasSize(1));
        assertThat(pushed.groupings().getFirst().dataType(), sameInstance(DataType.TIME_SERIES_METADATA_DATA_TYPE));
        assertThat(pushed.collect(PackDims.class), empty());
    }

    /** A keyword container cannot be written to: the aggregate keys by its labels instead, packed into one key. */
    public void testKeywordAggregateKeysByPackedLabels() {
        LogicalPlan plan = pushDownKeyword(regroupByPod());
        Aggregate pushed = regroupedByPod(plan);
        assertThat(pushed.child(), instanceOf(PackDims.class));
        assertEquals(List.of("pod"), ((PackDims) pushed.child()).dims().stream().map(Attribute::name).toList());
        assertThat(expressions(plan, AbstractTimeSeriesMetadataFunction.class), empty());
    }

    private Aggregate regroupByPod() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Alias key = new Alias(
            SOURCE,
            MetadataAttribute.TIMESERIES,
            new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), false)
        );
        Alias sum = new Alias(SOURCE, "value", new Values(SOURCE, aggregate.aggregates().getFirst().toAttribute()));
        return new Aggregate(SOURCE, aggregate, List.of(key), List.of(sum, key.toAttribute()));
    }

    private static Aggregate regroupedByPod(LogicalPlan plan) {
        return plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).findFirst().get();
    }

    /**
     * A join compares a side without a container to a side with one. The typed side writes its labels into an empty container;
     * a keyword container cannot be written to and holds every label it has, so the side compares as absent and never matches.
     */
    public void testKeywordJoinComparesASideWithoutContainerAsAbsent() {
        Alias leftLabels = sourceLabels();
        Alias kept = new Alias(
            SOURCE,
            MetadataAttribute.TIMESERIES,
            new TimeSeriesMetadataFilter(SOURCE, leftLabels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), false)
        );
        LogicalPlan left = new Eval(SOURCE, timeSeriesAggregate(leftLabels, TimeSeriesAggregate.Origin.PROMQL_COMMAND), List.of(kept));
        Alias rightLabels = sourceLabels();
        LogicalPlan right = timeSeriesAggregate(rightLabels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        LogicalPlan plan = new HashJoin(
            SOURCE,
            left,
            right,
            List.of(kept.toAttribute()),
            List.of(rightLabels.toAttribute()),
            List.of(),
            false
        );

        HashJoin keyword = as(pushDownKeyword(plan), HashJoin.class);
        assertThat(expressions(keyword, AbstractTimeSeriesMetadataFunction.class), empty());
        Eval compared = as(as(keyword.left(), PackDims.class).child(), Eval.class);
        Literal absent = as(compared.fields().getFirst().child(), Literal.class);
        assertNull(absent.value());
        assertThat(absent.dataType(), sameInstance(DataType.KEYWORD));
        assertThat(expressions(pushDown(plan), TimeSeriesMetadataSet.class), hasSize(1));
    }

    /**
     * A union of a branch with a container and a branch without one returns the labels of each whole. A branch without one
     * returns the empty container where it is typed, and absent where it is keyword.
     */
    public void testKeywordUnionReturnsABranchWithoutContainerAsAbsent() {
        Alias firstLabels = sourceLabels();
        TimeSeriesAggregate first = timeSeriesAggregate(firstLabels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Alias secondLabels = sourceLabels();
        TimeSeriesAggregate secondAggregate = timeSeriesAggregate(secondLabels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Alias none = new Alias(SOURCE, MetadataAttribute.TIMESERIES, TranslationContextV2.emptyLabels(SOURCE));
        LogicalPlan second = new Project(
            SOURCE,
            new Eval(SOURCE, secondAggregate, List.of(none)),
            List.of(secondAggregate.output().getFirst(), none.toAttribute())
        );
        LogicalPlan plan = new UnionAll(SOURCE, List.of(first, second), first.output());

        Literal keyword = as(emptyBranchContainer(pushDownKeyword(plan)), Literal.class);
        assertNull(keyword.value());
        assertThat(keyword.dataType(), sameInstance(DataType.KEYWORD));
        assertThat(emptyBranchContainer(pushDown(plan)).dataType(), sameInstance(DataType.TIME_SERIES_METADATA_DATA_TYPE));
    }

    /** What the last branch of the union {@code rewritten} returns as its label container: what the union returns too. */
    private static Expression emptyBranchContainer(LogicalPlan rewritten) {
        UnionAll union = as(as(rewritten, Project.class).child(), UnionAll.class);
        assertThat(expressions(union, AbstractTimeSeriesMetadataFunction.class), empty());
        Attribute container = union.output().stream().filter(a -> a.name().equals(MetadataAttribute.TIMESERIES)).findFirst().get();
        Eval branch = as(as(union.children().getLast(), Project.class).child(), Eval.class);
        Expression returned = branch.fields().getFirst().child();
        assertThat(container.dataType(), sameInstance(returned.dataType()));
        return returned;
    }

    /** A label container a user's time-series query computes is theirs: nothing is split. */
    public void testUserContainerIsKept() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.TS_COMMAND);
        Alias read = new Alias(SOURCE, "pod", new TimeSeriesMetadataPath(SOURCE, labels.toAttribute(), Literal.keyword(SOURCE, "pod")));
        LogicalPlan plan = new Eval(SOURCE, aggregate, List.of(read));

        assertThat(pushDown(plan), sameInstance(plan));
    }

    /** Where a node before 9.7 takes part, what is left of the container is the source's keyword _timeseries, rendered as it is. */
    public void testKeywordContainerIsTheSourceField() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Expression without = new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), true);
        Alias rendered = new Alias(SOURCE, MetadataAttribute.TIMESERIES, new ToString(SOURCE, without, EsqlTestUtils.TEST_CFG));
        LogicalPlan plan = pushDownKeyword(new Eval(SOURCE, aggregate, List.of(rendered)));

        assertThat(expressions(plan, TimeSeriesWithout.class), hasSize(1));
        assertThat(expressions(plan, TimeSeriesWithout.class).getFirst().dataType(), sameInstance(DataType.KEYWORD));
        assertThat(expressions(plan, ToString.class), empty());
        assertThat(as(plan, Eval.class).fields().getFirst().dataType(), sameInstance(DataType.KEYWORD));
    }

    /**
     * A use that leaves out more than every other use keeps its TS_UNSET over the loaded container. A keyword container cannot
     * change after it is loaded, so that use reads it as it is, as the keyword translation did.
     */
    public void testKeywordUseThatLeavesOutMoreReadsTheLoadedContainer() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Source withoutPod = new Source(1, 1, "without (pod)");
        Expression without = new TimeSeriesMetadataFilter(withoutPod, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), true);
        Alias narrow = new Alias(SOURCE, "narrow", new ToString(SOURCE, without, EsqlTestUtils.TEST_CFG));
        Alias whole = new Alias(SOURCE, "whole", new ToString(SOURCE, labels.toAttribute(), EsqlTestUtils.TEST_CFG));
        LogicalPlan plan = new Eval(SOURCE, aggregate, List.of(narrow, whole));

        Eval keyword = as(pushDownKeyword(plan), Eval.class);
        assertThat(expressions(keyword, TimeSeriesMetadataFilter.class), empty());
        assertThat(keyword.fields().get(0).child(), equalTo(keyword.fields().get(1).child()));
        assertThat(expressions(pushDown(plan), TimeSeriesMetadataFilter.class), hasSize(1));
    }

    /** A label read by name is needed off its container, and nothing uses the container as a whole. */
    public void testDemandOfLabelReadByName() {
        Alias labels = sourceLabels();
        Alias read = new Alias(SOURCE, "pod", path(labels.toAttribute(), "pod"));
        var demand = demand(labels, read);

        assertEquals(Set.of("pod"), demand.need(labels.id()));
        assertNull(demand.tolerated(labels.id()));
    }

    /** A label read through a TS_UNSET that leaves it out is null: nothing is needed off the container. */
    public void testDemandOfLabelReadThroughItsUnset() {
        Alias labels = sourceLabels();
        Alias read = new Alias(SOURCE, "pod", path(unset(labels.toAttribute(), "pod"), "pod"));

        assertEquals(Set.of(), demand(labels, read).need(labels.id()));
    }

    /** A use of the container as a whole tolerates what it leaves out. */
    public void testDemandOfWholeUse() {
        Alias labels = sourceLabels();
        Alias rendered = new Alias(SOURCE, "tags", new ToString(SOURCE, unset(labels.toAttribute(), "pod"), EsqlTestUtils.TEST_CFG));
        var demand = demand(labels, rendered);

        assertEquals(Set.of("pod"), demand.tolerated(labels.id()));
        assertEquals(Set.of(), demand.need(labels.id()));
    }

    /** Two uses tolerate only what both leave out. */
    public void testDemandOfTwoWholeUses() {
        Alias labels = sourceLabels();
        Alias narrow = new Alias(SOURCE, "narrow", new ToString(SOURCE, unset(labels.toAttribute(), "pod"), EsqlTestUtils.TEST_CFG));
        Alias whole = new Alias(SOURCE, "whole", new ToString(SOURCE, labels.toAttribute(), EsqlTestUtils.TEST_CFG));

        assertEquals(Set.of(), demand(labels, narrow, whole).tolerated(labels.id()));
    }

    /** TS_KEEP reads the labels it names, by name: nothing uses its container as a whole. */
    public void testDemandOfKeep() {
        Alias labels = sourceLabels();
        Alias kept = new Alias(
            SOURCE,
            "tags",
            new ToString(
                SOURCE,
                new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), false),
                EsqlTestUtils.TEST_CFG
            )
        );
        var demand = demand(labels, kept);

        assertEquals(Set.of("pod"), demand.need(labels.id()));
        assertNull(demand.tolerated(labels.id()));
    }

    /** TS_SET tolerates the label it writes: the container under it may leave the label out. */
    public void testDemandOfSet() {
        Alias labels = sourceLabels();
        Alias written = new Alias(
            SOURCE,
            MetadataAttribute.TIMESERIES,
            new TimeSeriesMetadataSet(SOURCE, labels.toAttribute(), Literal.keyword(SOURCE, "pod"), Literal.keyword(SOURCE, "p"))
        );
        Alias rendered = new Alias(SOURCE, "tags", new ToString(SOURCE, written.toAttribute(), EsqlTestUtils.TEST_CFG));

        assertEquals(Set.of("pod"), demand(labels, written, rendered).tolerated(labels.id()));
    }

    /** A label read by name off a container that no node of the plan defines cannot be replaced: the plan is rejected. */
    public void testLabelReadThatCannotBeReplacedFails() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Attribute other = new ReferenceAttribute(SOURCE, null, "other", DataType.TIME_SERIES_METADATA_DATA_TYPE);
        Alias read = new Alias(SOURCE, "pod", path(other, "pod"));
        LogicalPlan plan = new Eval(SOURCE, aggregate, List.of(read));

        expectThrows(IllegalStateException.class, () -> pushDown(plan));
    }

    /** A keyword container cannot change after it is loaded: a label written into a container that is then rendered fails. */
    public void testKeywordContainerCannotBeWritten() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Expression written = new TimeSeriesMetadataSet(
            SOURCE,
            labels.toAttribute(),
            Literal.keyword(SOURCE, "pod"),
            Literal.keyword(SOURCE, "p")
        );
        Alias rendered = new Alias(SOURCE, "tags", new ToString(SOURCE, written, EsqlTestUtils.TEST_CFG));
        LogicalPlan plan = new Eval(SOURCE, aggregate, List.of(rendered));

        expectThrows(VerificationException.class, () -> pushDownKeyword(plan));
        assertThat(expressions(pushDown(plan), TimeSeriesMetadataSet.class), hasSize(1));
    }

    /** The rule where every node reads the typed label container: it is under construction, so a release build has none. */
    private static LogicalPlan pushDown(LogicalPlan plan) {
        assumeTrue("_timeseries is under construction", EsqlCapabilities.Cap.ENABLE_TSMD_DATA_TYPE.isEnabled());
        return new ScalarizeTimeSeriesMetadataPaths().apply(
            plan,
            analyzer().minimumTransportVersion(TransportVersion.current()).buildContext()
        );
    }

    /** The rule where a node before 9.7 takes part: the label container is the keyword {@code _timeseries}. */
    private static LogicalPlan pushDownKeyword(LogicalPlan plan) {
        return new ScalarizeTimeSeriesMetadataPaths().apply(
            plan,
            analyzer().minimumTransportVersion(TimeSeriesCollapse.TS_COLLAPSE).buildContext()
        );
    }

    /** Step 1 of the rule alone, over the given fields evaluated above a time-series aggregate keyed by {@code labels}. */
    private ScalarizeTimeSeriesMetadataPaths.Run demand(Alias labels, Alias... fields) {
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        var run = new ScalarizeTimeSeriesMetadataPaths.Run() {
            @Override
            DataType containerType() {
                return DataType.TIME_SERIES_METADATA_DATA_TYPE;
            }

            @Override
            boolean writesContainers() {
                return true;
            }

            @Override
            Expression empty(Source at) {
                return TranslationContextV2.emptyLabels(at);
            }

            @Override
            Expression render(Source at, Expression whole) {
                return new ToString(at, whole, EsqlTestUtils.TEST_CFG);
            }

            /** A typed container compares whole: the promoted labels it does not hold written back. */
            @Override
            Expression compared(Parts parts, Source at) {
                return whole(parts, at);
            }

            @Override
            boolean keysWhole() {
                return true;
            }

            @Override
            void verify(LogicalPlan plan) {
                // what is left is a typed container every node reads
            }
        };
        run.demand(new Eval(SOURCE, aggregate, List.of(fields)));
        return run;
    }

    private static Expression path(Expression container, String label) {
        return new TimeSeriesMetadataPath(SOURCE, container, Literal.keyword(SOURCE, label));
    }

    private static Expression unset(Expression container, String label) {
        return new TimeSeriesMetadataFilter(SOURCE, container, List.of(Literal.keyword(SOURCE, label)), true);
    }

    private static Alias sourceLabels() {
        return new Alias(SOURCE, MetadataAttribute.TIMESERIES, TranslationContextV2.sourceLabels(SOURCE));
    }

    private TimeSeriesAggregate timeSeriesAggregate(Alias labels, TimeSeriesAggregate.Origin origin) {
        Alias last = new Alias(SOURCE, "value", new Values(SOURCE, value));
        return new TimeSeriesAggregate(SOURCE, relation, List.of(labels), List.of(last, labels.toAttribute()), null, null, origin, false);
    }

    private static FieldAttribute dimension(String name) {
        return new FieldAttribute(SOURCE, name, new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION));
    }

    private static List<Expression> expressions(LogicalPlan plan, Class<? extends Expression> type) {
        return plan.collect(node -> true)
            .stream()
            .flatMap(node -> node.expressions().stream())
            .flatMap(expression -> expression.collect(type::isInstance).stream())
            .toList();
    }

    private static List<String> names(List<? extends Expression> expressions) {
        return expressions.stream().map(e -> e instanceof NamedExpression named ? named.name() : e.toString()).toList();
    }
}
