/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
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
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
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
     * An aggregate keyed by a typed label container is keyed by the container whole: one value per row, the same for the same
     * label set, so the pack the translation puts under the aggregate and the unpack above it are gone.
     */
    public void testAggregateKeysByTheWholeContainer() {
        LogicalPlan plan = pushDown(regroupByPod());
        Aggregate pushed = regroupedByPod(plan);
        assertThat(pushed.groupings(), hasSize(1));
        assertThat(pushed.groupings().getFirst().dataType(), sameInstance(DataType.TIME_SERIES_METADATA_DATA_TYPE));
        assertThat(plan.collect(PackDims.class), empty());
        assertThat(plan.collect(UnpackDims.class), empty());
        assertThat(as(plan, Project.class).projections().getLast(), equalTo(pushed.groupings().getFirst()));
    }

    /** An aggregate keyed by the source's labels kept to {@code pod}, as translation builds it: the key packed, then unpacked. */
    private LogicalPlan regroupByPod() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Alias key = new Alias(
            SOURCE,
            MetadataAttribute.TIMESERIES,
            new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), false)
        );
        PackDims pack = new PackDims(
            SOURCE,
            new Eval(SOURCE, aggregate, List.of(key)),
            List.of(key.toAttribute()),
            PackDims.newPackedAttribute(SOURCE)
        );
        Alias packed = PackDims.newPackedGrouping(SOURCE, pack.packed());
        Alias sum = new Alias(SOURCE, "value", new Values(SOURCE, aggregate.aggregates().getFirst().toAttribute()));
        Aggregate regrouped = new Aggregate(SOURCE, pack, List.of(packed), List.of(sum, packed.toAttribute()));
        Attribute unpacked = new ReferenceAttribute(
            SOURCE,
            null,
            key.name(),
            DataType.TIME_SERIES_METADATA_DATA_TYPE,
            Nullability.TRUE,
            key.id(),
            false
        );
        return new Project(
            SOURCE,
            new UnpackDims(SOURCE, regrouped, packed.toAttribute(), List.of(unpacked)),
            List.of(sum.toAttribute(), unpacked)
        );
    }

    private static Aggregate regroupedByPod(LogicalPlan plan) {
        return plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).findFirst().get();
    }

    /** An aggregate, a join or a ranking outside the time-series aggregate keys by a label container only packed. */
    public void testUnpackedKeyFails() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Alias sum = new Alias(SOURCE, "value", new Values(SOURCE, aggregate.aggregates().getFirst().toAttribute()));
        LogicalPlan plan = new Aggregate(SOURCE, aggregate, List.of(labels.toAttribute()), List.of(sum, labels.toAttribute()));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> pushDown(plan));
        assertThat(e.getMessage(), containsString("[Aggregate] keys by the label container"));
    }

    /** A label container a user's time-series query computes is theirs: nothing is split. */
    public void testUserContainerIsKept() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, TimeSeriesAggregate.Origin.TS_COMMAND);
        Alias read = new Alias(SOURCE, "pod", new TimeSeriesMetadataPath(SOURCE, labels.toAttribute(), Literal.keyword(SOURCE, "pod")));
        LogicalPlan plan = new Eval(SOURCE, aggregate, List.of(read));

        assertThat(pushDown(plan), sameInstance(plan));
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

    /** The configuration of a query translated over a label container, see {@link QueryPragmas#ENABLE_PROMQL_TRANSLATION_V2}. */
    private static final Configuration V2 = EsqlTestUtils.configuration(
        new QueryPragmas(Settings.builder().put(QueryPragmas.ENABLE_PROMQL_TRANSLATION_V2.getKey(), true).build())
    );

    /** The rule over {@code plan}, translated over a label container. */
    private static LogicalPlan pushDown(LogicalPlan plan) {
        return new ScalarizeTimeSeriesMetadataPaths().apply(
            plan,
            analyzer().configuration(V2).minimumTransportVersion(TransportVersion.current()).buildContext()
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
