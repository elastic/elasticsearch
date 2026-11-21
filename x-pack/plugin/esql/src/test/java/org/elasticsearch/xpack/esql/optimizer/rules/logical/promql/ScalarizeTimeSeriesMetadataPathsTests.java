/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Reads of a label out of the keyword label container, on plans built the way the PromQL translation builds them: a
 * time-series aggregate keyed by the source's labels, and container functions above it. Every case goes through
 * {@link #scalarize}, which checks what holds for every plan: the output, the keys and the packs stay, and the rule is
 * idempotent.
 */
public class ScalarizeTimeSeriesMetadataPathsTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    private final FieldAttribute cluster = dimension("cluster");
    private final FieldAttribute pod = dimension("pod");
    private final FieldAttribute shard = new FieldAttribute(
        SOURCE,
        "shard",
        new EsField("shard", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION)
    );
    private final FieldAttribute event = new FieldAttribute(SOURCE, "event", new EsField("event", DataType.KEYWORD, Map.of(), true, null));
    private final FieldAttribute value = new FieldAttribute(SOURCE, "value", new EsField("value", DataType.DOUBLE, Map.of(), true, null));

    /** A read of a dimension is the dimension field, one more key of the time-series aggregate next to the container. */
    public void testReadOfADimensionIsTheField() {
        Alias labels = labels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels);
        LogicalPlan plan = scalarize(new Eval(SOURCE, aggregate, List.of(read("pod", labels.toAttribute(), "pod"))));

        TimeSeriesAggregate scalarized = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(scalarized.groupings(), hasItem(pod));
        assertThat(scalarized.output(), hasItem(pod));
        assertThat(reads(plan), empty());
        assertEquals(pod, valueOf(plan, "pod"));
    }

    /** A label the container leaves out, or one no field holds, is in no series. */
    public void testReadOfALabelNotInTheContainerIsNull() {
        Alias labels = new Alias(SOURCE, MetadataAttribute.TIMESERIES, new TimeSeriesWithout(SOURCE, List.of(pod)));
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels);
        LogicalPlan plan = scalarize(
            new Eval(SOURCE, aggregate, List.of(read("pod", labels.toAttribute(), "pod"), read("job", labels.toAttribute(), "job")))
        );

        assertThat(reads(plan), empty());
        assertNullLiteral(valueOf(plan, "pod"));
        assertNullLiteral(valueOf(plan, "job"));
        assertThat(plan.collect(TimeSeriesAggregate.class).getFirst().groupings(), hasSize(1));
    }

    /**
     * A field that isn't a keyword dimension may hold what the container doesn't, or hold it in another form: the read
     * stays.
     */
    public void testReadOfAFieldThatIsNotAKeywordDimensionStays() {
        Alias labels = labels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels);
        LogicalPlan plan = new Eval(
            SOURCE,
            aggregate,
            List.of(read("shard", labels.toAttribute(), "shard"), read("event", labels.toAttribute(), "event"))
        );
        assertThat(scalarize(plan), sameInstance(plan));
    }

    /** A written label reads as what was written, a label left out as null, any other label as before the writes. */
    public void testReadThroughWritesAndFilters() {
        Alias labels = labels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, cluster);
        Alias rewritten = new Alias(
            SOURCE,
            "rewritten",
            new TimeSeriesMetadataSet(
                SOURCE,
                new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "cluster")), true),
                Literal.keyword(SOURCE, "region"),
                cluster
            )
        );
        Eval writes = new Eval(SOURCE, aggregate, List.of(rewritten));
        LogicalPlan plan = scalarize(
            new Eval(
                SOURCE,
                writes,
                List.of(
                    read("region", rewritten.toAttribute(), "region"),
                    read("cluster", rewritten.toAttribute(), "cluster"),
                    read("pod", rewritten.toAttribute(), "pod")
                )
            )
        );

        assertThat(reads(plan), empty());
        assertEquals(cluster, valueOf(plan, "region"));
        assertNullLiteral(valueOf(plan, "cluster"));
        assertEquals(pod, valueOf(plan, "pod"));
    }

    /**
     * A label copied from another label reads as that label, in one run: the copy and a read of it are both replaced, as
     * {@code group_left} copies a label of the one side into the result labels that a read above takes.
     */
    public void testReadOfACopiedLabel() {
        Alias labels = labels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels);
        Alias copied = new Alias(
            SOURCE,
            "copied",
            new TimeSeriesMetadataSet(SOURCE, labels.toAttribute(), Literal.keyword(SOURCE, "copy"), path(labels.toAttribute(), "pod"))
        );
        Eval copies = new Eval(SOURCE, aggregate, List.of(copied));
        LogicalPlan plan = scalarize(new Eval(SOURCE, copies, List.of(read("copy", copied.toAttribute(), "copy"))));

        assertThat(reads(plan), empty());
        assertEquals(pod, valueOf(plan, "copy"));
    }

    /** A filter keeping labels keeps their reads; any other label is left out. */
    public void testReadThroughAKeepFilter() {
        Alias labels = labels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels);
        Expression kept = new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), false);
        LogicalPlan plan = scalarize(new Eval(SOURCE, aggregate, List.of(read("pod", kept, "pod"), read("cluster", kept, "cluster"))));

        assertThat(reads(plan), empty());
        assertEquals(pod, valueOf(plan, "pod"));
        assertNullLiteral(valueOf(plan, "cluster"));
    }

    /**
     * A label named by anything but a literal may be any label, so a read through it, and a write of several values, which
     * may hold an empty one the write leaves out, stay.
     */
    public void testReadThroughWhatIsNotKnownStays() {
        Alias labels = labels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels, cluster);
        Expression filtered = new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(cluster), true);
        Expression written = new TimeSeriesMetadataSet(SOURCE, labels.toAttribute(), cluster, Literal.keyword(SOURCE, "a"));
        Expression values = new TimeSeriesMetadataSet(
            SOURCE,
            labels.toAttribute(),
            Literal.keyword(SOURCE, "pod"),
            new Literal(SOURCE, List.of(new BytesRef("a"), new BytesRef("")), DataType.KEYWORD)
        );
        LogicalPlan plan = new Eval(
            SOURCE,
            aggregate,
            List.of(read("filtered", filtered, "pod"), read("written", written, "pod"), read("values", values, "pod"))
        );
        assertThat(scalarize(plan), sameInstance(plan));
    }

    /** A value read below a projection is projected too, so the read above it finds it. */
    public void testReadAboveAProjection() {
        Alias labels = labels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels);
        Attribute last = aggregate.output().getFirst();
        Project project = new Project(SOURCE, aggregate, List.of(last, labels.toAttribute()));
        Filter filter = new Filter(SOURCE, project, new IsNotNull(SOURCE, last));
        LogicalPlan plan = scalarize(new Eval(SOURCE, filter, List.of(read("pod", labels.toAttribute(), "pod"))));

        assertThat(reads(plan), empty());
        assertEquals(pod, valueOf(plan, "pod"));
        Eval top = as(as(plan, Project.class).child(), Eval.class);
        Attribute carried = as(top.fields().getFirst().child(), Attribute.class);
        assertThat(plan.collect(Project.class).getLast().output(), hasItem(carried));
    }

    /**
     * Each side of a join reads its own labels. The two reads of {@code pod} are two values, which the join keeps apart
     * though it merges its output by name.
     */
    public void testReadsOnBothSidesOfAJoin() {
        Alias leftLabels = labels();
        Alias leftContainer = new Alias(SOURCE, "$left", leftLabels.toAttribute());
        Eval left = new Eval(SOURCE, timeSeriesAggregate(leftLabels), List.of(leftContainer));
        Alias rightLabels = labels();
        Alias rightContainer = new Alias(SOURCE, "$right", rightLabels.toAttribute());
        Eval right = new Eval(SOURCE, timeSeriesAggregate(rightLabels), List.of(rightContainer));
        Attribute leftValue = left.output().getFirst();
        Attribute rightValue = right.output().getFirst();
        HashJoin join = new HashJoin(SOURCE, left, right, List.of(leftValue), List.of(rightValue), List.of(rightValue), false);
        LogicalPlan plan = scalarize(
            new Eval(
                SOURCE,
                join,
                List.of(read("left_pod", leftContainer.toAttribute(), "pod"), read("right_pod", rightContainer.toAttribute(), "pod"))
            )
        );

        assertThat(reads(plan), empty());
        Attribute leftPod = as(valueOf(plan, "left_pod"), Attribute.class);
        Attribute rightPod = as(valueOf(plan, "right_pod"), Attribute.class);
        assertEquals(pod, definitionOrSelf(plan, leftPod));
        assertEquals(pod, definitionOrSelf(plan, rightPod));
        HashJoin scalarized = plan.collect(HashJoin.class).getFirst();
        Eval above = as(plan instanceof Project project ? project.child() : plan, Eval.class);
        List<Attribute> carried = above.fields().stream().map(f -> as(f.child(), Attribute.class)).toList();
        assertThat(carried, hasSize(2));
        assertNotEquals(carried.get(0).name(), carried.get(1).name());
        assertTrue(scalarized.output().containsAll(carried));
        assertTrue(scalarized.left().outputSet().contains(carried.get(0)));
        // a join executes only what it adds of its right side
        assertThat(scalarized.addedFields(), hasItem(carried.get(1)));
    }

    /**
     * An aggregate keyed by a container returns the container, not the series' fields: a read above it stays, since a key
     * of several values would split its rows.
     */
    public void testReadAboveAnAggregateStays() {
        Alias labels = labels();
        TimeSeriesAggregate aggregate = timeSeriesAggregate(labels);
        Attribute last = aggregate.output().getFirst();
        Alias sum = new Alias(SOURCE, "sum", new Values(SOURCE, last));
        Aggregate regroup = new Aggregate(SOURCE, aggregate, List.of(labels.toAttribute()), List.of(sum, labels.toAttribute()));
        LogicalPlan plan = new Eval(SOURCE, regroup, List.of(read("pod", labels.toAttribute(), "pod")));
        assertThat(scalarize(plan), sameInstance(plan));
    }

    /** A plan reading no label is returned as it is. */
    public void testPlanWithoutReads() {
        Alias labels = labels();
        LogicalPlan plan = timeSeriesAggregate(labels);
        assertThat(scalarize(plan), sameInstance(plan));
    }

    /**
     * Applies the rule and checks what holds for every plan: the output stays, keys and packs stay, a time-series
     * aggregate gains only dimension fields as keys, and a second run changes nothing.
     */
    private static LogicalPlan scalarize(LogicalPlan plan) {
        LogicalPlan scalarized = new ScalarizeTimeSeriesMetadataPaths().apply(plan);
        assertEquals(plan.output(), scalarized.output());
        assertEquals(plan.collect(PackDims.class).size(), scalarized.collect(PackDims.class).size());
        assertEquals(plan.collect(UnpackDims.class).size(), scalarized.collect(UnpackDims.class).size());
        List<TimeSeriesAggregate> before = plan.collect(TimeSeriesAggregate.class);
        List<TimeSeriesAggregate> after = scalarized.collect(TimeSeriesAggregate.class);
        assertEquals(before.size(), after.size());
        for (int i = 0; i < before.size(); i++) {
            List<Expression> added = new ArrayList<>(after.get(i).groupings());
            assertTrue(added.containsAll(before.get(i).groupings()));
            added.removeAll(before.get(i).groupings());
            assertThat(added, everyItem(instanceOf(FieldAttribute.class)));
            assertTrue(added.stream().allMatch(g -> ((FieldAttribute) g).isDimension()));
        }
        assertEquals(scalarized, new ScalarizeTimeSeriesMetadataPaths().apply(scalarized));
        return scalarized;
    }

    private static Alias read(String name, Expression container, String label) {
        return new Alias(SOURCE, name, path(container, label));
    }

    private static Expression path(Expression container, String label) {
        return new TimeSeriesMetadataPath(SOURCE, container, Literal.keyword(SOURCE, label));
    }

    /** What the alias named {@code name} in the plan's top {@link Eval} evaluates to, looked through temporary names. */
    private static Expression valueOf(LogicalPlan plan, String name) {
        Eval eval = as(plan instanceof Project project ? project.child() : plan, Eval.class);
        Expression value = eval.fields().stream().filter(f -> f.name().equals(name)).findFirst().orElseThrow().child();
        while (value instanceof Attribute attribute) {
            Expression defined = definition(plan, attribute);
            if (defined == null) {
                return attribute;
            }
            value = defined;
        }
        return value;
    }

    private static Expression definitionOrSelf(LogicalPlan plan, Attribute attribute) {
        Expression defined = definition(plan, attribute);
        return defined == null ? attribute : defined;
    }

    /** The expression an alias of {@code plan} gives {@code attribute}, or null when no alias defines it. */
    private static Expression definition(LogicalPlan plan, Attribute attribute) {
        for (LogicalPlan node : plan.collect(n -> true)) {
            for (Expression e : node.expressions()) {
                if (e instanceof Alias alias && alias.id().equals(attribute.id())) {
                    return alias.child();
                }
            }
        }
        return null;
    }

    private static void assertNullLiteral(Expression expression) {
        Literal literal = as(expression, Literal.class);
        assertNull(literal.value());
        assertEquals(DataType.KEYWORD, literal.dataType());
    }

    private static List<Expression> reads(LogicalPlan plan) {
        return plan.collect(node -> true)
            .stream()
            .flatMap(node -> node.expressions().stream())
            .flatMap(expression -> expression.collect(TimeSeriesMetadataPath.class::isInstance).stream())
            .toList();
    }

    private static Alias labels() {
        return new Alias(SOURCE, MetadataAttribute.TIMESERIES, new TimeSeriesWithout(SOURCE));
    }

    /** A time-series aggregate keyed by {@code labels} and {@code keys}, returning the last value, then its keys. */
    private TimeSeriesAggregate timeSeriesAggregate(Alias labels, FieldAttribute... keys) {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(cluster, pod, shard, event, value));
        Alias last = new Alias(SOURCE, "value", new Values(SOURCE, value));
        List<Expression> groupings = new ArrayList<>(List.of(labels));
        List<NamedExpression> aggregates = new ArrayList<>(List.of(last, labels.toAttribute()));
        groupings.addAll(List.of(keys));
        aggregates.addAll(List.of(keys));
        return new TimeSeriesAggregate(
            SOURCE,
            relation,
            groupings,
            aggregates,
            null,
            null,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND,
            false
        );
    }

    private static FieldAttribute dimension(String name) {
        return new FieldAttribute(SOURCE, name, new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION));
    }
}
