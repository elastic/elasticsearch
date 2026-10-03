/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.exclude;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.intersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.project;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.promoted;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.rest;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.subtract;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class TranslationContextTests extends ESTestCase {

    public void testUnionMergesLabelsAndSkipSets() {
        TranslationConstraint header = union(
            union(union(promoted(List.of("cluster")), rest(Set.of("pod"))), rest(Set.of("pod"))),
            promoted(List.of("cluster", "region"))
        );

        assertThat(header.labels(), contains("cluster", "region"));
        assertThat(header.skips(), contains(Set.of("pod")));
        assertThat(union(header, TranslationConstraint.EMPTY), equalTo(header));
    }

    public void testSubtractDropsLabelsAndWidensSkipSets() {
        TranslationConstraint above = union(promoted(List.of("cluster", "pod")), rest(Set.of("region")));

        TranslationConstraint below = subtract(above, List.of("pod"));
        assertThat(below.labels(), contains("cluster"));
        assertThat(below.skips(), contains(Set.of("region", "pod")));

        // the regroup's own column composes as a second, finer skip set
        TranslationConstraint child = union(below, rest(Set.of("pod")));
        assertThat(child.skips(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
    }

    public void testIntersectIsTheUpwardCounterpartOfSubtract() {
        TranslationConstraint required = union(promoted(List.of("cluster", "pod")), rest(Set.of("region")));
        TranslationConstraint child = union(subtract(required, List.of("pod")), rest(Set.of("pod")));

        TranslationConstraint lifted = intersect(child, List.of("pod"));

        // every column the parent required, apart from the dropped label, comes back; so does the regroup's own
        // _timeseries column, which already excludes the dropped label and fixes the grain of the result
        assertThat(lifted.labels(), contains("cluster"));
        assertThat(lifted.skips(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
        // the regroup's own full label space does not survive dropping a label it still carries
        assertFalse(intersect(rest(), List.of("pod")).hasRest());
        // without () keeps everything
        assertThat(intersect(child, List.of()), equalTo(child));
    }

    public void testProjectKeepsSkipSets() {
        TranslationConstraint header = union(promoted(List.of("cluster", "pod", "region")), rest(Set.of("pod")));

        TranslationConstraint retained = project(header, List.of("cluster", "missing"));

        assertThat(retained.labels(), contains("cluster"));
        assertThat(retained.skips(), contains(Set.of("pod")));
    }

    public void testTimeSeriesNameDerivesFromTheSkipSet() {
        assertThat(TranslationContext.mapRest(Set.of()), equalTo(MetadataAttribute.TIMESERIES));
        assertThat(TranslationContext.mapRest(Set.of("region", "pod")), equalTo(MetadataAttribute.TIMESERIES + "$pod$region"));
        assertThat(TranslationContext.mapRest(Set.of("pod", "region")), equalTo(TranslationContext.mapRest(Set.of("region", "pod"))));
    }

    public void testFindByNameMatchesCanonicalNamesAndPrefersPassthroughFields() {
        Attribute bare = attr("cluster");
        Attribute prefixed = new ReferenceAttribute(Source.EMPTY, "labels.cluster", DataType.KEYWORD);
        Attribute excluding = attr(TranslationContext.mapRest(Set.of("pod")));

        assertThat(TranslationContext.find(List.of(bare, prefixed), "cluster"), sameInstance(prefixed));
        assertThat(TranslationContext.find(List.of(bare), "cluster"), sameInstance(bare));
        assertThat(TranslationContext.find(List.of(bare, excluding), TranslationContext.mapRest(Set.of("pod"))), sameInstance(excluding));
        assertNull(TranslationContext.find(List.of(bare), "pod"));
        assertThat(TranslationContext.mapPromoted(List.of(bare, prefixed, attr("pod"))), contains("cluster", "pod"));
    }

    public void testUnionAllowsRestToOverlapPromoted() {
        TranslationConstraint header = union(promoted(List.of("pod")), rest(Set.of()));

        assertThat(header.labels(), contains("pod"));
        assertThat(header.skips(), contains(Set.of()));
    }

    public void testMultipleRestsCoexist() {
        TranslationConstraint header = union(rest(Set.of()), rest(Set.of("pod")));

        assertThat(header.skips(), containsInAnyOrder(Set.of(), Set.of("pod")));
    }

    public void testDeliveredLabelsExcludesStepValueAndTimeSeriesColumns() {
        Attribute step = attr("step");
        Attribute value = attr("value");
        Attribute pod = attr("pod");
        Attribute prefixed = new ReferenceAttribute(Source.EMPTY, "labels.cluster", DataType.KEYWORD);
        var timeseries = timeseries(Set.of("region"));
        var plan = new Aggregate(
            Source.EMPTY,
            new LocalRelation(Source.EMPTY, List.of(step, value, pod, prefixed), EmptyLocalSupplier.EMPTY),
            List.of(step, timeseries, pod, prefixed),
            List.of(value, step, timeseries.toAttribute(), pod, prefixed)
        );

        assertThat(TranslationContext.deliveredLabels(plan, step, value), containsInAnyOrder("pod", "cluster"));
    }

    public void testDeliveredSkipsReadsTimeSeriesDefinitions() {
        Attribute step = attr("step");
        Attribute value = attr("value");
        var whole = timeseries(Set.of());
        var pod = timeseries(Set.of("pod"));
        var plan = new Aggregate(
            Source.EMPTY,
            new LocalRelation(Source.EMPTY, List.of(step, value), EmptyLocalSupplier.EMPTY),
            List.of(step, whole, pod),
            List.of(value, step, whole.toAttribute(), pod.toAttribute())
        );

        assertThat(TranslationContext.deliveredSkips(plan), containsInAnyOrder(Set.of(), Set.of("pod")));

        // a _timeseries column projected away is not carried, even though its definition sits below
        var projected = new Project(Source.EMPTY, plan, List.of(value, step, whole.toAttribute()));
        assertThat(TranslationContext.deliveredSkips(projected), contains(Set.of()));
    }

    public void testFinestTimeSeriesPicksFewestExclusions() {
        Attribute step = attr("step");
        Attribute value = attr("value");
        var whole = timeseries(Set.of());
        var pod = timeseries(Set.of("pod"));
        var plan = new Aggregate(
            Source.EMPTY,
            new LocalRelation(Source.EMPTY, List.of(step, value), EmptyLocalSupplier.EMPTY),
            List.of(step, pod, whole),
            List.of(value, step, pod.toAttribute(), whole.toAttribute())
        );

        assertThat(TranslationContext.finestTimeSeries(plan).name(), equalTo(TranslationContext.mapRest(Set.of())));

        var bare = new LocalRelation(Source.EMPTY, List.of(value, step), EmptyLocalSupplier.EMPTY);
        assertNull(TranslationContext.finestTimeSeries(bare));
    }

    public void testIntermediateResultRebuildsAroundNewPlanAndValue() {
        Attribute step = attr("step");
        Attribute value = attr("value");
        var plan = new LocalRelation(Source.EMPTY, List.of(value, step), EmptyLocalSupplier.EMPTY);
        var table = new IntermediateResult(plan, value, step, Literal.TRUE);

        assertThat(table.valueColumn(), sameInstance(value));
        assertThat(table.pendingFilter(), sameInstance(Literal.TRUE));
        assertThat(table.kind(), equalTo(IntermediateResult.Kind.BEFORE_INITIAL_AGGREGATE));

        var next = new LocalRelation(Source.EMPTY, List.of(value, step), EmptyLocalSupplier.EMPTY);
        var rebuilt = table.with(next, Literal.NULL);
        assertThat(rebuilt.plan(), sameInstance(next));
        assertThat(rebuilt.value(), sameInstance(Literal.NULL));
        assertThat(rebuilt.step(), sameInstance(step));
        assertThat(rebuilt.pendingFilter(), sameInstance(Literal.TRUE));
    }

    private static Alias timeseries(Set<String> skip) {
        List<Expression> excluded = skip.stream().<Expression>map(TranslationContextTests::attr).toList();
        return new Alias(Source.EMPTY, TranslationContext.mapRest(skip), new TimeSeriesWithout(Source.EMPTY, excluded));
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    public void testTimeSeriesUnsetOnlyOnceEveryNodeSupportsIt() {
        TransportVersion unset = TimeSeriesUnset.ESQL_TIMESERIES_METADATA_UNSET;
        assertFalse(TranslationContext.supportsTimeSeriesUnset(FieldAttribute.ESQL_TIMESERIES_METADATA_ATTRIBUTE));
        assertFalse(TranslationContext.supportsTimeSeriesUnset(TransportVersionUtils.getPreviousVersion(unset)));
        assertTrue(TranslationContext.supportsTimeSeriesUnset(unset));
        assertTrue(TranslationContext.supportsTimeSeriesUnset(TransportVersion.current()));
    }

    /** Unlike {@link TranslationConstraint#subtract}, excluding labels keeps the one {@code _timeseries} as it is. */
    public void testExcludeDropsLabelsAndKeepsTheTimeSeries() {
        TranslationConstraint required = union(promoted(List.of("pod", "cluster")), rest());
        TranslationConstraint excluded = exclude(required, List.of("pod"));
        assertThat(excluded.labels(), contains("cluster"));
        assertThat(excluded.skips(), contains(Set.of()));
        assertThat(subtract(required, List.of("pod")).skips(), contains(Set.of("pod")));
    }
}
