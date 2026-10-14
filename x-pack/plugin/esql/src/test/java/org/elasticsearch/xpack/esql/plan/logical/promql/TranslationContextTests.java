/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintIntersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintProject;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintSub;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintUnion;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintUnset;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintWithPromoted;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class TranslationContextTests extends ESTestCase {

    public void testUnionMergesLabelsAndSkipSets() {
        TranslationSchema schema = newConstraintUnion(
            newConstraintUnion(
                newConstraintUnion(newConstraintWithPromoted(List.of("cluster")), newConstraintUnset(Set.of("pod"))),
                newConstraintUnset(Set.of("pod"))
            ),
            newConstraintWithPromoted(List.of("cluster", "region"))
        );

        assertThat(schema.labels(), contains("cluster", "region"));
        assertThat(schema.skips(), contains(Set.of("pod")));
        assertThat(newConstraintUnion(schema, TranslationSchema.EMPTY), equalTo(schema));
    }

    public void testSubtractDropsLabelsAndWidensSkipSets() {
        TranslationSchema above = newConstraintUnion(
            newConstraintWithPromoted(List.of("cluster", "pod")),
            newConstraintUnset(Set.of("region"))
        );

        TranslationSchema below = newConstraintSub(above, List.of("pod"));
        assertThat(below.labels(), contains("cluster"));
        assertThat(below.skips(), contains(Set.of("region", "pod")));

        // the regroup's own column composes as a second, finer skip set
        TranslationSchema child = newConstraintUnion(below, newConstraintUnset(Set.of("pod")));
        assertThat(child.skips(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
    }

    public void testIntersectIsTheUpwardCounterpartOfSubtract() {
        TranslationSchema required = newConstraintUnion(
            newConstraintWithPromoted(List.of("cluster", "pod")),
            newConstraintUnset(Set.of("region"))
        );
        TranslationSchema child = newConstraintUnion(newConstraintSub(required, List.of("pod")), newConstraintUnset(Set.of("pod")));

        TranslationSchema lifted = newConstraintIntersect(child, List.of("pod"));

        // every column the parent required, apart from the dropped label, comes back; so does the regroup's own
        // _timeseries column, which already excludes the dropped label and fixes the grain of the result
        assertThat(lifted.labels(), contains("cluster"));
        assertThat(lifted.skips(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
        // the regroup's own full label space does not survive dropping a label it still carries
        assertFalse(newConstraintIntersect(newConstraintUnset(), List.of("pod")).hasMetadata());
        // without () keeps everything
        assertThat(newConstraintIntersect(child, List.of()), equalTo(child));
    }

    public void testProjectKeepsSkipSets() {
        TranslationSchema schema = newConstraintUnion(
            newConstraintWithPromoted(List.of("cluster", "pod", "region")),
            newConstraintUnset(Set.of("pod"))
        );

        TranslationSchema retained = newConstraintProject(schema, List.of("cluster", "missing"));

        assertThat(retained.labels(), contains("cluster"));
        assertThat(retained.skips(), contains(Set.of("pod")));
    }

    public void testTimeSeriesNameDerivesFromTheSkipSet() {
        assertThat(TranslationContext.asMetadataLabel(Set.of()), equalTo(MetadataAttribute.TIMESERIES));
        assertThat(TranslationContext.asMetadataLabel(Set.of("region", "pod")), equalTo(MetadataAttribute.TIMESERIES + "$pod$region"));
        assertThat(
            TranslationContext.asMetadataLabel(Set.of("pod", "region")),
            equalTo(TranslationContext.asMetadataLabel(Set.of("region", "pod")))
        );
    }

    public void testFindByNameMatchesCanonicalNamesAndPrefersPassthroughFields() {
        Attribute bare = attr("cluster");
        Attribute prefixed = new ReferenceAttribute(Source.EMPTY, "labels.cluster", DataType.KEYWORD);
        Attribute excluding = attr(TranslationContext.asMetadataLabel(Set.of("pod")));

        assertThat(TranslationContext.find(List.of(bare, prefixed), "cluster"), sameInstance(prefixed));
        assertThat(TranslationContext.find(List.of(bare), "cluster"), sameInstance(bare));
        assertThat(
            TranslationContext.find(List.of(bare, excluding), TranslationContext.asMetadataLabel(Set.of("pod"))),
            sameInstance(excluding)
        );
        assertNull(TranslationContext.find(List.of(bare), "pod"));
        assertThat(TranslationContext.asPromotedLabels(List.of(bare, prefixed, attr("pod"))), contains("cluster", "pod"));
    }

    public void testUnionAllowsRestToOverlapPromoted() {
        TranslationSchema schema = newConstraintUnion(newConstraintWithPromoted(List.of("pod")), newConstraintUnset(Set.of()));

        assertThat(schema.labels(), contains("pod"));
        assertThat(schema.skips(), contains(Set.of()));
    }

    public void testMultipleRestsCoexist() {
        TranslationSchema schema = newConstraintUnion(newConstraintUnset(Set.of()), newConstraintUnset(Set.of("pod")));

        assertThat(schema.skips(), containsInAnyOrder(Set.of(), Set.of("pod")));
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

        assertThat(
            TranslationContext.newConstraintDeliveredBy(new IntermediateResult(plan, value, step)).labels(),
            containsInAnyOrder("pod", "cluster")
        );
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

        assertThat(
            TranslationContext.newConstraintDeliveredBy(new IntermediateResult(plan, value, step)).skips(),
            containsInAnyOrder(Set.of(), Set.of("pod"))
        );

        // a _timeseries column projected away is not carried, even though its definition sits below
        var projected = new Project(Source.EMPTY, plan, List.of(value, step, whole.toAttribute()));
        assertThat(TranslationContext.newConstraintDeliveredBy(new IntermediateResult(projected, value, step)).skips(), contains(Set.of()));
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

        assertThat(TranslationContext.finestTimeSeries(plan).name(), equalTo(TranslationContext.asMetadataLabel(Set.of())));

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
        return new Alias(Source.EMPTY, TranslationContext.asMetadataLabel(skip), new TimeSeriesWithout(Source.EMPTY, excluded));
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }
}
