/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.DynamicColumnList;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.Static;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationResult.Kind;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.sameInstance;

public class TranslationResultTests extends ESTestCase {

    public void testLabelsAreOrderedDynamicByIncreasingExclusionsThenStatic() {
        Attribute cluster = attr("cluster");
        Attribute full = tsma();
        Attribute withoutPod = tsma("pod");
        Attribute withoutPodRegion = tsma("pod", "region");
        var labels = new LinkedHashMap<TranslationColumn, Attribute>();
        labels.put(new Static("cluster"), cluster);
        labels.put(new DynamicColumnList(Set.of("pod", "region")), withoutPodRegion);
        labels.put(new DynamicColumnList(Set.of("pod")), withoutPod);
        labels.put(new DynamicColumnList(Set.of()), full);

        TranslationResult table = table(labels, cluster, full, withoutPod, withoutPodRegion);

        assertThat(table.attributes(), contains(full, withoutPod, withoutPodRegion, cluster));
        assertSame(full, table.grain());
        assertThat(table.statics(), contains("cluster"));
        assertThat(table.shape().names(), contains("cluster"));
        assertTrue(table.shape().isOpen());
        // equal-size exclusion sets are ordered by name, so neither is lost
        Attribute withoutRegion = tsma("region");
        TranslationResult tie = table(
            Map.of(new DynamicColumnList(Set.of("region")), withoutRegion, new DynamicColumnList(Set.of("pod")), withoutPod),
            withoutRegion,
            withoutPod
        );
        assertThat(
            tie.attributes().stream().map(attribute -> ((TimeSeriesMetadataAttribute) attribute).excludedFields()).toList(),
            contains(Set.of("pod"), Set.of("region"))
        );
        TranslationResult scalar = TranslationResult.scalar(plan(), Literal.NULL, attr("step"));
        assertNull(scalar.grain());
        assertThat(scalar.attributes(), empty());
    }

    public void testLabelLookupSeesOnlyTheLabelColumns() {
        Attribute cluster = attr("opaque_label");
        Attribute undeclared = attr("pod");
        Attribute full = tsma();
        Attribute withoutPod = tsma("pod");
        Map<TranslationColumn, Attribute> labels = Map.of(
            new Static("cluster"),
            cluster,
            new DynamicColumnList(Set.of()),
            full,
            new DynamicColumnList(Set.of("pod")),
            withoutPod
        );
        for (Kind kind : Kind.values()) {
            var result = new TranslationResult(plan(cluster, undeclared, full, withoutPod), labels, Literal.NULL, attr("step"), null, kind);
            // Only declared columns are visible, under their declared names; the plan may carry more.
            assertSame(cluster, result.label("cluster"));
            assertNull(result.label("pod"));
            assertSame(full, result.grain());
            assertSame(withoutPod, result.labels().get(new DynamicColumnList(Set.of("pod"))));
            assertEquals(kind, result.kind());
        }
    }

    public void testDropRemovesLabelsAndTheDynamicColumnsStillCarryingThem() {
        Attribute cluster = attr("cluster");
        Attribute pod = attr("pod");
        Attribute full = tsma();
        Attribute withoutPod = tsma("pod");
        TranslationResult table = table(
            Map.of(
                new Static("cluster"),
                cluster,
                new Static("pod"),
                pod,
                new DynamicColumnList(Set.of()),
                full,
                new DynamicColumnList(Set.of("pod")),
                withoutPod
            ),
            cluster,
            pod,
            full,
            withoutPod
        );

        TranslationResult dropped = table.drop(Set.of("pod"));

        assertThat(dropped.statics(), contains("cluster"));
        assertSame(withoutPod, dropped.grain());
        assertTrue(dropped.shape().excludes(Set.of("pod")));
        assertSame(table.plan(), dropped.plan());
        assertSame(table.step(), dropped.step());
        // dropping the last label a packing excludes closes the table
        assertNull(dropped.drop(Set.of("cluster", "region")).grain());
    }

    public void testBindAddsOrShadowsALabel() {
        Attribute cluster = attr("cluster");
        Attribute derived = attr("cluster");
        Attribute region = attr("region");
        TranslationResult table = table(Map.of(new Static("cluster"), cluster), cluster, derived, region);

        TranslationResult shadowed = table.bind("cluster", derived);
        assertSame(derived, shadowed.label("cluster"));
        assertThat(shadowed.statics(), contains("cluster"));

        TranslationResult widened = shadowed.bind("region", region);
        assertThat(widened.statics(), contains("cluster", "region"));
        assertThat(widened.attributes(), contains(derived, region));
        expectThrows(UnsupportedOperationException.class, () -> widened.labels().clear());
    }

    public void testLabelAttributesMustBelongToThePlanOutput() {
        Attribute cluster = attr("cluster");
        Attribute packed = tsma();
        LogicalPlan plan = plan(cluster, packed);
        for (Kind kind : Kind.values()) {
            // The name is the same, but this attribute's identity is not part of the plan's output.
            expectThrows(
                AssertionError.class,
                () -> new TranslationResult(
                    plan,
                    Map.of(new Static("cluster"), attr("cluster"), new DynamicColumnList(Set.of()), packed),
                    Literal.NULL,
                    attr("step"),
                    null,
                    kind
                )
            );
            expectThrows(
                AssertionError.class,
                () -> new TranslationResult(
                    plan,
                    Map.of(new Static("cluster"), cluster, new DynamicColumnList(Set.of()), attr("_timeseries")),
                    Literal.NULL,
                    attr("step"),
                    null,
                    kind
                )
            );
        }
        // every label column is bound
        expectThrows(
            AssertionError.class,
            () -> new TranslationResult(
                plan,
                Collections.singletonMap(new Static("cluster"), null),
                Literal.NULL,
                attr("step"),
                null,
                Kind.CONSTANT
            )
        );
    }

    public void testPromqlLabelsFindPrefersPassthroughFields() {
        Attribute bare = attr("cluster");
        Attribute prefixed = new ReferenceAttribute(Source.EMPTY, "labels.cluster", DataType.KEYWORD);
        Attribute packed = tsma("pod");

        assertThat(PromqlLabels.find(List.of(bare, prefixed), "cluster"), sameInstance(prefixed));
        assertThat(PromqlLabels.find(List.of(bare), "cluster"), sameInstance(bare));
        assertNull(PromqlLabels.find(List.of(bare), "pod"));
        assertThat(PromqlLabels.labelNames(List.of(bare, prefixed, attr("pod"))), contains("cluster", "pod"));
        assertThat(new DynamicColumnList(Set.of("pod")).find(List.of(bare, packed)), sameInstance(packed));
        assertNull(new DynamicColumnList(Set.of()).find(List.of(bare, packed)));
    }

    private static TranslationResult table(Map<TranslationColumn, Attribute> labels, Attribute... output) {
        return new TranslationResult(plan(output), labels, Literal.NULL, attr("step"), null, Kind.AFTER_INITIAL_AGGREGATE);
    }

    private static LogicalPlan plan(Attribute... output) {
        return new LocalRelation(Source.EMPTY, List.of(output), EmptyLocalSupplier.EMPTY);
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    private static TimeSeriesMetadataAttribute tsma(String... except) {
        return new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of(except));
    }
}
