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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationResult.Kind;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.sameInstance;

/** Pins the translator's state without making storage projections part of its label model. */
public class TranslationResultTests extends ESTestCase {
    public void testOneCurrentRecordAndNamedBindings() {
        Attribute cluster = attr("cluster");
        Attribute current = attr("current");
        Attribute obsolete = attr("old_record");
        var table = table(Map.of("cluster", cluster), current, cluster, current, obsolete);
        assertThat(table.attributes(), contains(current, cluster));
        assertSame(current, table.packedLabels());
        assertThat(table.labelNames(), contains("cluster"));
        assertTrue(table.shape().isOpen());
        assertNull(table.label("old_record"));
        assertThat(TranslationResult.scalar(plan(), Literal.NULL, attr("step")).attributes(), empty());
    }

    public void testReplacementDoesNotRetainOldRecord() {
        Attribute cluster = attr("cluster");
        Attribute old = attr("old_record");
        Attribute updated = attr("updated_record");
        var table = table(Map.of("cluster", cluster), old, cluster, old, updated);
        var result = table.with(table.plan(), table.labels(), updated, table.value());
        assertThat(result.attributes(), contains(updated, cluster));
        assertSame(old, table.packedLabels());
        assertSame(updated, result.packedLabels());
        var closed = result.with(result.plan(), result.labels(), null, result.value());
        assertFalse(closed.shape().isOpen());
        assertThat(closed.attributes(), contains(cluster));
    }

    public void testBindingsUseLogicalNamesAndAreImmutable() {
        Attribute cluster = attr("physical_column");
        var labels = new LinkedHashMap<String, Attribute>();
        labels.put("cluster", cluster);
        var table = table(labels, null, cluster);
        labels.clear();
        assertSame(cluster, table.label("cluster"));
        assertNull(table.label("physical_column"));
        expectThrows(UnsupportedOperationException.class, () -> table.labels().clear());
    }

    public void testBothRepresentationsMustBelongToTheOutput() {
        Attribute cluster = attr("cluster");
        Attribute packed = attr("record");
        for (Kind kind : Kind.values()) {
            expectThrows(
                AssertionError.class,
                () -> new TranslationResult(
                    plan(cluster, packed),
                    Map.of("cluster", attr("cluster")),
                    packed,
                    Literal.NULL,
                    attr("step"),
                    null,
                    kind
                )
            );
            expectThrows(
                AssertionError.class,
                () -> new TranslationResult(
                    plan(cluster, packed),
                    Map.of("cluster", cluster),
                    attr("record"),
                    Literal.NULL,
                    attr("step"),
                    null,
                    kind
                )
            );
        }
    }

    public void testPromqlLabelsFindPrefersPassthroughFields() {
        Attribute bare = attr("cluster");
        Attribute prefixed = attr("labels.cluster");
        assertThat(PromqlLabels.find(List.of(bare, prefixed), "cluster"), sameInstance(prefixed));
        assertThat(PromqlLabels.find(List.of(bare), "cluster"), sameInstance(bare));
        assertNull(PromqlLabels.find(List.of(bare), "pod"));
        assertThat(PromqlLabels.labelNames(List.of(bare, prefixed, attr("pod"))), contains("cluster", "pod"));
    }

    private static TranslationResult table(Map<String, Attribute> labels, Attribute packed, Attribute... output) {
        return new TranslationResult(plan(output), labels, packed, Literal.NULL, attr("step"), null, Kind.AFTER_INITIAL_AGGREGATE);
    }

    private static LogicalPlan plan(Attribute... output) {
        return new LocalRelation(Source.EMPTY, List.of(output), EmptyLocalSupplier.EMPTY);
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }
}
