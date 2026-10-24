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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.LabelEval;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesGet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;

/** Checks that label materialization respects the child's current identity and preserves its evaluation state. */
public class TranslationContextLabelsTests extends ESTestCase {
    private final TranslationContext context = new TranslationContext(null, analyzer().buildContext());

    /** Existing named columns need no Eval, even when the plan retains unbound metadata-valued columns. */
    public void testNamedBindingsReuseColumns() {
        Attribute host = attribute("host", DataType.KEYWORD);
        Attribute cluster = attribute("cluster", DataType.KEYWORD);
        Attribute incidental = attribute("metadata", DataType.TIMESERIES_DATA_TYPE);
        var child = result(List.of(host, cluster, incidental), Map.of("host", host, "cluster", cluster), null);
        var labels = context.addInput(Source.EMPTY, child, List.of(attribute("host", DataType.KEYWORD)), false);
        assertSame(child.plan(), labels.plan());
        assertSame(host, labels.labelColumn("host"));
        assertNull(labels.labelColumn("cluster"));
        assertEquals(Map.of("host", host), labels.labelBindings());
        assertNull(labels.metadata());
        assertSame(cluster, child.labelColumn("cluster"));
        assertEquals(Set.of("host", "cluster"), child.labelBindings().keySet());
        assertStatePreserved(child, labels);
    }

    /** Metadata reads use the current carrier, convert native types, and materialize each requested label once. */
    public void testMetadataBindingsConvertAndDeduplicate() {
        Attribute metadata = attribute("current_metadata", DataType.TIMESERIES_DATA_TYPE);
        var child = result(List.of(metadata), Map.of(), metadata);
        var requested = List.of(
            attribute("host", DataType.KEYWORD),
            attribute("shard", DataType.LONG),
            attribute("enabled", DataType.BOOLEAN),
            attribute("address", DataType.IP),
            attribute("shard", DataType.LONG)
        );
        var labels = context.addInput(Source.EMPTY, child, requested, false);
        Eval eval = asInstanceOf(Eval.class, labels.plan());
        assertSame(child.plan(), eval.child());
        assertEquals(4, eval.fields().size());
        assertEquals(Set.of("host", "shard", "enabled", "address"), labels.labelBindings().keySet());
        for (int i = 0; i < eval.fields().size(); i++) {
            Attribute requestedLabel = requested.get(i);
            Alias alias = eval.fields().get(i);
            assertEquals(alias.toAttribute(), labels.labelColumn(requestedLabel.name()));
            assertEquals(requestedLabel.dataType(), alias.dataType());
            assertTrue(alias.resolved());
            Expression value = asInstanceOf(LabelEval.class, alias.child()).surrogate();
            TimeSeriesGet get = asInstanceOf(
                TimeSeriesGet.class,
                requestedLabel.dataType() == DataType.KEYWORD ? value : value.children().getFirst()
            );
            assertSame(metadata, get.children().getFirst());
            assertEquals(Literal.keyword(Source.EMPTY, requestedLabel.name()), get.children().get(1));
        }
        assertSame(metadata, child.metadata());
        assertTrue(child.labelBindings().isEmpty());
        assertEquals(List.of(metadata, child.step(), child.value()), child.plan().output());
        assertStatePreserved(child, labels);
        assertNull(labels.metadata());
        assertSame(metadata, eval.output().stream().filter(a -> a.dataType() == DataType.TIMESERIES_DATA_TYPE).findFirst().orElseThrow());
    }

    /** Duplicate declarations with incompatible types must not silently reuse the first conversion. */
    public void testConflictingDuplicateTypesAreRejected() {
        Attribute metadata = attribute("metadata", DataType.TIMESERIES_DATA_TYPE);
        var child = result(List.of(metadata), Map.of(), metadata);
        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> context.addInput(
                Source.EMPTY,
                child,
                List.of(attribute("host", DataType.KEYWORD), attribute("host", DataType.LONG)),
                false
            )
        );
        assertEquals("Conflicting types for label [host]", error.getMessage());
    }

    /** Named and derived bindings obey the same requested-type contract, including the first read of a label. */
    public void testConflictingNamedTypeIsRejectedOnFirstRead() {
        Attribute host = attribute("host", DataType.KEYWORD);
        var child = result(List.of(host), Map.of("host", host), null);
        for (int copies : List.of(1, 2)) {
            var requested = Collections.nCopies(copies, attribute("host", DataType.LONG));
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> context.addInput(Source.EMPTY, child, requested, false)
            );
            assertEquals("Conflicting types for label [host]", error.getMessage());
        }
    }

    /** A label removed by an earlier aggregation cannot be resurrected from a column outside its published schema. */
    public void testAbsentNamedLabelUsesTypedNull() {
        Attribute host = attribute("host", DataType.KEYWORD);
        Attribute incidental = attribute("shard", DataType.LONG);
        var child = result(List.of(host, incidental), Map.of("host", host), null);
        var labels = context.addInput(Source.EMPTY, child, List.of(attribute("shard", DataType.LONG)), false);
        Eval eval = asInstanceOf(Eval.class, labels.plan());
        assertEquals(1, eval.fields().size());
        assertEquals(new Literal(Source.EMPTY, null, DataType.LONG), eval.fields().getFirst().child());
        assertEquals(DataType.LONG, labels.labelColumn("shard").dataType());
        assertNotEquals(incidental.id(), labels.labelColumn("shard").id());
        assertNull(child.labelColumn("shard"));
        assertSame(host, child.labelColumn("host"));
        assertStatePreserved(child, labels);
    }

    /** Including no labels clears either representation without evaluating metadata or changing the child plan. */
    public void testEmptyIncludeClearsLabelsWithoutEval() {
        Attribute host = attribute("host", DataType.KEYWORD);
        Attribute metadata = attribute("metadata", DataType.TIMESERIES_DATA_TYPE);
        for (var child : List.of(result(List.of(host), Map.of("host", host), null), result(List.of(metadata), Map.of(), metadata))) {
            var labels = context.addInput(Source.EMPTY, child, List.of(), false);
            assertSame(child.plan(), labels.plan());
            assertTrue(labels.labelBindings().isEmpty());
            assertNull(labels.metadata());
            assertStatePreserved(child, labels);
        }
    }

    /** An excluded label stays absent even while its source column remains physically available. */
    public void testIncludeAfterExcludeDoesNotRestoreSourceLabel() {
        Attribute shard = attribute("shard", DataType.LONG);
        var child = result(List.of(shard), Map.of("shard", shard), null);
        var excluded = context.addInput(Source.EMPTY, child, List.of(shard), true);
        var included = context.addInput(Source.EMPTY, excluded, List.of(shard), false);
        assertSame(child.plan(), excluded.plan());
        assertNull(excluded.labelColumn("shard"));
        Eval eval = asInstanceOf(Eval.class, included.plan());
        assertSame(child.plan(), eval.child());
        assertEquals(new Literal(Source.EMPTY, null, DataType.LONG), eval.fields().getFirst().child());
        assertNotEquals(shard.id(), included.labelColumn("shard").id());
        assertSame(shard, child.labelColumn("shard"));
        assertStatePreserved(child, included);
    }

    /** Removing named labels changes the consumer's schema without adding metadata or rewriting the input plan. */
    public void testWithoutNamedLabelsDoesNotAddEval() {
        Attribute host = attribute("private_host", DataType.KEYWORD);
        Attribute cluster = attribute("private_cluster", DataType.KEYWORD);
        Attribute name = attribute("private_name", DataType.KEYWORD);
        var child = result(List.of(host, cluster, name), Map.of("host", host, "cluster", cluster, LabelMatcher.NAME, name), null);
        var mappedHost = new FieldAttribute(
            Source.EMPTY,
            "labels.host",
            new EsField("labels.host", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION)
        );
        var labels = context.addInput(Source.EMPTY, child, List.of(mappedHost), true);
        assertSame(child.plan(), labels.plan());
        assertEquals(Map.of("cluster", cluster), labels.labelBindings());
        assertSame(cluster, labels.labelColumn("cluster"));
        assertNull(labels.labelColumn("host"));
        assertNull(labels.labelColumn(LabelMatcher.NAME));
        assertEquals(Set.of("host", "cluster", LabelMatcher.NAME), child.labelBindings().keySet());
        assertSame(host, child.labelColumn("host"));
        assertSame(name, child.labelColumn(LabelMatcher.NAME));
        assertNull(labels.metadata());
        assertStatePreserved(child, labels);
    }

    /** Metadata removal replaces its carrier at this boundary, leaving exactly one carrier and an unchanged child. */
    public void testWithoutMetadataReplacesTheCarrier() {
        Attribute metadata = attribute("current_metadata", DataType.TIMESERIES_DATA_TYPE);
        var child = result(List.of(metadata), Map.of(), metadata);
        var labels = context.addInput(Source.EMPTY, child, List.of(attribute("host", DataType.KEYWORD)), true);
        Eval eval = asInstanceOf(Eval.class, labels.plan());
        assertSame(child.plan(), eval.child());
        assertEquals(1, eval.fields().size());
        Alias alias = eval.fields().getFirst();
        TimeSeriesUnset unset = asInstanceOf(TimeSeriesUnset.class, alias.child());
        assertSame(metadata, unset.children().getFirst());
        assertEquals(
            Set.of(Literal.keyword(Source.EMPTY, "host"), Literal.keyword(Source.EMPTY, LabelMatcher.NAME)),
            unset.children().stream().skip(1).collect(Collectors.toSet())
        );
        assertEquals(metadata.name(), alias.name());
        assertNotEquals(metadata.id(), alias.id());
        assertEquals(1, eval.output().stream().filter(a -> a.dataType() == DataType.TIMESERIES_DATA_TYPE).count());
        assertSame(metadata, child.metadata());
        assertEquals(List.of(metadata, child.step(), child.value()), child.plan().output());
        assertTrue(labels.labelBindings().isEmpty());
        assertEquals(alias.toAttribute(), labels.metadata());
        assertStatePreserved(child, labels);
    }

    /** Empty WITHOUT still removes the metric name while preserving every other label and the input identity. */
    public void testEmptyWithoutOnlyRemovesMetricName() {
        Attribute host = attribute("host", DataType.KEYWORD);
        Attribute name = attribute("metric_name", DataType.KEYWORD);
        var named = result(List.of(host, name), Map.of("host", host, LabelMatcher.NAME, name), null);
        var namedOutput = context.addInput(Source.EMPTY, named, List.of(), true);
        assertSame(named.plan(), namedOutput.plan());
        assertEquals(Map.of("host", host), namedOutput.labelBindings());
        assertSame(name, named.labelColumn(LabelMatcher.NAME));
        Attribute metadata = attribute("metadata", DataType.TIMESERIES_DATA_TYPE);
        var derived = result(List.of(metadata), Map.of(), metadata);
        var derivedOutput = context.addInput(Source.EMPTY, derived, List.of(), true);
        Eval eval = asInstanceOf(Eval.class, derivedOutput.plan());
        TimeSeriesUnset unset = asInstanceOf(TimeSeriesUnset.class, eval.fields().getFirst().child());
        assertEquals(List.of(metadata, Literal.keyword(Source.EMPTY, LabelMatcher.NAME)), unset.children());
        assertSame(metadata, derived.metadata());
        assertTrue(derivedOutput.labelBindings().isEmpty());
        assertStatePreserved(named, namedOutput);
        assertStatePreserved(derived, derivedOutput);
    }

    private static IntermediateResult result(List<Attribute> columns, Map<String, Attribute> bindings, Attribute metadata) {
        Attribute step = attribute("step", DataType.DATETIME);
        Attribute value = attribute("value", DataType.DOUBLE);
        var output = new ArrayList<>(columns);
        output.add(step);
        output.add(value);
        var plan = new LocalRelation(Source.EMPTY, output, EmptyLocalSupplier.EMPTY);
        return new IntermediateResult(
            plan,
            value,
            step,
            Literal.TRUE,
            IntermediateResult.Kind.BEFORE_INITIAL_AGGREGATE,
            bindings,
            metadata
        );
    }

    private static void assertStatePreserved(IntermediateResult before, IntermediateResult after) {
        assertSame(before.value(), after.value());
        assertSame(before.step(), after.step());
        assertSame(before.pendingFilter(), after.pendingFilter());
        assertSame(before.kind(), after.kind());
    }

    private static Attribute attribute(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }
}
