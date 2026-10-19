/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;

/** Checks that each result owns one label representation and retains its physical bindings by identity. */
public class TranslationContextResultTests extends ESTestCase {
    /** These result contracts belong to the opt-in metadata representation. */
    @BeforeClass
    public static void requireFunctionalTimeSeries() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
    }

    /** Named labels and the complete metadata carrier cannot both define the result's identity. */
    public void testNamedLabelsAndMetadataAreExclusive() {
        Attribute host = attribute("host", DataType.KEYWORD);
        Attribute metadata = attribute("metadata", DataType.TIMESERIES_DATA_TYPE);
        expectThrows(IllegalArgumentException.class, () -> result(List.of(host, metadata), Map.of("host", host), metadata));
        assertSame(host, result(List.of(host), Map.of("host", host), null).labelColumn("host"));
        var derived = result(List.of(metadata), Map.of(), metadata);
        assertSame(metadata, derived.metadata());
        assertTrue(derived.labelBindings().isEmpty());
    }

    /** Caller mutations must not change either the logical labels or their requested order. */
    public void testBindingsAreAnOrderedDefensiveCopy() {
        Attribute host = attribute("private_host", DataType.KEYWORD);
        Attribute cluster = attribute("private_cluster", DataType.KEYWORD);
        var bindings = new LinkedHashMap<String, Attribute>();
        bindings.put("host", host);
        bindings.put("cluster", cluster);
        var result = result(List.of(host, cluster), bindings, null);
        bindings.clear();
        assertThat(result.labelBindings().keySet(), contains("host", "cluster"));
        assertSame(host, result.labelColumn("host"));
        assertSame(cluster, result.labelColumn("cluster"));
        expectThrows(UnsupportedOperationException.class, () -> result.labelBindings().clear());
        expectThrows(UnsupportedOperationException.class, () -> result.labelBindings().put("extra", host));
    }

    /** Updating only the sample leaves either label representation and its evaluation state intact. */
    public void testValueChangesPreserveLabelIdentity() {
        Attribute host = attribute("private_host", DataType.KEYWORD);
        Attribute metadata = attribute("metadata", DataType.TIMESERIES_DATA_TYPE);
        for (var child : List.of(result(List.of(host), Map.of("host", host), null), result(List.of(metadata), Map.of(), metadata))) {
            var value = new Literal(Source.EMPTY, 42.0, DataType.DOUBLE);
            var updated = child.with(child.plan(), value);
            assertSame(child.plan(), updated.plan());
            assertSame(value, updated.value());
            assertNotSame(child.value(), updated.value());
            assertSame(child.labelColumn("host"), updated.labelColumn("host"));
            assertSame(child.metadata(), updated.metadata());
            assertEquals(child.labelBindings(), updated.labelBindings());
            assertSame(child.step(), updated.step());
            assertSame(child.pendingFilter(), updated.pendingFilter());
            assertSame(child.kind(), updated.kind());
        }
    }

    /** Renamed physical columns retain their labels by NameId, never through a same-named replacement. */
    public void testNamedBindingsAreRetainedByIdentity() {
        Attribute label = attribute("private_label", DataType.KEYWORD);
        var child = result(List.of(label), Map.of("result", label), null);
        Attribute renamed = label.withName("renamed");
        Attribute unrelated = attribute(label.name(), DataType.KEYWORD);
        var renamedPlan = new LocalRelation(
            Source.EMPTY,
            List.of(child.step(), child.valueColumn(), unrelated, renamed),
            EmptyLocalSupplier.EMPTY
        );
        var rebound = child.with(renamedPlan, child.value());
        assertSame(renamed, rebound.labelColumn("result"));
        assertSame(label, child.labelColumn("result"));
        assertNull(rebound.labelColumn("private_label"));
        assertNull(rebound.labelColumn("step"));
        assertStatePreserved(child, rebound);
        var missingPlan = new LocalRelation(Source.EMPTY, List.of(child.step(), child.valueColumn(), unrelated), EmptyLocalSupplier.EMPTY);
        expectThrows(IllegalStateException.class, () -> child.with(missingPlan, child.value()));
    }

    /** The carrier is explicit even when the plan retains other metadata-valued temporary columns. */
    public void testMetadataIsRetainedByIdentity() {
        Attribute metadata = attribute("metadata", DataType.TIMESERIES_DATA_TYPE);
        Attribute incidental = attribute("incidental", DataType.TIMESERIES_DATA_TYPE);
        var child = result(List.of(incidental, metadata), Map.of(), metadata);
        assertSame(metadata, child.metadata());
        Attribute renamed = metadata.withName("renamed_metadata");
        var renamedPlan = new LocalRelation(
            Source.EMPTY,
            List.of(child.step(), child.valueColumn(), incidental, renamed),
            EmptyLocalSupplier.EMPTY
        );
        var rebound = child.with(renamedPlan, child.value());
        assertSame(renamed, rebound.metadata());
        assertSame(metadata, child.metadata());
        assertStatePreserved(child, rebound);
        var missingPlan = new LocalRelation(Source.EMPTY, List.of(child.step(), child.valueColumn(), incidental), EmptyLocalSupplier.EMPTY);
        expectThrows(IllegalStateException.class, () -> child.with(missingPlan, child.value()));
    }

    /** A named result must not discover labels or metadata by scanning columns left by earlier evaluation. */
    public void testIncidentalColumnsDoNotDefineLabels() {
        Attribute host = attribute("private_host", DataType.KEYWORD);
        Attribute incidental = attribute("shard", DataType.LONG);
        Attribute first = attribute("first_metadata", DataType.TIMESERIES_DATA_TYPE);
        Attribute second = attribute("second_metadata", DataType.TIMESERIES_DATA_TYPE);
        var child = result(List.of(host, incidental, first, second), Map.of("host", host), null);
        assertSame(host, child.labelColumn("host"));
        assertNull(child.labelColumn("shard"));
        assertNull(child.labelColumn("private_host"));
        assertNull(child.metadata());
        var rebound = child.with(child.plan(), child.value());
        assertNull(rebound.metadata());
        assertNull(rebound.labelColumn("shard"));
        assertEquals(child.labelBindings(), rebound.labelBindings());
    }

    /** Publishing a new representation clears the previous one without changing evaluation state or the input result. */
    public void testRepresentationChangesAreExplicit() {
        Attribute host = attribute("host", DataType.KEYWORD);
        Attribute metadata = attribute("metadata", DataType.TIMESERIES_DATA_TYPE);
        var child = result(List.of(host, metadata), Map.of("host", host), null);
        var derived = child.withMetadata(child.plan(), metadata);
        assertSame(metadata, derived.metadata());
        assertTrue(derived.labelBindings().isEmpty());
        assertNull(derived.labelColumn("host"));
        assertSame(host, child.labelColumn("host"));
        assertNull(child.metadata());
        assertStatePreserved(child, derived);
        var named = derived.withLabels(derived.plan(), Map.of("renamed_host", host));
        assertNull(named.metadata());
        assertSame(host, named.labelColumn("renamed_host"));
        assertNull(named.labelColumn("host"));
        assertSame(metadata, derived.metadata());
        assertStatePreserved(derived, named);
    }

    /** A metadata name cannot turn an ordinary string column into a typed metadata carrier. */
    public void testMetadataMustHaveTheDedicatedType() {
        Attribute textualMetadata = attribute("_timeseries", DataType.KEYWORD);
        expectThrows(IllegalArgumentException.class, () -> result(List.of(textualMetadata), Map.of(), textualMetadata));
        var named = result(List.of(textualMetadata), Map.of(), null);
        expectThrows(IllegalArgumentException.class, () -> named.withMetadata(named.plan(), textualMetadata));
    }

    private static IntermediateResult result(List<Attribute> columns, Map<String, Attribute> bindings, Attribute metadata) {
        Attribute step = attribute("step", DataType.DATETIME);
        Attribute value = attribute("result", DataType.DOUBLE);
        var output = new ArrayList<>(columns);
        output.add(step);
        output.add(value);
        var plan = new LocalRelation(Source.EMPTY, output, EmptyLocalSupplier.EMPTY);
        return new IntermediateResult(plan, value, step, Literal.TRUE, IntermediateResult.Kind.AFTER_INITIAL_AGGREGATE, bindings, metadata);
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
