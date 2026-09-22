/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.Header;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.bind;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.filter;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.finite;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.mapOpen;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.open;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.select;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.sub;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.union;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class TranslationContextTests extends ESTestCase {

    public void testUnionMergesRegularAndPackedColumns() {
        Header header = union(finite(List.of("cluster")), open(Set.of("pod")), open(Set.of("pod")), finite(List.of("cluster", "region")));

        assertThat(header.finiteColumns(), contains("cluster", "region"));
        assertThat(header.openColumns(), contains(Set.of("pod")));
        assertThat(union(header, Header.PassThrough), equalTo(header));
        assertThat(union(header), equalTo(header));
        assertThat(union(), equalTo(Header.PassThrough));
    }

    public void testOpenLiftsAFiniteHeaderIntoAPackedColumn() {
        Header dropped = union(finite(List.of("pod")), finite(List.of("__name__")));
        assertThat(open(dropped), equalTo(open(Set.of("pod", "__name__"))));
        // a `without (pod)` requirement, as written in the translator
        Header below = union(sub(open(), dropped), open(dropped));
        assertThat(below.finiteColumns(), empty());
        assertThat(below.openColumns(), contains(Set.of("pod", "__name__")));
    }

    public void testSubtractDropsColumnsAndWidensPackedColumns() {
        Header above = union(finite(List.of("cluster", "pod")), open(Set.of("region")));

        Header below = sub(above, finite(List.of("pod")));
        assertThat(below.finiteColumns(), contains("cluster"));
        assertThat(below.openColumns(), contains(Set.of("region", "pod")));

        // the regroup's own packed column composes as a second, finer one
        Header child = union(below, open(Set.of("pod")));
        assertThat(child.openColumns(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
    }

    public void testSurvivingIsTheUpwardCounterpartOfSubtract() {
        Header required = union(finite(List.of("cluster", "pod")), open(Set.of("region")));
        Header child = union(sub(required, finite(List.of("pod"))), open(Set.of("pod")));
        Header carried = bound(child);

        Header lifted = select(carried, finite(List.of("pod")));

        // every column the parent required, apart from the dropped label, comes back; so does the regroup's own
        // packing, which already excludes the dropped label and fixes the grain of the result - with its binding
        assertThat(lifted.finiteColumns(), contains("cluster"));
        assertThat(lifted.openColumns(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
        assertThat(lifted, equalTo(carried));
        assertTrue(lifted.isBound());
        // the regroup's own full label space does not survive dropping a label it still carries
        assertFalse(select(bound(open()), finite(List.of("pod"))).isOpen());
        // without () keeps everything
        assertThat(select(carried, Header.PassThrough), equalTo(carried));
    }

    public void testNarrowingAHeaderDropsTheBindingsOfRemovedColumns() {
        Header table = bound(union(finite(List.of("cluster", "pod")), open()));
        Attribute cluster = table.getExpr("cluster");

        // sub: the dropped label goes, and the widened packing is a new column nothing carries yet, so it is unbound
        Header below = sub(table, finite(List.of("pod")));
        assertThat(below.finiteColumns(), contains("cluster"));
        assertThat(below.openColumns(), contains(Set.of("pod")));
        assertSame(cluster, below.getExpr("cluster"));
        assertNull(below.getExpr("pod"));
        assertNull(below.getExpr(Set.of("pod")));
        assertFalse(below.isBound());
        // a packing that already excludes the dropped label keeps its name, and with it its binding
        Header same = sub(bound(open(Set.of("pod"))), finite(List.of("pod")));
        assertTrue(same.isBound());

        // filter keeps the columnExpr of the retained columns
        Header retained = filter(table, finite(List.of("cluster")));
        assertSame(cluster, retained.getExpr("cluster"));
        assertTrue(retained.isBound());

        // union keeps the first binding of a column bound on both sides, and adds requirements unbound
        Header merged = union(table, bound(finite(List.of("cluster"))), finite(List.of("region")));
        assertSame(cluster, merged.getExpr("cluster"));
        assertNull(merged.getExpr("region"));
        assertFalse(merged.isBound());
    }

    public void testPackedColumnsAreOrderedByIncreasingExclusions() {
        Header header = union(open(Set.of("region", "pod")), open(Set.of("pod")), open());
        assertThat(header.openColumns(), contains(Set.of(), Set.of("pod"), Set.of("region", "pod")));
        // widening every packed column keeps the order; the header is a set, so the order does not affect equality
        assertThat(
            sub(header, finite(List.of("zone"))).openColumns(),
            contains(Set.of("zone"), Set.of("pod", "zone"), Set.of("region", "pod", "zone"))
        );
        assertThat(header, equalTo(union(open(), open(Set.of("pod")), open(Set.of("region", "pod")))));

        Header table = bound(header);
        assertThat(table.openColumns().iterator().next(), equalTo(Set.of()));
        assertFalse(Header.PassThrough.isOpen());
        assertTrue(Header.PassThrough.isEmpty());
        assertTrue(Header.PassThrough.isBound());
    }

    public void testDistinctPackedColumnsOfEqualSizeAreBothKeptInAStableOrder() {
        // ordered by size alone, `{pod}` and `{region}` would compare equal and one would be lost
        Header header = union(open(Set.of("region")), open(Set.of("pod")), open(Set.of("zone", "pod")));
        assertThat(header.openColumns(), contains(Set.of("pod"), Set.of("region"), Set.of("pod", "zone")));
        // the order is a property of the header, not of how it was built
        assertThat(union(open(Set.of("pod")), open(Set.of("region"))).openColumns(), contains(Set.of("pod"), Set.of("region")));
        assertThat(header.openColumns().iterator().next(), equalTo(Set.of("pod")));
    }

    public void testHeaderColumnsAreImmutable() {
        Header header = union(finite(List.of("cluster")), open(Set.of("pod")));
        expectThrows(UnsupportedOperationException.class, () -> header.finiteColumns().add("pod"));
        expectThrows(UnsupportedOperationException.class, () -> header.openColumns().add(Set.of()));
        expectThrows(UnsupportedOperationException.class, () -> header.openColumns().iterator().next().add("zone"));
        expectThrows(UnsupportedOperationException.class, () -> header.columnExpr().put("cluster", attr("cluster")));
    }

    public void testBindResolvesColumnsAndNullFillsMissingLabels() {
        Attribute cluster = attr("cluster");
        Attribute packed = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("labels.pod"));
        var plan = new LocalRelation(Source.EMPTY, List.of(cluster, packed), EmptyLocalSupplier.EMPTY);
        Header input = new Header(Set.of("cluster"), Set.of(Set.of("pod")), Map.of("cluster", cluster, mapOpen(Set.of("pod")), packed));
        Header grouping = union(finite(List.of("cluster", "region")), open(Set.of("pod")));

        Header bound = bind(grouping, input);

        assertTrue(bound.isBound());
        assertThat(bound.finiteColumns(), equalTo(grouping.finiteColumns()));
        assertThat(bound.openColumns(), equalTo(grouping.openColumns()));
        assertSame(cluster, bound.getExpr("cluster"));
        assertSame(packed, bound.getExpr(Set.of("pod")));
        // the label the input lacks is bound to a fresh reference under its own name ...
        Attribute region = bound.getExpr("region");
        assertThat(region.name(), equalTo("region"));
        // ... which the keys expose after the packing and the carried label, in header order ...
        assertThat(bound.expressions(), contains(packed, cluster, region));
        // ... and which the plan must define as null, being the one column it does not carry
        List<Alias> nullFills = bound.nullFills(plan);
        assertThat(nullFills, hasSize(1));
        assertThat(nullFills.getFirst().name(), equalTo("region"));
        assertThat(nullFills.getFirst().id(), equalTo(region.id()));
        assertThat(nullFills.getFirst().child(), equalTo(new Literal(Source.EMPTY, null, DataType.KEYWORD)));
        // once defined, nothing is missing
        var defined = new LocalRelation(Source.EMPTY, List.of(cluster, packed, region), EmptyLocalSupplier.EMPTY);
        assertThat(bound.nullFills(defined), empty());
        // a requirement has no keys to bind against
        expectThrows(AssertionError.class, grouping::expressions);
    }

    public void testBindOneColumnAtATimeFoldsARequirementIntoATable() {
        Header in = union(finite(List.of("cluster")), open(Set.of("pod")));
        Attribute cluster = attr("cluster");
        Attribute packed = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("labels.pod"));

        Header out = in;
        for (Set<String> packedField : in.openColumns()) {
            out = bind(out, packedField, packed);
        }
        for (String name : in.finiteColumns()) {
            out = bind(out, name, cluster);
        }

        assertFalse(in.isBound());
        assertTrue(out.isBound());
        assertThat(out.finiteColumns(), equalTo(in.finiteColumns()));
        assertThat(out.openColumns(), equalTo(in.openColumns()));
        assertSame(cluster, out.getExpr("cluster"));
        assertSame(packed, out.getExpr(Set.of("pod")));
        // binding a column the header does not have yet adds it
        Attribute full = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        Header widened = bind(bind(out, Set.of(), full), "region", attr("region"));
        assertThat(widened.openColumns(), contains(Set.of(), Set.of("pod")));
        assertThat(widened.finiteColumns(), contains("cluster", "region"));
        assertSame(full, widened.getExpr(widened.openColumns().iterator().next()));
    }

    /** The header of a table exposing every column of {@code header}, each bound to a fresh attribute. */
    private static Header bound(Header header) {
        var columnExpr = new LinkedHashMap<String, Attribute>();
        header.finiteColumns().forEach(name -> columnExpr.put(name, attr(name)));
        header.openColumns().forEach(exclusions -> columnExpr.put(mapOpen(exclusions), attr(mapOpen(exclusions))));
        return new Header(header.finiteColumns(), header.openColumns(), columnExpr);
    }

    public void testFilterKeepsPackedColumns() {
        Header header = union(finite(List.of("cluster", "pod", "region")), open(Set.of("pod")));

        Header retained = filter(header, finite(List.of("cluster", "missing")));

        assertThat(retained.finiteColumns(), contains("cluster"));
        assertThat(retained.openColumns(), contains(Set.of("pod")));
    }

    public void testPackedColumnNameDerivesFromItsExclusions() {
        assertThat(mapOpen(Set.of()), equalTo(MetadataAttribute.TIMESERIES));
        assertThat(mapOpen(Set.of("region", "pod")), equalTo(MetadataAttribute.TIMESERIES + "$pod$region"));
        assertThat(mapOpen(Set.of("pod", "region")), equalTo(mapOpen(Set.of("region", "pod"))));
    }

    public void testFindByNameMatchesCanonicalNamesAndPrefersPassthroughFields() {
        Attribute bare = attr("cluster");
        Attribute prefixed = new ReferenceAttribute(Source.EMPTY, "labels.cluster", DataType.KEYWORD);
        Attribute packed = attr(mapOpen(Set.of("pod")));

        assertThat(TranslationContext.find(List.of(bare, prefixed), "cluster"), sameInstance(prefixed));
        assertThat(TranslationContext.find(List.of(bare), "cluster"), sameInstance(bare));
        assertThat(TranslationContext.find(List.of(bare, packed), mapOpen(Set.of("pod"))), sameInstance(packed));
        assertNull(TranslationContext.find(List.of(bare), "pod"));
        assertThat(TranslationContext.mapFinite(List.of(bare, prefixed, attr("pod"))), contains("cluster", "pod"));
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    public void testColumnExpressionsAreOpaqueAndOnlyDeclaredColumnsAreVisible() {
        Attribute cluster = attr("opaque_label");
        Attribute undeclared = attr("pod");
        Attribute full = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        Attribute withoutPod = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("labels.pod"));
        var plan = new LocalRelation(Source.EMPTY, List.of(cluster, undeclared, full, withoutPod), EmptyLocalSupplier.EMPTY);
        for (Kind kind : Kind.values()) {
            var result = new IntermediateResult(
                plan,
                new Header(
                    Set.of("cluster"),
                    Set.of(Set.of(), Set.of("pod")),
                    Map.of("cluster", cluster, mapOpen(Set.of()), full, mapOpen(Set.of("pod")), withoutPod)
                ),
                Literal.NULL,
                attr("step"),
                null,
                kind
            );
            // Only declared columns are visible, under their declared names; the plan may carry more.
            assertThat(result.header().finiteColumns(), contains("cluster"));
            assertThat(result.header().openColumns(), containsInAnyOrder(Set.of(), Set.of("pod")));
            assertSame(cluster, result.getExpr("cluster"));
            assertNull(result.getExpr("pod"));
            assertSame(full, result.getExpr(Set.of()));
            assertSame(withoutPod, result.getExpr(Set.of("pod")));
            assertNull(result.getExpr(Set.of("region")));
            var narrowed = result.with(plan, select(result.header(), finite(Set.of("pod"))), result.value());
            assertNull(narrowed.getExpr(Set.of()));
            assertSame(withoutPod, narrowed.getExpr(Set.of("pod")));
            assertSame(result.step(), narrowed.step());
            assertEquals(kind, narrowed.kind());
        }
    }

    public void testColumnExpressionsMustBelongToThePlanOutput() {
        // An unbound header is a requirement, not a table.
        expectThrows(
            AssertionError.class,
            () -> new IntermediateResult(
                new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY),
                finite(List.of("cluster")),
                Literal.NULL,
                attr("step"),
                null,
                Kind.AFTER_INITIAL_AGGREGATE
            )
        );
        Attribute cluster = attr("cluster");
        Attribute packed = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var plan = new LocalRelation(Source.EMPTY, List.of(cluster, packed), EmptyLocalSupplier.EMPTY);
        for (Kind kind : Kind.values()) {
            // The name is the same, but this attribute's identity is not part of the plan's output.
            expectThrows(
                AssertionError.class,
                () -> new IntermediateResult(
                    plan,
                    new Header(Set.of("cluster"), Set.of(Set.of()), Map.of("cluster", attr("cluster"), mapOpen(Set.of()), packed)),
                    Literal.NULL,
                    attr("step"),
                    null,
                    kind
                )
            );
            expectThrows(
                AssertionError.class,
                () -> new IntermediateResult(
                    plan,
                    new Header(Set.of("cluster"), Set.of(Set.of()), Map.of("cluster", cluster, mapOpen(Set.of()), attr("_timeseries"))),
                    Literal.NULL,
                    attr("step"),
                    null,
                    kind
                )
            );
        }
    }
}
