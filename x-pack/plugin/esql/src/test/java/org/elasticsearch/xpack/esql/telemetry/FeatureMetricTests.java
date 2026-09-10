/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link FeatureMetric#set} throws for any plan node it cannot map and that is not on the exclusion list. Since the
 * exclusion list matches the {@code UnresolvedSourceRelation} marker, both {@code FROM}-style leaf shapes must be
 * excluded together — neither should raise. This pins the "telemetered together (i.e. not at all)" decision for the
 * two unresolved source relations.
 */
public class FeatureMetricTests extends ESTestCase {

    public void testIndexRelationIsExcluded() {
        UnresolvedRelation index = new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, "idx"),
            false,
            List.of(),
            IndexMode.STANDARD,
            null
        );
        // No throw == excluded as expected; an excluded node must not flip any feature bit.
        BitSet bitset = new BitSet();
        FeatureMetric.set(index, bitset);
        assertTrue("excluded plans must not set any telemetry bit", bitset.isEmpty());
    }

    public void testExternalRelationIsExcluded() {
        UnresolvedExternalRelation external = new UnresolvedExternalRelation(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "s3://bucket/table"),
            Map.of()
        );
        // The external shape must be excluded via the shared marker, exactly like the index shape; a regression that
        // dropped it from the exclusion list would surface here as an EsqlIllegalArgumentException.
        BitSet bitset = new BitSet();
        FeatureMetric.set(external, bitset);
        assertTrue("excluded plans must not set any telemetry bit", bitset.isEmpty());
    }

    public void testSourceFanInDoesNotSetForkBit() {
        UnresolvedRelation first = relation("ds1");
        UnresolvedRelation second = relation("ds2");
        SourceFanInUnionAll fanIn = new SourceFanInUnionAll(Source.EMPTY, List.of(first, second), List.of());

        BitSet bitset = new BitSet();
        FeatureMetric.set(fanIn, bitset);
        assertTrue(bitset.isEmpty());
        assertFalse(bitset.get(FeatureMetric.FORK.ordinal()));
    }

    public void testOrdinaryForkSetsForkBit() {
        Fork fork = new Fork(Source.EMPTY, List.of(relation("a"), relation("b")), List.of());
        BitSet bitset = new BitSet();
        FeatureMetric.set(fork, bitset);
        assertTrue(bitset.get(FeatureMetric.FORK.ordinal()));
    }

    public void testPlainUnionAllSetsForkBit() {
        UnionAll union = new UnionAll(Source.EMPTY, List.of(relation("a"), relation("b")), List.of());
        BitSet bitset = new BitSet();
        FeatureMetric.set(union, bitset);
        assertTrue(bitset.get(FeatureMetric.FORK.ordinal()));
    }

    public void testViewUnionAllKeepsForkClassification() {
        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("v1", relation("a"));
        children.put("v2", relation("b"));
        ViewUnionAll viewUnion = new ViewUnionAll(Source.EMPTY, children, List.of());

        BitSet bitset = new BitSet();
        FeatureMetric.set(viewUnion, bitset);
        assertTrue(bitset.get(FeatureMetric.FORK.ordinal()));
    }

    public void testProvisionalSourceFanInKeepsForkClassification() {
        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("v1", relation("a"));
        children.put("v2", relation("b"));
        SourceFanInUnionAll provisional = SourceFanInUnionAll.provisional(Source.EMPTY, children, List.of());

        BitSet bitset = new BitSet();
        FeatureMetric.set(provisional, bitset);
        assertTrue(bitset.get(FeatureMetric.FORK.ordinal()));
    }

    private static UnresolvedRelation relation(String name) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, name), false, List.of(), IndexMode.STANDARD, null);
    }
}
