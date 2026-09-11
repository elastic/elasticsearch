/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class SourceExpansionNormalizerTests extends ESTestCase {

    public void testNoProvisionalReturnsSamePlan() {
        LogicalPlan plan = relation("idx");
        assertThat(SourceExpansionNormalizer.normalize(plan), sameInstance(plan));
    }

    public void testIndexOnlyCandidateRestoresViewUnionAll() {
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("view_a", relation("idx"));
        branches.put("view_b", relation("idx"));
        SourceFanInUnionAll candidate = SourceFanInUnionAll.provisional(Source.EMPTY, branches, List.of());

        LogicalPlan normalized = SourceExpansionNormalizer.normalize(candidate);
        assertThat(normalized, instanceOf(ViewUnionAll.class));
        ViewUnionAll restored = (ViewUnionAll) normalized;
        assertThat(restored.namedSubqueries().keySet().toArray(), equalTo(new Object[] { "view_a", "view_b" }));
        assertThat(restored.children(), hasSize(2));
        assertFalse(normalized.anyMatch(p -> p instanceof SourceFanInUnionAll));
    }

    public void testIndexOnlyCandidatePreservesNullBranchKey() {
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put(null, relation("idx_a"));
        branches.put("view", relation("idx_b"));
        SourceFanInUnionAll candidate = SourceFanInUnionAll.provisional(Source.EMPTY, branches, List.of());

        LogicalPlan normalized = SourceExpansionNormalizer.normalize(candidate);
        ViewUnionAll restored = (ViewUnionAll) normalized;
        assertTrue(restored.namedSubqueries().containsKey(null));
        assertThat(restored.namedSubqueries().get("view"), instanceOf(UnresolvedRelation.class));
    }

    public void testIndexOnlyCandidateWithShadowRestoresShadow() {
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("view", relation("idx"));
        branches.put("shadow", new ViewShadowRelation(Source.EMPTY, "idx", LinkedIndexPattern.Kind.OPTIONAL, "idx"));
        SourceFanInUnionAll candidate = SourceFanInUnionAll.provisional(Source.EMPTY, branches, List.of());

        LogicalPlan normalized = SourceExpansionNormalizer.normalize(candidate);
        ViewUnionAll restored = (ViewUnionAll) normalized;
        assertThat(restored.children(), hasSize(2));
        assertTrue(restored.children().stream().anyMatch(c -> c instanceof ViewShadowRelation));
        assertFalse(normalized.anyMatch(p -> p instanceof SourceFanInUnionAll));
    }

    public void testExternalCandidateWithoutNestedFanInRestoresViewUnionAll() {
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("left", external("s3://a/"));
        branches.put("right", external("s3://b/"));
        SourceFanInUnionAll candidate = SourceFanInUnionAll.provisional(Source.EMPTY, branches, List.of());

        LogicalPlan normalized = SourceExpansionNormalizer.normalize(candidate);
        assertThat(normalized, instanceOf(ViewUnionAll.class));
        ViewUnionAll restored = (ViewUnionAll) normalized;
        assertThat(restored.children(), hasSize(2));
        assertFalse(normalized.anyMatch(p -> p instanceof SourceFanInUnionAll));
    }

    public void testMixedNestedGroupWithoutFinalFanInRestoresViewUnionAll() {
        LinkedHashMap<String, LogicalPlan> indexOnly = new LinkedHashMap<>();
        indexOnly.put("first", relation("idx"));
        indexOnly.put("second", relation("idx"));
        SourceFanInUnionAll nestedIndex = SourceFanInUnionAll.provisional(Source.EMPTY, indexOnly, List.of());

        LinkedHashMap<String, LogicalPlan> outer = new LinkedHashMap<>();
        outer.put("indexes", nestedIndex);
        outer.put("dataset", external("s3://ds/"));
        SourceFanInUnionAll candidate = SourceFanInUnionAll.provisional(Source.EMPTY, outer, List.of());

        LogicalPlan normalized = SourceExpansionNormalizer.normalize(candidate);
        assertThat(normalized, instanceOf(ViewUnionAll.class));
        assertTrue(normalized.anyMatch(p -> p instanceof UnresolvedExternalRelation));
        assertFalse(normalized.anyMatch(p -> p instanceof SourceFanInUnionAll fanIn && fanIn.isProvisional() == false));
    }

    public void testNestedFinalFanInPlusDatasetFlattens() {
        SourceFanInUnionAll nested = new SourceFanInUnionAll(Source.EMPTY, List.of(external("s3://a/"), external("s3://b/")), List.of());
        LinkedHashMap<String, LogicalPlan> outer = new LinkedHashMap<>();
        outer.put("fan", nested);
        outer.put("extra", external("s3://c/"));
        SourceFanInUnionAll candidate = SourceFanInUnionAll.provisional(Source.EMPTY, outer, List.of());

        LogicalPlan normalized = SourceExpansionNormalizer.normalize(candidate);
        assertThat(normalized, instanceOf(SourceFanInUnionAll.class));
        SourceFanInUnionAll fanIn = (SourceFanInUnionAll) normalized;
        assertFalse(fanIn.isProvisional());
        assertThat(fanIn.children(), hasSize(3));
    }

    public void testRelationalWrapperIsNotPromoted() {
        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("fan", new SourceFanInUnionAll(Source.EMPTY, List.of(external("s3://a/"), external("s3://b/")), List.of()));
        children.put("pipe", new Filter(Source.EMPTY, relation("idx"), Literal.TRUE));
        ViewUnionAll relational = new ViewUnionAll(Source.EMPTY, children, List.of());

        LogicalPlan normalized = SourceExpansionNormalizer.normalize(relational);
        assertSame(relational, normalized);
        assertThat(normalized, instanceOf(ViewUnionAll.class));
    }

    public void testNormalizeIsIdempotent() {
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("a", relation("idx"));
        branches.put("b", relation("other"));
        LogicalPlan first = SourceExpansionNormalizer.normalize(SourceFanInUnionAll.provisional(Source.EMPTY, branches, List.of()));
        LogicalPlan second = SourceExpansionNormalizer.normalize(first);
        assertThat(second, sameInstance(first));
    }

    public void testSingleSurvivorCollapses() {
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("only", relation("idx"));
        SourceFanInUnionAll candidate = new SourceFanInUnionAll(Source.EMPTY, List.of(relation("idx")), List.of(), List.of("only"));

        LogicalPlan normalized = SourceExpansionNormalizer.normalize(candidate);
        assertThat(normalized, instanceOf(UnresolvedRelation.class));
        assertThat(((UnresolvedRelation) normalized).indexPattern().indexPattern(), equalTo("idx"));
    }

    private static UnresolvedRelation relation(String name) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, name), false, List.of(), IndexMode.STANDARD, null);
    }

    private static UnresolvedExternalRelation external(String table) {
        return new UnresolvedExternalRelation(Source.EMPTY, Literal.keyword(Source.EMPTY, table), Map.of());
    }
}
