/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.dsltranslate.QueryDslTranslator.DroppedClause;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for the source-agnostic {@link FilterRewriter} mechanism — the generic "install a bound {@code Filter} at any
 * node the target predicate selects" primitive, exercised independently of the request-filter policy (no
 * {@code HeaderWarning}, dropped clauses collected through the {@code onDropped} callback). {@link RequestFilterRewriterTests}
 * covers the policy on top of this (targeting {@code ExternalRelation}, version-gating, warning wording).
 */
public class FilterRewriterTests extends ESTestCase {

    private static final long NOW = 1_600_000_000_000L;
    private static final BiConsumer<LogicalPlan, DroppedClause> IGNORE_DROPS = (node, clause) -> {};

    private static ReferenceAttribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static ExternalRelation relation(String name, Attribute... attrs) {
        List<Attribute> output = List.of(attrs);
        SourceMetadata metadata = new SimpleSourceMetadata(output, "test", "file:///" + name + ".csv");
        return new ExternalRelation(Source.EMPTY, "file:///" + name + ".csv", metadata, output, FileList.UNRESOLVED, Map.of(), name);
    }

    // --- genericity: the target predicate is the API, nothing here knows about datasets/sources ---

    public void testInstallsAboveAnArbitraryNonSourceNode() {
        ExternalRelation rel = relation("ds", attr("a", DataType.INTEGER));
        Limit limit = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 10, DataType.INTEGER), rel);
        LogicalPlan result = FilterRewriter.rewrite(limit, node -> node == limit, QueryBuilders.termQuery("a", 1), NOW, IGNORE_DROPS);
        assertThat(result, instanceOf(Filter.class));
        Filter filter = (Filter) result;
        assertThat("installs above the chosen node, not the leaf", filter.child(), sameInstance(limit));
        assertThat(filter.condition().references().names(), contains("a"));
    }

    public void testBindingIsRelativeToTheTargetNodesOutputNotTheLeafs() {
        ReferenceAttribute a = attr("a", DataType.INTEGER);
        ReferenceAttribute x = attr("x", DataType.KEYWORD);
        ExternalRelation rel = relation("ds", a, x);
        // Project drops x from its output; installing above the Project must bind x to NULL (absent from THAT node).
        Project project = new Project(Source.EMPTY, rel, List.of(a));
        LogicalPlan aboveProject = FilterRewriter.rewrite(
            project,
            node -> node == project,
            QueryBuilders.termQuery("x", "v"),
            NOW,
            IGNORE_DROPS
        );
        assertThat(
            "x is absent from the Project's output -> NULL-bound -> no references",
            ((Filter) aboveProject).condition().references().names(),
            empty()
        );

        // Installing above the relation, where x is present, binds the real attribute.
        LogicalPlan aboveRelation = FilterRewriter.rewrite(rel, node -> node == rel, QueryBuilders.termQuery("x", "v"), NOW, IGNORE_DROPS);
        assertThat(((Filter) aboveRelation).condition().references().names(), contains("x"));
    }

    public void testPredicateMatchingNothingReturnsThePlanUntouched() {
        ExternalRelation rel = relation("ds", attr("a", DataType.INTEGER));
        List<DroppedClause> drops = new ArrayList<>();
        LogicalPlan result = FilterRewriter.rewrite(rel, node -> false, QueryBuilders.termQuery("a", 1), NOW, (n, c) -> drops.add(c));
        assertThat(result, sameInstance(rel));
        assertThat(drops, empty());
    }

    public void testEveryMatchingNodeIsWrappedOnce() {
        ExternalRelation relA = relation("dsA", attr("id", DataType.INTEGER));
        ExternalRelation relB = relation("dsB", attr("id", DataType.INTEGER));
        UnionAll union = new UnionAll(Source.EMPTY, List.of(relA, relB), List.of());
        LogicalPlan result = FilterRewriter.rewrite(
            union,
            ExternalRelation.class::isInstance,
            QueryBuilders.termQuery("id", 1),
            NOW,
            IGNORE_DROPS
        );
        List<Filter> filters = new ArrayList<>();
        result.forEachDown(Filter.class, filters::add);
        assertThat("one Filter per matching leaf", filters, hasSize(2));
        // The fresh Filter nodes are not re-matched by transformUp, so no Filter wraps a Filter.
        filters.forEach(f -> assertThat(f.child(), instanceOf(ExternalRelation.class)));
    }

    // --- per-node schema binding and the onDropped callback ---

    public void testMultipleTargetsEachBindTheirOwnSchema() {
        ExternalRelation relA = relation("dsA", attr("id", DataType.INTEGER), attr("region", DataType.KEYWORD));
        ExternalRelation relB = relation("dsB", attr("id", DataType.INTEGER)); // no region
        UnionAll union = new UnionAll(Source.EMPTY, List.of(relA, relB), List.of());
        List<DroppedClause> drops = new ArrayList<>();
        LogicalPlan result = FilterRewriter.rewrite(
            union,
            ExternalRelation.class::isInstance,
            QueryBuilders.termQuery("region", "eu"),
            NOW,
            (n, c) -> drops.add(c)
        );
        Map<String, Filter> byDataset = new HashMap<>();
        result.forEachDown(Filter.class, f -> byDataset.put(((ExternalRelation) f.child()).datasetName(), f));

        assertThat(
            "dsA has region -> bound to the real attribute",
            byDataset.get("dsA").condition().references().names(),
            contains("region")
        );
        assertThat(
            "dsB lacks region -> NULL-bound (runtime-false), still installed",
            byDataset.get("dsB").condition().references().names(),
            empty()
        );
        assertThat("a missing field is leniency, not a dropped clause -> no warning", drops, empty());
    }

    public void testOnDroppedFiresPerClausePerNode() {
        ExternalRelation relKeyword = relation("dsKw", attr("status", DataType.KEYWORD));
        ExternalRelation relInteger = relation("dsInt", attr("status", DataType.INTEGER));
        UnionAll union = new UnionAll(Source.EMPTY, List.of(relKeyword, relInteger), List.of());
        Map<String, List<DroppedClause>> byDataset = new HashMap<>();
        FilterRewriter.rewrite(
            union,
            ExternalRelation.class::isInstance,
            QueryBuilders.termQuery("status", "active"), // valid keyword, but not an integral value
            NOW,
            (node, clause) -> byDataset.computeIfAbsent(((ExternalRelation) node).datasetName(), k -> new ArrayList<>()).add(clause)
        );
        assertThat("keyword status applies exactly, nothing dropped", byDataset.getOrDefault("dsKw", List.of()), empty());
        assertThat("a non-integral value on an integer field is dropped for THAT node only", byDataset.get("dsInt"), hasSize(1));
    }

    // --- fold-to-no-op vs installed-false ---

    public void testWhollyUnsupportedFilterLeavesNodeUnwrapped() {
        ExternalRelation rel = relation("ds", attr("a", DataType.INTEGER));
        List<DroppedClause> drops = new ArrayList<>();
        LogicalPlan result = FilterRewriter.rewrite(
            rel,
            ExternalRelation.class::isInstance,
            QueryBuilders.wildcardQuery("a", "x*"),
            NOW,
            (n, c) -> drops.add(c)
        );
        assertThat("folds to the TRUE singleton -> node left unwrapped", result, sameInstance(rel));
        assertThat(drops, hasSize(1));
    }

    public void testMatchAllLeavesNodeUnwrapped() {
        ExternalRelation rel = relation("ds", attr("a", DataType.INTEGER));
        List<DroppedClause> drops = new ArrayList<>();
        LogicalPlan result = FilterRewriter.rewrite(
            rel,
            ExternalRelation.class::isInstance,
            QueryBuilders.matchAllQuery(),
            NOW,
            (n, c) -> drops.add(c)
        );
        assertThat(result, sameInstance(rel));
        assertThat("match_all is a supported no-op, not a drop", drops, empty());
    }

    public void testMatchNoneInstallsAFalseFilter() {
        ExternalRelation rel = relation("ds", attr("a", DataType.INTEGER));
        LogicalPlan result = FilterRewriter.rewrite(
            rel,
            ExternalRelation.class::isInstance,
            new MatchNoneQueryBuilder(),
            NOW,
            IGNORE_DROPS
        );
        assertThat(result, instanceOf(Filter.class));
        assertThat(
            "match_none is an exact FALSE, installed (not folded to a no-op)",
            ((Filter) result).condition(),
            sameInstance(Literal.FALSE)
        );
    }

    public void testMustNotDropIsRecordedWithNegativePolarity() {
        ExternalRelation rel = relation("ds", attr("a", DataType.INTEGER));
        List<DroppedClause> drops = new ArrayList<>();
        FilterRewriter.rewrite(
            rel,
            ExternalRelation.class::isInstance,
            QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery("a", "x*")),
            NOW,
            (n, c) -> drops.add(c)
        );
        assertThat(drops, hasSize(1));
        assertThat("a drop under must_not relaxes an exclusion -> previously-excluded rows", drops.get(0).positive(), is(false));
    }

    // --- nowInMillis threading and analyzed-marking ---

    public void testNowInMillisIsThreadedIntoTheTranslation() {
        ExternalRelation rel = relation("ds", attr("ts", DataType.DATETIME));
        QueryBuilder filter = QueryBuilders.rangeQuery("ts").gte("now-15m");
        Filter early = (Filter) FilterRewriter.rewrite(rel, ExternalRelation.class::isInstance, filter, NOW, IGNORE_DROPS);
        Filter later = (Filter) FilterRewriter.rewrite(rel, ExternalRelation.class::isInstance, filter, NOW + 3_600_000L, IGNORE_DROPS);
        assertThat("different query start times resolve now-math to different bounds", early.condition(), not(equalTo(later.condition())));
    }

    public void testInstalledTreeIsMarkedAnalyzed() {
        ExternalRelation rel = relation("ds", attr("a", DataType.INTEGER));
        LogicalPlan result = FilterRewriter.rewrite(
            rel,
            ExternalRelation.class::isInstance,
            QueryBuilders.termQuery("a", 1),
            NOW,
            IGNORE_DROPS
        );
        result.forEachDown(LogicalPlan.class, node -> assertTrue("every node incl. the fresh Filter is marked analyzed", node.analyzed()));
    }
}
