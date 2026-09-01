/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class RequestFilterRewriterTests extends ESTestCase {

    private static final long NOW = 1_600_000_000_000L;
    private static final Configuration CONFIG = new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).now(Instant.ofEpochMilli(NOW)).build();
    private static final TransportVersion CURRENT = RequestFilterRewriter.ESQL_REQUEST_FILTER_ON_DATASET;
    private static final TransportVersion TOO_OLD = TransportVersion.minimumCompatible();

    private static ExternalRelation relation() {
        List<Attribute> output = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.INTEGER));
        SourceMetadata metadata = new SimpleSourceMetadata(output, "test", "file:///data.csv");
        return new ExternalRelation(Source.EMPTY, "file:///data.csv", metadata, output, FileList.UNRESOLVED, Map.of(), "ds");
    }

    public void testNullFilterLeavesPlanUnchanged() {
        ExternalRelation relation = relation();
        assertSame(relation, RequestFilterRewriter.rewrite(relation, null, true, CONFIG, CURRENT, false));
    }

    public void testSupportedFilterIsInstalledAboveTheRelation() {
        ExternalRelation relation = relation();
        LogicalPlan result = RequestFilterRewriter.rewrite(relation, QueryBuilders.termQuery("a", 1), true, CONFIG, CURRENT, false);
        assertThat(result, instanceOf(Filter.class));
        assertThat(((Filter) result).child(), sameInstance(relation));
    }

    /** Fail-closed: a wholly-unsupported filter fails the whole query with a 400 (VerificationException) listing the construct. */
    public void testWhollyUnsupportedFilterFailsTheQuery() {
        ExternalRelation relation = relation();
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> RequestFilterRewriter.rewrite(relation, QueryBuilders.wildcardQuery("a", "x*"), true, CONFIG, CURRENT, false)
        );
        assertThat(e.getMessage(), containsString("[wildcard]"));
    }

    /**
     * Fail-closed: a filter that mixes a supported term with an unsupported wildcard fails the whole query — the
     * supported clause does not rescue it, and no widened superset is silently applied.
     */
    public void testMixedFilterWithAnUnsupportedClauseFailsTheQuery() {
        ExternalRelation relation = relation();
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> RequestFilterRewriter.rewrite(
                relation,
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("a", 1)).must(QueryBuilders.wildcardQuery("a", "x*")),
                true,
                CONFIG,
                CURRENT,
                false
            )
        );
        assertThat(e.getMessage(), containsString("[wildcard]"));
    }

    /**
     * The feature gate: when off (a release build, where the feature flag is excluded) the relation is read
     * unfiltered and the user is told, rather than the filter being silently dropped.
     */
    public void testDisabledLeavesThePlanUnchangedAndWarns() {
        ExternalRelation relation = relation();
        LogicalPlan result = RequestFilterRewriter.rewrite(relation, QueryBuilders.termQuery("a", 1), false, CONFIG, CURRENT, false);
        assertSame(relation, result);
        assertWarnings(
            "The request filter was not applied to external dataset(s) [ds] because applying the request filter to "
                + "datasets is not enabled in this build; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from external datasets instead"
        );
    }

    /** Disabled short-circuits before translation, so even an unsupported construct cannot fail the query. */
    public void testDisabledDoesNotFailOnUnsupportedConstruct() {
        ExternalRelation relation = relation();
        LogicalPlan result = RequestFilterRewriter.rewrite(relation, QueryBuilders.wildcardQuery("a", "x*"), false, CONFIG, CURRENT, false);
        assertSame(relation, result);
        assertWarnings(
            "The request filter was not applied to external dataset(s) [ds] because applying the request filter to "
                + "datasets is not enabled in this build; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from external datasets instead"
        );
    }

    /** The critical version gate: below the feature version the rewrite is skipped, so no plan an old node can't read ships. */
    public void testOldMinimumVersionSkipsTheRewriteEntirely() {
        ExternalRelation relation = relation();
        LogicalPlan result = RequestFilterRewriter.rewrite(relation, QueryBuilders.termQuery("a", 1), true, CONFIG, TOO_OLD, false);
        assertSame(relation, result);
        assertWarnings(
            "The request filter was not applied to external dataset(s) [ds] because the cluster contains a node "
                + "too old to evaluate the translated filter; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from external datasets instead"
        );
    }

    private static ExternalRelation relation(String name, Attribute... attrs) {
        List<Attribute> output = List.of(attrs);
        String path = "file:///" + name + ".csv";
        SourceMetadata metadata = new SimpleSourceMetadata(output, "test", path);
        return new ExternalRelation(Source.EMPTY, path, metadata, output, FileList.UNRESOLVED, Map.of(), name);
    }

    private static ReferenceAttribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    /** Index leaves keep their pre-analysis request-filter path; the rewrite targets only dataset leaves. */
    public void testOnlyExternalRelationsAreTargeted() {
        EsRelation index = EsqlTestUtils.relation();
        ExternalRelation dataset = relation("ds", attr("a", DataType.INTEGER));
        UnionAll union = new UnionAll(Source.EMPTY, List.of(index, dataset), List.of());
        LogicalPlan result = RequestFilterRewriter.rewrite(union, QueryBuilders.termQuery("a", 1), true, CONFIG, CURRENT, false);

        Map<Boolean, LogicalPlan> children = new HashMap<>();
        ((UnionAll) result).children().forEach(c -> children.put(c instanceof Filter, c));
        assertThat("the dataset branch is wrapped in a Filter", ((Filter) children.get(true)).child(), sameInstance(dataset));
        assertThat("the index branch is left untouched", children.get(false), sameInstance(index));
    }

    /** Fail-closed: an unsupported clause under must_not fails the whole query too — the polarity does not matter. */
    public void testUnsupportedClauseUnderMustNotFailsTheQuery() {
        ExternalRelation relation = relation("ds", attr("a", DataType.INTEGER));
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> RequestFilterRewriter.rewrite(
                relation,
                QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery("a", "x*")),
                true,
                CONFIG,
                CURRENT,
                false
            )
        );
        assertThat(e.getMessage(), containsString("[wildcard]"));
    }

    /** The version-gate warning names every distinct dataset once. */
    public void testVersionGateWarningNamesAllDatasetsOnce() {
        UnionAll union = new UnionAll(
            Source.EMPTY,
            List.of(relation("dsA", attr("a", DataType.INTEGER)), relation("dsB", attr("a", DataType.INTEGER))),
            List.of()
        );
        LogicalPlan result = RequestFilterRewriter.rewrite(union, QueryBuilders.termQuery("a", 1), true, CONFIG, TOO_OLD, false);
        assertThat(result, sameInstance(union));
        assertWarnings(
            "The request filter was not applied to external dataset(s) [dsA, dsB] because the cluster contains a node "
                + "too old to evaluate the translated filter; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from external datasets instead"
        );
    }

    /** A dataset appearing more than once collapses to a single name in the version-gate warning. */
    public void testVersionGateWarningDeduplicatesRepeatedDatasetName() {
        UnionAll union = new UnionAll(
            Source.EMPTY,
            List.of(relation("ds", attr("a", DataType.INTEGER)), relation("ds", attr("a", DataType.INTEGER))),
            List.of()
        );
        RequestFilterRewriter.rewrite(union, QueryBuilders.termQuery("a", 1), true, CONFIG, TOO_OLD, false);
        assertWarnings(
            "The request filter was not applied to external dataset(s) [ds] because the cluster contains a node "
                + "too old to evaluate the translated filter; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from external datasets instead"
        );
    }

    /** No datasets in the plan -> the version gate is silent (nothing to warn about). */
    public void testVersionGateOnPlanWithoutDatasetsStaysSilent() {
        EsRelation index = EsqlTestUtils.relation();
        LogicalPlan result = RequestFilterRewriter.rewrite(index, QueryBuilders.termQuery("a", 1), true, CONFIG, TOO_OLD, false);
        assertThat(result, sameInstance(index));
        // no assertWarnings: the datasets.isEmpty() guard means nothing is emitted
    }

    /** When a dataset has no name, the version-gate warning falls back to its source path. */
    public void testWarningFallsBackToSourcePathWhenDatasetNameIsNull() {
        List<Attribute> output = List.of(attr("a", DataType.INTEGER));
        SourceMetadata metadata = new SimpleSourceMetadata(output, "test", "file:///data.csv");
        ExternalRelation relation = new ExternalRelation(
            Source.EMPTY,
            "file:///data.csv",
            metadata,
            output,
            FileList.UNRESOLVED,
            Map.of(),
            null
        );
        RequestFilterRewriter.rewrite(relation, QueryBuilders.termQuery("a", 1), true, CONFIG, TOO_OLD, false);
        assertWarnings(
            "The request filter was not applied to external dataset(s) [file:///data.csv] because the cluster contains a node "
                + "too old to evaluate the translated filter; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from external datasets instead"
        );
    }

    /** Fail-closed: an unsupported clause fails the query even when several datasets are queried together, listing all. */
    public void testUnsupportedClauseFailsTheQueryAcrossDatasets() {
        UnionAll union = new UnionAll(
            Source.EMPTY,
            List.of(relation("dsA", attr("a", DataType.INTEGER)), relation("dsB", attr("a", DataType.INTEGER))),
            List.of()
        );
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> RequestFilterRewriter.rewrite(union, QueryBuilders.wildcardQuery("a", "x*"), true, CONFIG, CURRENT, false)
        );
        assertThat(e.getMessage(), containsString("[wildcard]"));
        // Each dataset produces one failure entry — both named in the error.
        assertThat(e.getMessage(), containsString("dsA"));
        assertThat(e.getMessage(), containsString("dsB"));
    }

    /**
     * The headline: heterogeneous datasets queried together each get their OWN filter, bound against their OWN schema.
     * A field present on one dataset and absent on another binds to the real attribute on the first and to NULL
     * (runtime-false, index-consistent) on the second — silently, with no warning, because a missing field is leniency,
     * not a dropped clause.
     */
    public void testHeterogeneousDatasetsEachGetTheirOwnBoundFilter() {
        ExternalRelation dsA = relation(
            "dsA",
            attr("id", DataType.INTEGER),
            attr("status", DataType.INTEGER),
            attr("region", DataType.KEYWORD)
        );
        ExternalRelation dsB = relation("dsB", attr("id", DataType.INTEGER), attr("status", DataType.INTEGER)); // no region
        UnionAll union = new UnionAll(Source.EMPTY, List.of(dsA, dsB), List.of());
        LogicalPlan result = RequestFilterRewriter.rewrite(
            union,
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("region", "eu"))
                .must(QueryBuilders.rangeQuery("status").gte(200).lte(400)),
            true,
            CONFIG,
            CURRENT,
            false
        );

        Map<String, Filter> byDataset = new HashMap<>();
        result.forEachDown(Filter.class, f -> byDataset.put(((ExternalRelation) f.child()).datasetName(), f));

        assertThat(
            "dsA binds both region and status",
            byDataset.get("dsA").condition().references().names(),
            containsInAnyOrder("region", "status")
        );
        assertThat(
            "dsB lacks region -> only status is bound (region is NULL, runtime-false)",
            byDataset.get("dsB").condition().references().names(),
            contains("status")
        );
        // No assertWarnings call: a missing field is index-consistent leniency, so nothing is dropped and nothing warns.
    }

    /**
     * Partial mode: an unsupported top-level clause is dropped with a warning instead of failing the query.
     * The supported conjuncts are applied.
     */
    public void testPartialModeDropsUnsupportedClauseWithWarning() {
        ExternalRelation relation = relation("ds", attr("a", DataType.INTEGER));
        LogicalPlan result = RequestFilterRewriter.rewrite(
            relation,
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("a", 1)).must(QueryBuilders.wildcardQuery("a", "x*")),
            true,
            CONFIG,
            CURRENT,
            true
        );
        // The plan should have a Filter (from the supported term clause) rather than failing.
        assertThat(result, instanceOf(Filter.class));
        // A single warning must be emitted naming the dropped construct.
        assertWarnings(true, List.of(containsString("[wildcard]")));
    }

    /**
     * Partial mode: a wholly unsupported filter leaves the plan unchanged (no filter installed)
     * and emits a warning — it does not fail the query.
     */
    public void testPartialModeWhollyUnsupportedLeavesNodeUnwrappedWithWarning() {
        ExternalRelation relation = relation("ds", attr("a", DataType.INTEGER));
        LogicalPlan result = RequestFilterRewriter.rewrite(relation, QueryBuilders.wildcardQuery("a", "x*"), true, CONFIG, CURRENT, true);
        assertThat("no filter installed when no conjuncts translated", result, sameInstance(relation));
        assertWarnings(true, List.of(containsString("[wildcard]")));
    }

    /**
     * Partial mode with multiple datasets: unsupported clauses from each dataset are all reported in a single warning.
     */
    public void testPartialModeMultipleDatasetsWarnsAll() {
        UnionAll union = new UnionAll(
            Source.EMPTY,
            List.of(relation("dsA", attr("a", DataType.INTEGER)), relation("dsB", attr("a", DataType.INTEGER))),
            List.of()
        );
        RequestFilterRewriter.rewrite(union, QueryBuilders.wildcardQuery("a", "x*"), true, CONFIG, CURRENT, true);
        // Both datasets named in a single warning.
        assertWarnings(true, List.of(allOf(containsString("dsA"), containsString("dsB"), containsString("[wildcard]"))));
    }
}
