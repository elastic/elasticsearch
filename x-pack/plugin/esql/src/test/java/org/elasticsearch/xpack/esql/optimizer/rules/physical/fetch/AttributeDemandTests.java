/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Runs {@link AttributeDemand} on real distributed plans, as {@code PlanFetch} sees them after {@code ProjectAwayColumns}.
 */
public class AttributeDemandTests extends ESTestCase {

    public void testSortKeyKept() {
        assertDemand("FROM idx | WHERE x > 1 | SORT ts DESC | LIMIT 10 | KEEP a, b, ts", List.of("ts"), List.of("a", "b"), List.of("x"));
    }

    /** The cut reads the sort key, so it crosses the exchange even though the query does not return it. */
    public void testSortKeyNotKept() {
        assertDemand("FROM idx | SORT ts | LIMIT 10 | KEEP a", List.of("ts"), List.of("a"), List.of());
    }

    public void testPlainLimitDefersEverything() {
        assertDemand("FROM idx | LIMIT 10 | KEEP a, b", List.of(), List.of("a", "b"), List.of());
    }

    /** A value computed inside the fragment crosses as a value. Its input is read inside the fragment only. */
    public void testEvalBeforeTheCut() {
        assertDemand("FROM idx | EVAL z = a + 1 | SORT ts | LIMIT 10 | KEEP z, b", List.of("ts", "z"), List.of("b"), List.of("a"));
    }

    /** A filter after the cut reads its field after the cut, so the field is loaded for the surviving rows only. */
    public void testFilterAfterTheCut() {
        assertDemand("FROM idx | SORT ts | LIMIT 10 | WHERE a > 0 | KEEP b", List.of("ts"), List.of("a", "b"), List.of());
    }

    public void testRenameAfterTheCut() {
        assertDemand("FROM idx | SORT ts | LIMIT 10 | RENAME a AS renamed | KEEP renamed", List.of("ts"), List.of("a"), List.of());
    }

    /** {@code _score} is computed by the query and cannot be loaded from a document, it stays eager. */
    public void testScoreStaysEager() {
        assertDemand(
            "FROM idx METADATA _score | WHERE MATCH(kw, \"q\") | SORT _score DESC | LIMIT 10 | KEEP a, _score",
            List.of("_score"),
            List.of("a"),
            List.of("kw")
        );
    }

    public void testSource() {
        assertDemand("FROM idx METADATA _source | SORT ts | LIMIT 10 | KEEP _source", List.of("ts"), List.of("_source"), List.of());
    }

    private static void assertDemand(String query, List<String> eager, List<String> deferred, List<String> local) {
        PhysicalPlan plan = plan(query);
        List<ExchangeExec> exchanges = plan.collect(ExchangeExec.class);
        assertThat(plan.toString(), exchanges, hasSize(1));
        ExchangeExec exchange = exchanges.getFirst();
        FragmentExec fragment = (FragmentExec) exchange.child();
        assertThat(fragment.fragment(), instanceOf(Project.class));
        Project fragmentRoot = (Project) fragment.fragment();
        EsRelation relation = fragmentRoot.collect(EsRelation.class).getFirst();
        PhysicalPlan cut = parentOf(plan, exchange);

        AttributeDemand.Demand demand = AttributeDemand.analyze(fragmentRoot, relation, cut);
        assertThat("eager", names(demand.eager()), equalTo(eager));
        assertThat("deferred", names(demand.deferred()), equalTo(deferred));
        assertThat("local", names(demand.local()), equalTo(local));
    }

    private static PhysicalPlan parentOf(PhysicalPlan plan, PhysicalPlan child) {
        return plan.collect(p -> p.children().contains(child)).getFirst();
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).sorted().toList();
    }

    static PhysicalPlan plan(String query) {
        Analyzer analyzer = EsqlTestUtils.analyzer()
            .addIndex(EsIndexGenerator.esIndex("idx", mapping(), Map.of("idx", IndexMode.STANDARD)))
            .minimumTransportVersion(TransportVersion.current())
            .buildAnalyzer();
        return new TestPlannerOptimizer(EsqlTestUtils.TEST_CFG, analyzer, EsqlFlags.DEFAULTS).distributedPlan(query);
    }

    private static Map<String, EsField> mapping() {
        return Map.of(
            "a",
            new EsField("a", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "b",
            new EsField("b", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "kw",
            new EsField("kw", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "ts",
            new EsField("ts", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "x",
            new EsField("x", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }
}
