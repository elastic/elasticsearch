/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

/**
 * Plan shape of PromQL {@code absent}: every evaluation step of the range, anti-joined against the steps where the argument
 * has an element. Assertions are on the analyzed plan, because the optimizer replaces an anti join whose right side is known
 * to be empty by its left side.
 */
public class PromqlPlanAbsentTests extends AbstractPromqlPlanOptimizerTests {

    private static final String RANGE = "start=\"2024-05-10T00:00:00.000Z\" end=\"2024-05-10T00:30:00.000Z\" step=5m";

    public PromqlPlanAbsentTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void assumeAbsentEnabled() {
        assumeTrue("Requires PROMQL_ABSENT capability", EsqlCapabilities.Cap.PROMQL_ABSENT.isEnabled());
    }

    public void testAntiJoinsStepsAgainstPresence() {
        LogicalPlan plan = analyze("PROMQL index=k8s " + RANGE + " result=(absent(network.cost{pod=\"nope\"}))");
        assertThat(outputColumns(plan), contains("result", "step", "pod"));

        AntiJoin antiJoin = singleAntiJoin(plan);
        assertThat(antiJoin.left().collect(MvExpand.class), hasSize(1));
        assertThat(antiJoin.left().collect(Row.class), hasSize(1));
        Aggregate presence = antiJoin.right().collect(Aggregate.class).getFirst();
        assertThat(presence.groupings().stream().map(Expressions::name).toList(), contains("step"));
    }

    public void testNonSelectorArgumentHasNoLabels() {
        LogicalPlan plan = analyze("PROMQL index=k8s " + RANGE + " result=(absent(sum by (pod) (network.cost{pod=\"nope\"})))");
        assertThat(outputColumns(plan), contains("result", "step"));
        singleAntiJoin(plan);
    }

    public void testStepKeyTypesMatchOnDateNanos() {
        LogicalPlan plan = analyze("PROMQL index=datenanos-k8s " + RANGE + " result=(absent(network.cost{pod=\"nope\"}))");
        AntiJoin antiJoin = singleAntiJoin(plan);
        assertEquals(antiJoin.config().leftFields().getFirst().dataType(), antiJoin.config().rightFields().getFirst().dataType());
    }

    public void testInstantQuery() {
        singleAntiJoin(analyze("PROMQL index=k8s time=\"2024-05-10T00:10:00.000Z\" result=(absent(network.cost{pod=\"nope\"}))"));
    }

    public void testOffset() {
        singleAntiJoin(analyze("PROMQL index=k8s " + RANGE + " result=(absent(network.cost{pod=\"nope\"} offset 10m))"));
    }

    public void testByGroupingKeepsDerivedLabel() {
        LogicalPlan plan = analyze("PROMQL index=k8s " + RANGE + " result=(sum by (pod) (absent(network.cost{pod=\"nope\"})))");
        assertThat(outputColumns(plan), contains("result", "step", "pod"));
    }

    public void testRelabelOverwritingDerivedLabel() {
        LogicalPlan plan = analyze(
            "PROMQL index=k8s " + RANGE + " result=(label_replace(absent(network.cost{pod=\"a\"}), \"pod\", \"b\", \"pod\", \"(.*)\"))"
        );
        assertThat(outputColumns(plan), contains("result", "step", "pod"));
    }

    /** An argument that reads no source data has an element at every step, so its {@code absent} is empty. */
    public void testArgumentWithoutSourceDataIsEmpty() {
        for (String argument : List.of("vector(1)", "vector(scalar(network.cost))", "scalar(network.cost) * vector(1)")) {
            LogicalPlan plan = analyze("PROMQL index=k8s " + RANGE + " result=(absent(" + argument + "))");
            assertTrue(argument, plan.collect(AntiJoin.class).isEmpty());
            assertFalse(argument, plan.collect(LocalRelation.class).isEmpty());
            assertThat(argument, outputColumns(plan), contains("result", "step"));
        }
    }

    /** The inner {@code absent} is empty at every step, so the outer one keeps every step. */
    public void testNestedAbsentOfConstant() {
        singleAntiJoin(analyze("PROMQL index=k8s " + RANGE + " result=(absent(absent(vector(1))))"));
    }

    private LogicalPlan analyze(String query) {
        return tsAnalyzer().query(query);
    }

    private static AntiJoin singleAntiJoin(LogicalPlan plan) {
        List<AntiJoin> antiJoins = plan.collect(AntiJoin.class);
        assertThat(antiJoins, hasSize(1));
        return antiJoins.getFirst();
    }

    private static List<String> outputColumns(LogicalPlan plan) {
        return plan.output().stream().map(Attribute::name).toList();
    }
}
