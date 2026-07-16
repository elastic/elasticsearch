/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.FillNull;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Logical-optimizer coverage for the {@code FILLNULL} command, whose {@link FillNull} node is rewritten away by
 * {@link SubstituteSurrogatePlans} into a {@code Project} over an {@code Eval} of {@code Coalesce} aliases, and whose
 * un-fillable targets are surfaced as response-header warnings by {@link WarnUnfillableFillNull}.
 */
public class SubstituteSurrogatePlansFillNullCommandTests extends AbstractLogicalPlanOptimizerTests {

    public void testFillNullChainedWithFork() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW a = null, b = null
            | EVAL a = a::keyword, b = b::integer
            | FILLNULL WITH "unknown" a
            | FILLNULL WITH 0 b
            | FORK (WHERE true | LIMIT 300) (WHERE true)
            | LIMIT 300
            | WHERE _fork == "fork1"
            | DROP _fork
            """);
        assertThat(plan, instanceOf(Project.class));
    }

    public void testFillNullChained() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW a = null, b = null
            | EVAL a = a::keyword, b = b::integer
            | FILLNULL WITH "unknown" a
            | FILLNULL WITH 0 b
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertThat(Expressions.names(plan.output()), containsInAnyOrder("a", "b"));
    }

    public void testFillNullBetweenEvalsIsCombined() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW a = null, x = 1
            | EVAL a = a::integer
            | FILLNULL WITH 0 a
            | EVAL sum = a + x
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertThat(Expressions.names(plan.output()), containsInAnyOrder("a", "x", "sum"));
    }

    public void testFillNullUnfillableTargetedFieldWarns() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW d = null
            | EVAL d = d::datetime
            | FILLNULL d
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertWarnings(
            "Line 3:3: [FILLNULL] field [d] of type [datetime] has no default fill value and was left unchanged; "
                + "provide a value using WITH"
        );
    }

    public void testFillNullAllFieldsUnfillableColumnsWarnOnce() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW d = null, a = null, i = null
            | EVAL d = d::datetime, a = a::ip, i = i::integer
            | FILLNULL
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertWarnings(
            "Line 3:3: [FILLNULL] the following fields have no default fill value for their type and were left "
                + "unchanged: [d, a]; provide a value using WITH"
        );
    }

    public void testFillNullAllFieldsUnfillableColumnsSummaryIsCapped() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW d1 = null, d2 = null, d3 = null, d4 = null, d5 = null, d6 = null, d7 = null, d8 = null, d9 = null,
                d10 = null, d11 = null
            | EVAL d1 = d1::datetime, d2 = d2::datetime, d3 = d3::datetime, d4 = d4::datetime, d5 = d5::datetime,
                d6 = d6::datetime, d7 = d7::datetime, d8 = d8::datetime, d9 = d9::datetime, d10 = d10::datetime,
                d11 = d11::datetime
            | FILLNULL
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertWarnings(
            "Line 6:3: [FILLNULL] the following fields have no default fill value for their type and were left "
                + "unchanged: [d1, d2, d3, d4, d5, d6, d7, d8, d9, d10]; provide a value using WITH; "
                + "only the first 10 of 11 fields are shown"
        );
    }

    public void testFillNullUnderNullnessFiltersIsAccepted() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW a = null, b = 1
            | EVAL a = a::integer
            | WHERE a IS NULL
            | FILLNULL WITH 0 a
            | WHERE a IS NOT NULL
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertThat(Expressions.names(plan.output()), containsInAnyOrder("a", "b"));
    }
}
