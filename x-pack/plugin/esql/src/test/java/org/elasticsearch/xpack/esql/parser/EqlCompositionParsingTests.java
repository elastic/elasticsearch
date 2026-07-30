/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedEqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Parses the {@code EQL} source command in composition contexts: as a subquery source
 * ({@code FROM (EQL idx "...")}), on the right-hand side of {@code WHERE x IN (EQL idx "...")} (same
 * {@code subquery} grammar rule), and as the upstream of {@code FORK}. The composition surface is gated to
 * snapshot builds via {@link EsqlCapabilities.Cap#EQL_COMMAND}, mirroring the top-level command; the
 * release-build rejection is covered by {@link #testEqlSubquerySourceUnavailableOnRealReleaseBuild}.
 *
 * @see EqlCommandParsingTests for the top-level command parsing.
 * @see SubqueryTests for the {@code FROM}-subquery parsing this mirrors.
 */
public class EqlCompositionParsingTests extends AbstractStatementParserTests {

    /**
     * A lone EQL subquery with no main pattern collapses to the inner relation, exactly as {@code FROM (FROM idx)}
     * collapses to the {@code idx} relation.
     */
    public void testEqlAsLoneSubquerySource() {
        assumeSnapshotSubquerySupport();

        LogicalPlan plan = query("FROM (EQL logs-* \"process where true\")");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(eql.indexPattern().indexPattern(), equalTo("logs-*"));
    }

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[main_index]
     * \_Subquery[]
     *   \_UnresolvedEqlRelation[process where true]
     */
    public void testEqlAsMixedSiblingSubquerySource() {
        assumeSnapshotSubquerySupport();

        LogicalPlan plan = query("FROM main_index, (EQL logs-* \"process where true\")");

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertThat(children, hasSize(2));

        UnresolvedRelation main = as(children.get(0), UnresolvedRelation.class);
        assertThat(main.indexPattern().indexPattern(), equalTo("main_index"));

        Subquery subquery = as(children.get(1), Subquery.class);
        UnresolvedEqlRelation eql = as(subquery.plan(), UnresolvedEqlRelation.class);
        assertThat(eql.indexPattern().indexPattern(), equalTo("logs-*"));
    }

    /**
     * Piped processing commands apply generically on top of the EQL subquery source, same as a FROM subquery.
     * Keep[[name]]
     * \_UnresolvedEqlRelation[process where true]
     */
    public void testEqlSubquerySourceWithProcessing() {
        assumeSnapshotSubquerySupport();

        LogicalPlan plan = query("FROM (EQL logs-* \"process where true\" | KEEP name)");

        Keep keep = as(plan, Keep.class);
        as(keep.child(), UnresolvedEqlRelation.class);
    }

    /**
     * The subquery rule also feeds {@code WHERE x IN (...)}, so the single grammar arm lights up the IN surface too.
     * Filter[InSubquery[x, Keep[[pid]] \_UnresolvedEqlRelation[process where true]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testEqlInWhereInSubquery() {
        assumeTrue("Requires snapshot builds for the EQL command", Build.current().isSnapshot());
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
        assumeTrue("Requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        LogicalPlan plan = query("FROM main_index | WHERE x IN (EQL logs-* \"process where true\" | KEEP pid)");

        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        UnresolvedAttribute value = as(inSubquery.value(), UnresolvedAttribute.class);
        assertThat(value.name(), equalTo("x"));

        Keep keep = as(inSubquery.subquery(), Keep.class);
        as(keep.child(), UnresolvedEqlRelation.class);

        UnresolvedRelation main = as(filter.child(), UnresolvedRelation.class);
        assertThat(main.indexPattern().indexPattern(), equalTo("main_index"));
    }

    /**
     * EQL as the upstream source of FORK needs no grammar change (FORK branches are processing-only for every
     * source); each branch receives the same replicated EQL leaf. Parses today; pinned here for parity.
     * Fork[...]
     * \_UnresolvedEqlRelation[process where true]
     */
    public void testEqlUpstreamOfFork() {
        assumeTrue("Requires snapshot builds for the EQL command", Build.current().isSnapshot());
        assumeTrue("Requires FORK support", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        assumeTrue("Requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        LogicalPlan plan = query("""
            EQL logs-* "process where true"
            | FORK ( WHERE pid == 100 ) ( WHERE pid == 200 )
            """);

        Fork fork = as(plan, Fork.class);
        assertThat(fork.children(), hasSize(2));
        for (LogicalPlan branch : fork.children()) {
            // Each branch replicates the same EQL leaf under its own processing (Eval[_fork] over the WHERE).
            List<UnresolvedEqlRelation> leaves = new ArrayList<>();
            branch.forEachDown(UnresolvedEqlRelation.class, leaves::add);
            assertThat(leaves, hasSize(1));
            assertThat(leaves.get(0).indexPattern().indexPattern(), equalTo("logs-*"));
        }
    }

    /**
     * Regression guard mirroring {@link EqlCommandParsingTests#testEqlCommandUnavailableOnRealReleaseBuild}: on a
     * genuine release build the EQL subquery-source grammar arm is rejected.
     */
    public void testEqlSubquerySourceUnavailableOnRealReleaseBuild() {
        assumeFalse("only exercises the real release-build gate", Build.current().isSnapshot());

        assertFalse(
            "EQL command capability must not be reported as enabled on a release build",
            EsqlCapabilities.Cap.EQL_COMMAND.isEnabled()
        );

        EsqlParser prodParser = new EsqlParser(new EsqlConfig(TEST_FUNCTION_REGISTRY));
        ParsingException pe = expectThrows(
            ParsingException.class,
            () -> prodParser.createStatement("FROM (EQL logs-* \"process where true\")")
        );
        assertThat(pe.getMessage(), containsString("line 1:"));
    }

    private void assumeSnapshotSubquerySupport() {
        assumeTrue("Requires snapshot builds for the EQL command", Build.current().isSnapshot());
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());
    }
}
