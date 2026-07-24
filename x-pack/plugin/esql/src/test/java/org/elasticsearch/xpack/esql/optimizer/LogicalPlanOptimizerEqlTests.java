/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushLimitIntoEqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.optimizer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link PushLimitIntoEqlRelation}: a row {@code LIMIT} directly above an event-mode {@link EqlRelation}
 * is folded into the request size ({@code pushedLimit}); everything else leaves it {@code null} (cap-driven).
 */
public class LogicalPlanOptimizerEqlTests extends ESTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testExplicitLimitPushedIntoEventRelation() {
        assertPushedLimit("EQL eql_test \"process where true\" | LIMIT 5", 5);
    }

    public void testBareEventQueryPushesImplicitDefaultLimit() {
        // AddImplicitLimit adds LIMIT 1000 directly above the relation; it is pushed like an explicit one.
        assertPushedLimit("EQL eql_test \"process where true\"", 1000);
    }

    public void testStatsBlocksLimitPushdown() {
        // The implicit limit sits above the Aggregate, never reaching the relation — the silent-truncation case.
        assertPushedLimit("EQL eql_test \"process where true\" | STATS c = COUNT(*)", null);
    }

    public void testFilterBlocksLimitPushdown() {
        // A limit above a Filter must not be pushed: the source has to over-scan so the filter still sees enough.
        assertPushedLimit("EQL eql_test \"process where true\" | WHERE pid == 100 | LIMIT 2", null);
    }

    public void testSequenceModeIsNotPushed() {
        // Sequence rows are unnested per event, so a row limit does not map to the number of matches.
        assertPushedLimit("EQL eql_test \"sequence [process where true] [network where true]\" | LIMIT 3", null);
    }

    public void testLimitPushdownPreservesMetadataColumns() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());
        LogicalPlan optimized = optimizer().addIndex("eql_test", "mapping-eql_test.json")
            .coordinatorPlan("EQL eql_test \"process where true\" METADATA _id | LIMIT 5");
        List<EqlRelation> leaves = new ArrayList<>();
        optimized.forEachDown(EqlRelation.class, leaves::add);
        assertThat(leaves, hasSize(1));
        assertThat(leaves.get(0).pushedLimit(), equalTo(5));
        Attribute id = leaves.get(0).output().stream().filter(a -> a.name().equals("_id")).findFirst().orElseThrow();
        assertThat("_id must survive limit pushdown as a metadata column", id, instanceOf(MetadataAttribute.class));
    }

    private static void assertPushedLimit(String query, Integer expected) {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());
        LogicalPlan optimized = optimizer().addIndex("eql_test", "mapping-eql_test.json").coordinatorPlan(query);
        List<EqlRelation> leaves = new ArrayList<>();
        optimized.forEachDown(EqlRelation.class, leaves::add);
        assertThat(leaves, hasSize(1));
        assertThat(leaves.get(0).pushedLimit(), expected == null ? nullValue() : equalTo(expected));
    }
}
