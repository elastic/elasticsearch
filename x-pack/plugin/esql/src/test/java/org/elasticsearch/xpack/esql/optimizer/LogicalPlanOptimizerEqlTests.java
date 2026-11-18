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
 * Tests for {@link PushLimitIntoEqlRelation}: only a {@code LIMIT} directly above an {@link EqlRelation} with
 * {@code result_position: head} and no EQL {@code head}/{@code tail} pipe is pushed; otherwise {@code pushedLimit} is {@code null}.
 */
public class LogicalPlanOptimizerEqlTests extends ESTestCase {

    private static final String HEAD = " WITH {\"result_position\": \"head\"}";

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testHeadLimitIsPushedIntoEventRelation() {
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | LIMIT 5", 5);
    }

    public void testHeadLimitBehindKeepIsPushed() {
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | KEEP pid | LIMIT 2", 2);
    }

    public void testHeadLimitBehindEvalIsPushed() {
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | EVAL next = pid + 1 | LIMIT 2", 2);
    }

    public void testHeadSequenceLimitIsPushed() {
        assertPushedLimit("EQL eql_test \"sequence [process where true] [network where true]\"" + HEAD + " | LIMIT 3", 3);
    }

    public void testHeadSampleLimitIsPushed() {
        assertPushedLimit("EQL eql_test \"sample by category [process where true] [network where true]\"" + HEAD + " | LIMIT 4", 4);
    }

    public void testSmallestLimitIsPushed() {
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | LIMIT 5 | KEEP pid | LIMIT 3", 3);
    }

    public void testTailIsTheDefaultAndBlocksPush() {
        assertPushedLimit("EQL eql_test \"process where true\" | LIMIT 5", null);
    }

    public void testExplicitTailResultPositionBlocksPush() {
        assertPushedLimit("EQL eql_test \"process where true\" WITH {\"result_position\": \"tail\"} | LIMIT 2", null);
    }

    public void testTailSequenceLimitIsNotPushed() {
        assertPushedLimit("EQL eql_test \"sequence [process where true] [network where true]\" | LIMIT 3", null);
    }

    public void testTailSampleLimitIsNotPushed() {
        assertPushedLimit("EQL eql_test \"sample by category [process where true] [network where true]\" | LIMIT 4", null);
    }

    public void testImplicitLimitIsPushedLikeAnyOther() {
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD, 1000);
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | KEEP pid", 1000);
        assertPushedLimit("EQL eql_test \"sequence [process where true] [network where true]\"" + HEAD, 1000);
    }

    public void testLimitEqualToTheImplicitOneIsPushed() {
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | LIMIT 1000", 1000);
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | LIMIT 10000", 10000);
    }

    public void testLimitAboveTheMaxIsPushedAsTheMax() {
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | LIMIT 1000000", 10000);
    }

    public void testImplicitLimitOfATailQueryIsNotPushed() {
        assertPushedLimit("EQL eql_test \"process where true\"", null);
    }

    public void testStatsBlocksLimitPushdown() {
        // The implicit limit sits above the Aggregate, never reaching the relation.
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | STATS c = COUNT(*)", null);
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | STATS c = COUNT(*) | LIMIT 2", null);
    }

    public void testFilterBlocksLimitPushdown() {
        // A limit above a Filter must not be pushed: the source has to over-scan so the filter still sees enough.
        assertPushedLimit("EQL eql_test \"process where true\"" + HEAD + " | WHERE pid == 100 | LIMIT 2", null);
    }

    public void testExplicitQueryTailBlocksPush() {
        // The EQL query carries its own | tail 3; an outer LIMIT must not be pushed on top of it (it would fold into
        // the tail and change which events come back).
        assertPushedLimit("EQL eql_test \"process where true | tail 3\"" + HEAD + " | LIMIT 2", null);
    }

    public void testExplicitQueryHeadBlocksPush() {
        assertPushedLimit("EQL eql_test \"process where true | head 3\"" + HEAD + " | LIMIT 2", null);
    }

    public void testLimitPushdownPreservesMetadataColumns() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());
        LogicalPlan optimized = optimizer().addIndex("eql_test", "mapping-eql_test.json")
            .coordinatorPlan("EQL eql_test \"process where true\" METADATA _id" + HEAD + " | LIMIT 5");
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
