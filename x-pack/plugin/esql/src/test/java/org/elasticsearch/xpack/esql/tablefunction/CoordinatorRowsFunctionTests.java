/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationBuilder;
import org.junit.Before;

import java.util.List;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * {@code toRelation} driven directly, which is where its failure paths are: a query cannot make a function fail its
 * listener, throw instead of completing, or leave a column short, and each of those has to release what was written.
 * The breaker here is what notices if one does not.
 */
public class CoordinatorRowsFunctionTests extends ESTestCase {

    private static final ByteSizeValue LIMIT = ByteSizeValue.ofKb(256);
    /** Enough appended bytes that the breaker is certainly charged, so a later assertion that it is zero means something. */
    private static final int ENOUGH_TO_CHARGE = 1000;

    private CircuitBreaker breaker;
    private BlockFactory blockFactory;

    @Before
    public void buildBreakingBlockFactory() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, LIMIT).withCircuitBreaking();
        breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        blockFactory = new BlockFactory(breaker, bigArrays);
    }

    /** A function whose body each test supplies, so one subclass reaches every path through the shared {@code toRelation}. */
    private class Scripted extends CoordinatorRowsFunction {

        private final BiConsumer<LocalRelationBuilder, ActionListener<Void>> body;

        Scripted(BiConsumer<LocalRelationBuilder, ActionListener<Void>> body) {
            this.body = body;
        }

        @Override
        public String name() {
            return "scripted";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of();
        }

        @Override
        BlockFactory blockFactory() {
            return blockFactory;
        }

        @Override
        protected void write(
            TableFunctionInvocation call,
            ProjectMetadata projectMetadata,
            LocalRelationBuilder rows,
            ActionListener<Void> listener
        ) {
            body.accept(rows, listener);
        }
    }

    private static final Source CALL_SOURCE = new Source(1, 5, "scripted()");

    private PlainActionFuture<LeafPlan> relationOf(BiConsumer<LocalRelationBuilder, ActionListener<Void>> body) {
        PlainActionFuture<LeafPlan> future = new PlainActionFuture<>();
        TableFunctionInvocation call = new TableFunctionInvocation(CALL_SOURCE, "scripted", List.of());
        // Not inside a try/catch on purpose: a throw escaping here rather than reaching the listener fails the test.
        new Scripted(body).toRelation(call, List.of(), null, future);
        assertThat("toRelation answers inline", future.isDone(), equalTo(true));
        return future;
    }

    private static void fill(LocalRelationBuilder.Keyword column, int rows) {
        for (int i = 0; i < rows; i++) {
            column.append("some text that takes a few bytes " + i);
        }
    }

    /** The relation is the declared columns over the appended values, and it owns the blocks from there. */
    public void testTheRelationIsTheDeclaredColumnsOverTheAppendedValues() {
        LeafPlan leaf = relationOf((rows, listener) -> {
            LocalRelationBuilder.Keyword name = rows.keyword("name");
            LocalRelationBuilder.Longs count = rows.longs("count");
            name.append("a");
            count.append(1L);
            name.append("b");
            count.append(2L);
            listener.onResponse(null);
        }).actionGet();

        assertThat(leaf, instanceOf(LocalRelation.class));
        LocalRelation relation = (LocalRelation) leaf;
        assertThat(relation.source(), equalTo(CALL_SOURCE));
        assertThat(relation.output().stream().map(Attribute::name).toList(), contains("name", "count"));
        assertThat(relation.output().stream().map(Attribute::dataType).toList(), contains(DataType.KEYWORD, DataType.LONG));

        Page page = relation.supplier().get();
        assertThat(page.getBlockCount(), equalTo(2));
        assertThat(page.getPositionCount(), equalTo(2));
        assertThat(((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()), equalTo(new BytesRef("b")));
        assertThat(((LongBlock) page.getBlock(1)).getLong(1), equalTo(2L));
        // Still charged, because the relation holds them: this is what makes a zero elsewhere evidence of a release.
        assertThat(breaker.getUsed() > 0, equalTo(true));

        page.releaseBlocks();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A function that fails its listener fails the query, and leaves nothing of its half-written table charged. */
    public void testAFailedListenerReleasesWhatWasWritten() {
        PlainActionFuture<LeafPlan> future = relationOf((rows, listener) -> {
            fill(rows.keyword("name"), ENOUGH_TO_CHARGE);
            assertThat(breaker.getUsed() > 0, equalTo(true));
            listener.onFailure(new IllegalStateException("listing refused"));
        });

        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("listing refused"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A function that throws rather than completing is the same case, and the throw still arrives as a failure. */
    public void testAThrowFromWriteReleasesWhatWasWrittenAndReachesTheListener() {
        PlainActionFuture<LeafPlan> future = relationOf((rows, listener) -> {
            fill(rows.keyword("name"), ENOUGH_TO_CHARGE);
            assertThat(breaker.getUsed() > 0, equalTo(true));
            throw new IllegalArgumentException("gave up part way");
        });

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("gave up part way"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * A column left short of another is found when the page is built, which is after the function said it was done —
     * so it has to come back through the listener rather than out of {@code toRelation}.
     */
    public void testARaggedColumnFailsThroughTheListener() {
        PlainActionFuture<LeafPlan> future = relationOf((rows, listener) -> {
            LocalRelationBuilder.Keyword name = rows.keyword("name");
            LocalRelationBuilder.Longs count = rows.longs("count");
            fill(name, ENOUGH_TO_CHARGE);
            count.append(1L);
            assertThat(breaker.getUsed() > 0, equalTo(true));
            listener.onResponse(null);
        });

        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("wrote [1] values to the column [count] and [1000] to [name]"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** The function's own name is what a misuse is reported against, so a reader of the exception knows whose bug it is. */
    public void testAMisuseIsReportedAgainstTheFunctionName() {
        PlainActionFuture<LeafPlan> future = relationOf((rows, listener) -> {
            rows.keyword("name");
            rows.keyword("name");
            listener.onResponse(null);
        });

        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("[scripted] declared the column [name] more than once"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** {@code computed} is for a function with no listener of its own: writing and completing are one step. */
    public void testComputedCompletesTheListener() {
        LeafPlan leaf = relationOf(
            (rows, listener) -> CoordinatorRowsFunction.computed(listener, out -> out.keyword("name").append("a"), rows)
        ).actionGet();

        Page page = ((LocalRelation) leaf).supplier().get();
        assertThat(((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()), equalTo(new BytesRef("a")));
        page.releaseBlocks();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** And a throw from inside it is a failure rather than an escape, which is the only reason it is worth having. */
    public void testComputedRoutesAThrowToTheListener() {
        PlainActionFuture<LeafPlan> future = relationOf((rows, listener) -> CoordinatorRowsFunction.computed(listener, out -> {
            fill(out.keyword("name"), ENOUGH_TO_CHARGE);
            assertThat(breaker.getUsed() > 0, equalTo(true));
            throw new IllegalArgumentException("nothing to list");
        }, rows));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("nothing to list"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }
}
