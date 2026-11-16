/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.spill.ResultCollector;
import org.elasticsearch.compute.spill.ResultPages;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * The sink of a result collected on the coordinator, and the collector's release with the query.
 */
public class ComputeServiceResultSinkTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
    private final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    /**
     * The collector reaches every driver's output operator as itself, so the operator reports what the collector
     * spilled; handed over as a plain consumer it would be wrapped, and the operator would have nothing to report.
     */
    public void testCollectorReachesTheOperatorAsItself() {
        AtomicInteger released = new AtomicInteger();
        ResultCollector collector = new ResultCollector(null, released::incrementAndGet, Long.MAX_VALUE, "result");
        try {
            OutputExec sink = ComputeService.outputSink(plan(), collector, new ArrayList<Page>()::add);
            assertSame(collector, sink.getPageConsumerFactory().create(driverContext));
            assertSame("every driver of the sink shares the collector", collector, sink.getPageConsumerFactory().create(driverContext));
            try (
                Operator operator = new OutputOperator.OutputOperatorFactory(List.of("a"), p -> p, sink.getPageConsumerFactory()).get(
                    driverContext
                )
            ) {
                int positions = between(1, 100);
                operator.addInput(new Page(blockFactory.newConstantIntBlockWith(1, positions)));
                assertThat(operator.status(), instanceOf(OutputOperator.Status.class));
                OutputOperator.Status status = (OutputOperator.Status) operator.status();
                assertThat(status.pagesReceived(), equalTo(1));
                assertThat(status.rowsReceived(), equalTo((long) positions));
                assertThat(collector.rows(), equalTo((long) positions));
            }
        } finally {
            collector.close();
        }
        assertEquals(1, released.get());
    }

    /**
     * Without a collector the pages go to the list consumer, which has no spill to report.
     */
    public void testListConsumerWithoutACollector() {
        List<Page> pages = new ArrayList<>();
        OutputExec sink = ComputeService.outputSink(plan(), null, pages::add);
        try (
            Operator operator = new OutputOperator.OutputOperatorFactory(List.of("a"), p -> p, sink.getPageConsumerFactory()).get(
                driverContext
            )
        ) {
            operator.addInput(new Page(blockFactory.newConstantIntBlockWith(1, 3)));
            assertThat(operator.status(), nullValue());
            assertThat(pages.size(), equalTo(1));
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Once the collector handed its result over, the result owns what the collector held: the query's completion
     * releases nothing a second time.
     */
    public void testCompletionAfterHandOverReleasesNothingMore() {
        AtomicInteger released = new AtomicInteger();
        ResultCollector collector = new ResultCollector(null, released::incrementAndGet, Long.MAX_VALUE, "result");
        collector.consume(new Page(blockFactory.newConstantIntBlockWith(1, 3)));
        PlainActionFuture<ResultPages> handedOver = new PlainActionFuture<>();
        collector.handOver(handedOver);
        try (ResultPages result = handedOver.actionGet()) {
            assertEquals(1, released.get());
            PlainActionFuture<Result> future = new PlainActionFuture<>();
            ComputeService.closingCollector(collector, future).onResponse(emptyResult());
            assertTrue(future.isDone());
            assertEquals(1, released.get());
            assertThat("the pages are still the result's", result.rows(), equalTo(3L));
        }
    }

    /**
     * A failure before the hand-over releases the collector's pages and its lease.
     */
    public void testFailureBeforeHandOverReleasesTheCollector() {
        AtomicInteger released = new AtomicInteger();
        ResultCollector collector = new ResultCollector(null, released::incrementAndGet, Long.MAX_VALUE, "result");
        Block block = blockFactory.newConstantIntBlockWith(1, 3);
        collector.consume(new Page(block));
        PlainActionFuture<Result> future = new PlainActionFuture<>();
        ComputeService.closingCollector(collector, future).onFailure(new IllegalStateException("simulated"));
        expectThrows(IllegalStateException.class, future::actionGet);
        assertEquals(1, released.get());
        assertTrue(block.isReleased());
    }

    public void testNoCollectorLeavesTheListenerAlone() {
        PlainActionFuture<Result> future = new PlainActionFuture<>();
        assertSame(future, ComputeService.closingCollector(null, future));
    }

    private static Result emptyResult() {
        return new Result(
            List.of(),
            List.of(),
            null,
            configuration(new QueryPragmas(Settings.EMPTY)),
            DriverCompletionInfo.EMPTY,
            null,
            null
        );
    }

    private static PhysicalPlan plan() {
        return new LocalSourceExec(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
    }
}
