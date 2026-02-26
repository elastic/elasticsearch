/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.BreakerTestUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

/**
 * Base tests for {@link Operator}s that are not {@link SourceOperator} or {@link SinkOperator}.
 */
public abstract class OperatorTestCase extends AnyOperatorTestCase {

    /**
     * Valid input to be sent to {@link #simple};
     */
    protected abstract SourceOperator simpleInput(BlockFactory blockFactory, int size);

    /**
     * Assert that output from {@link #simple} is correct for the
     * given input.
     */
    protected abstract void assertSimpleOutput(List<Page> input, List<Page> results);

    /**
     * Test a small input set against {@link #simple}. Smaller input sets
     * are more likely to discover accidental behavior for clumped inputs.
     */
    public final void testSimpleSmallInput() {
        assertSimple(driverContext(), smallInputSize());
    }

    protected int smallInputSize() {
        return randomIntBetween(10, 100);
    }

    /**
     * Test a larger input set against {@link #simple}.
     */
    public final void testSimpleLargeInput() {
        assertSimple(driverContext(), largeInputSize());
    }

    protected int largeInputSize() {
        return randomIntBetween(1_000, 10_000);
    }

    /**
     * Enough memory for {@link #simple} not to throw a {@link CircuitBreakingException}.
     * It's fine if this is <strong>much</strong> more memory than {@linkplain #simple} needs.
     * When we want to make {@linkplain #simple} throw we'll find the precise amount of memory
     * that'll make it throw with a binary search.
     */
    protected ByteSizeValue enoughMemoryForSimple() {
        return ByteSizeValue.ofGb(1);
    }

    /**
     * Run {@link #simple} with a circuit breaker many times, making sure all blocks
     * are properly released. In particular, we perform a binary search to find the
     * largest amount of memory that'll throw a {@link CircuitBreakingException} with
     * starting bounds of {@code 0b} and {@link #enoughMemoryForSimple}. Then we pick
     * a random amount of memory between {@code 0b} and the maximum and run that,
     * asserting both that this throws a {@link CircuitBreakingException} and releases
     * all pages.
     */
    public void testSimpleCircuitBreaking() throws Exception {
        /*
         * Build the input before building `simple` to handle the rare
         * cases where `simple` need some state from the input - mostly
         * this is ValuesSourceReaderOperator.
         */
        DriverContext inputFactoryContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), largeInputSize()));

        ByteSizeValue memoryLimitForSimple = enoughMemoryForSimple();
        Operator.OperatorFactory simple = simple(new SimpleOptions(true));
        try {
            ByteSizeValue limit = BreakerTestUtil.findBreakerLimit(memoryLimitForSimple, l -> runWithLimit(simple, input, l));
            ByteSizeValue testWithSize = ByteSizeValue.ofBytes(randomLongBetween(0, limit.getBytes()));
            logger.info("testing with {} against a limit of {}", testWithSize, limit);
            Exception e = expectThrows(CircuitBreakingException.class, () -> runWithLimit(simple, input, testWithSize));
            assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }
        assertThat(inputFactoryContext.breaker().getUsed(), equalTo(0L));
    }

    private void runWithLimit(Operator.OperatorFactory factory, List<Page> input, ByteSizeValue limit) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);
        List<Page> localInput = CannedSourceOperator.deepCopyOf(blockFactory, input);
        boolean driverStarted = false;
        try {
            var operator = factory.get(driverContext);
            driverStarted = true;
            new TestDriverRunner().builder(driverContext).input(localInput).run(operator);
        } finally {
            if (driverStarted == false) {
                // if drive hasn't even started then we need to release the input pages manually
                Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(localInput.iterator(), p -> p::releaseBlocks)));
            }
            blockFactory.ensureAllBlocksAreReleased();
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    /**
     * Run {@link #simple} with the {@link CrankyCircuitBreakerService}
     * which fails randomly. This will catch errors caused by not
     * properly cleaning up things like {@link BigArray}s, particularly
     * in ctors.
     */
    public final void testSimpleWithCranky() {
        DriverContext inputFactoryContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), largeInputSize()));

        DriverContext driverContext = crankyDriverContext();

        Exception exception = null;
        boolean driverStarted = false;
        try {
            Operator operator = simple().get(driverContext);
            driverStarted = true;
            new TestDriverRunner().builder(driverContext).input(input).run(operator);
            // Either we get lucky and cranky doesn't throw and the test completes or we don't and it throws
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
            exception = e;
        }
        if (driverStarted == false) {
            // if drive hasn't even started then we need to release the input pages
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }

        // Note the lack of try/finally here - we're asserting that when the driver throws an exception we clear the breakers.
        long inputUsedBytes = inputFactoryContext.breaker().getUsed();
        if (inputUsedBytes != 0L) {
            fail(exception, "Expected no used bytes for input, found: " + inputUsedBytes);
        }
        long driverUsedBytes = driverContext.breaker().getUsed();
        if (driverUsedBytes != 0L) {
            fail(exception, "Expected no used bytes for driver, found: " + driverUsedBytes);
        }
    }

    /**
     * Run the {@code operators} once per page in the {@code input}.
     */
    protected final List<Page> oneDriverPerPage(List<Page> input, Supplier<List<Operator>> operators) {
        return oneDriverPerPageList(input.stream().map(List::of).iterator(), operators);
    }

    /**
     * Run the {@code operators} once to entry in the {@code source}.
     */
    protected final List<Page> oneDriverPerPageList(Iterator<List<Page>> source, Supplier<List<Operator>> operators) {
        List<Page> result = new ArrayList<>();
        while (source.hasNext()) {
            List<Page> in = source.next();
            try (
                Driver d = TestDriverFactory.create(
                    driverContext(),
                    new CannedSourceOperator(in.iterator()),
                    operators.get(),
                    new PageConsumerOperator(result::add)
                )
            ) {
                new TestDriverRunner().run(d);
            }
        }
        return result;
    }

    protected final void assertSimple(DriverContext context, int size) {
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(context.blockFactory(), size));

        List<Block> inputBlocks = new ArrayList<>();
        for (Page p : input) {
            for (int i = 0; i < p.getBlockCount(); i++) {
                inputBlocks.add(p.getBlock(i));
            }
        }

        // Clone the input so that the operator can close it, then, later, we can read it again to build the assertion.
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());

        var operator = simple().get(context);
        List<Page> results = new TestDriverRunner().builder(context).input(input).run(operator);
        assertSimpleOutput(origInput, results);
        assertOperatorStatus(operator, origInput, results);
        assertThat(context.breaker().getUsed(), equalTo(0L));

        // Release all result blocks. After this, all input blocks should be released as well, otherwise we have a leak.
        for (Page p : results) {
            p.releaseBlocks();
        }

        int unreleasedInputs = 0;
        for (Block b : inputBlocks) {
            if (b.isReleased() == false) {
                unreleasedInputs++;
            }
        }
        if (unreleasedInputs > 0) {
            throw new AssertionError("[" + unreleasedInputs + "] unreleased input blocks");
        }
    }

    /**
     * Tests that finish then close without calling {@link Operator#getOutput} to
     * retrieve a potential last page, releases all memory.
     */
    public void testSimpleFinishClose() {
        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), 1));
        // eventually, when driverContext always returns a tracking factory, we can enable this assertion
        // assertThat(driverContext.blockFactory().breaker().getUsed(), greaterThan(0L));
        try (var operator = simple().get(driverContext)) {
            assert operator.needsInput();
            for (Page page : input) {
                if (operator.needsInput()) {
                    operator.addInput(page);
                } else {
                    page.releaseBlocks();
                }
            }
            operator.finish();
            // for async operator, we need to wait for async actions to finish.
            if (operator instanceof AsyncOperator<?> || randomBoolean()) {
                driverContext.finish();
                PlainActionFuture<Void> waitForAsync = new PlainActionFuture<>();
                driverContext.waitForAsyncActions(waitForAsync);
                waitForAsync.actionGet(TimeValue.timeValueSeconds(30));
            }
        }
    }

    public static void assertDriverContext(DriverContext driverContext) {
        assertTrue(driverContext.isFinished());
        assertThat(driverContext.getSnapshot().releasables(), empty());
    }

    public static int randomPageSize() {
        if (randomBoolean()) {
            return between(1, 16);
        } else {
            return between(1, 16 * 1024);
        }
    }

    // Returns the size of an empty bytesRefBlockHash depending on the underlying implementation.
    protected final String byteRefBlockHashSize() {
        if (HashImplFactory.SWISS_TABLES_HASHING.isEnabled()) {
            return "213120b";
        }
        return "400b";
    }
}
