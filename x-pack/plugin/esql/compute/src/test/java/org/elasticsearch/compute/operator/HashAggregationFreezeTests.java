/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumIntGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The freeze route of {@link HashAggregationOperator} over every hash the route looks keys up in, with {@code SUM} over
 * ints, whose function partitions and so is eligible, under an initial stage per input page and one final stage:
 * single keys of each type, including the hashes that reserve an id for null, and composite keys over longs,
 * keywords and mixed types. A spilling run under a budget every page crosses must give the result of a resident run,
 * with no group twice, which is what a null key or a group first seen after the freeze would produce if the lookup
 * answered for a key the hash did not hold.
 */
public class HashAggregationFreezeTests extends ComputeTestCase {

    private TestThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() throws Exception {
        // the scopes' spilled data goes on the pool, before the pool stops
        closeSpillingScopes();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private static final List<List<ElementType>> SINGLE_KEYS = List.of(
        List.of(ElementType.LONG),
        List.of(ElementType.INT),
        List.of(ElementType.DOUBLE),
        List.of(ElementType.BOOLEAN),
        List.of(ElementType.BYTES_REF)
    );

    private static final List<List<ElementType>> COMPOSITE_KEYS = List.of(
        List.of(ElementType.LONG, ElementType.LONG),
        List.of(ElementType.LONG, ElementType.INT),
        List.of(ElementType.LONG, ElementType.BYTES_REF),
        List.of(ElementType.BYTES_REF, ElementType.BYTES_REF),
        List.of(ElementType.BYTES_REF, ElementType.BYTES_REF, ElementType.BYTES_REF),
        List.of(ElementType.INT, ElementType.DOUBLE, ElementType.BOOLEAN)
    );

    /**
     * Null keys that first appear once the operator is likely frozen, for every single-key hash.
     */
    public void testNullKeyAfterTheFreeze() throws Exception {
        for (List<ElementType> keys : SINGLE_KEYS) {
            run(keys, true);
        }
    }

    public void testSingleKeys() throws Exception {
        for (List<ElementType> keys : SINGLE_KEYS) {
            run(keys, false);
        }
    }

    public void testCompositeKeys() throws Exception {
        for (List<ElementType> keys : COMPOSITE_KEYS) {
            run(keys, randomBoolean());
        }
    }

    private void run(List<ElementType> keys, boolean nullsLate) throws Exception {
        int pages = between(8, 40);
        int rowsPerPage = between(10, 200);
        int domain = between(5, 200);
        List<List<Object>> rows = new ArrayList<>();
        for (int page = 0; page < pages; page++) {
            boolean late = page >= pages / 2;
            for (int r = 0; r < rowsPerPage; r++) {
                List<Object> row = new ArrayList<>(keys.size() + 1);
                for (ElementType type : keys) {
                    boolean nullKey = (nullsLate == false || late) && randomInt(10) == 0;
                    row.add(nullKey ? null : key(type, between(0, late ? 2 * domain : domain)));
                }
                row.add(randomIntBetween(-1_000_000, 1_000_000));
                rows.add(row);
            }
        }
        Map<List<Object>, Long> expected = runChain(
            this::residentDriverContext,
            residentDriverContext(),
            keys,
            rows,
            rowsPerPage,
            Freeze.NONE
        );
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        try {
            // zero freezes before the first page, on no group; the boolean keys make the smallest hash, two keys and
            // the null, which holds more than the other minimum before its second page
            long minSpillBytes = randomBoolean() ? 0 : between(1, 48);
            DriverContext spilling = spillingDriverContext(
                ByteSizeValue.ofKb(1),
                minSpillBytes,
                store,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            Map<List<Object>, Long> actual = runChain(
                () -> spillingDriverContext(spilling),
                spilling,
                keys,
                rows,
                rowsPerPage,
                minSpillBytes == 0 ? Freeze.ON_NO_GROUP : Freeze.ON_FIRST_GROUPS
            );
            assertThat("keys " + keys, actual, equalTo(expected));
        } finally {
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            shutdown.get(30, TimeUnit.SECONDS);
        }
    }

    /** What the final stage must have done. */
    private enum Freeze {
        NONE,
        /** Frozen before its first page, with every row gone to a run. */
        ON_NO_GROUP,
        /** Frozen on the groups of its first pages, with the rows of the groups first seen after that gone to the runs. */
        ON_FIRST_GROUPS
    }

    private DriverContext residentDriverContext() {
        var blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private static Object key(ElementType type, int n) {
        return switch (type) {
            case LONG -> (long) n;
            case INT -> n;
            case DOUBLE -> (double) n;
            case BOOLEAN -> n % 2 == 0;
            case BYTES_REF -> new BytesRef("key-" + n);
            default -> throw new IllegalArgumentException(type.toString());
        };
    }

    /**
     * Runs an initial aggregation of {@code SUM(value) BY keys} over each input page in a driver of its own, as a
     * data node's drivers would, then the final aggregation over all the partial pages in one driver, and returns the
     * groups with their sums, failing on a group emitted twice. Under spilling the final stage must have frozen.
     *
     * @param initialContexts a context per initial driver, siblings of {@code finalContext} under spilling
     */
    private Map<List<Object>, Long> runChain(
        Supplier<DriverContext> initialContexts,
        DriverContext finalContext,
        List<ElementType> keys,
        List<List<Object>> rows,
        int rowsPerPage,
        Freeze expectFreeze
    ) {
        Stages stages = stages(keys);
        List<Page> partials = partials(initialContexts, stages, rows, rowsPerPage);
        List<Page> results = new ArrayList<>();
        DriverProfile profile;
        try (
            Driver d = TestDriverFactory.create(
                finalContext,
                new CannedSourceOperator(partials.iterator()),
                List.of(stages.last().get(finalContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(d);
            profile = d.profile();
        }
        Map<List<Object>, Long> out = sums(results, stages.k());
        if (expectFreeze != Freeze.NONE) {
            HashAggregationOperator.FreezeStatus freeze = null;
            for (OperatorStatus status : profile.operators()) {
                HashAggregationOperator.FreezeStatus found = freezeOf(status.status());
                if (found != null) {
                    freeze = found;
                }
            }
            assertNotNull("the final stage froze: " + profile.operators(), freeze);
            assertThat(freeze.spill().spillRuns(), greaterThan(0));
            switch (expectFreeze) {
                case ON_NO_GROUP -> {
                    assertThat("keys " + keys, freeze.groupsFrozen(), equalTo(0L));
                    assertThat("keys " + keys, freeze.rowsSpilled(), greaterThan(0L));
                }
                case ON_FIRST_GROUPS -> {
                    assertThat("keys " + keys, freeze.groupsFrozen(), greaterThan(0L));
                    // a boolean key alone is all seen before the freeze, the other domains double after it
                    if (keys.equals(List.of(ElementType.BOOLEAN)) == false) {
                        assertThat("keys " + keys, freeze.rowsSpilled(), greaterThan(0L));
                    }
                }
                default -> throw new AssertionError(expectFreeze);
            }
        }
        return out;
    }

    /**
     * The upstream finishes while a page waits for the writer: the freeze fires as the headroom is still being created,
     * so the page is held, and the operator is finished before the page is routed. The held page's rows of frozen
     * groups must merge before the frozen groups go out, for the result to be the resident run's. The store holds
     * every completion until the test releases it, so the operator is driven by hand.
     */
    public void testHeldPageAtFinish() throws Exception {
        List<ElementType> keys = List.of(ElementType.LONG);
        int rowsPerPage = 100;
        List<List<Object>> rows = new ArrayList<>();
        // the first page's groups are the frozen ones; the second page has rows of them and of new groups
        for (int page = 0; page < 2; page++) {
            for (int r = 0; r < rowsPerPage; r++) {
                rows.add(List.of((long) between(0, page == 0 ? 9 : 19), randomIntBetween(-1_000_000, 1_000_000)));
            }
        }
        Map<List<Object>, Long> expected = runChain(
            this::residentDriverContext,
            residentDriverContext(),
            keys,
            rows,
            rowsPerPage,
            Freeze.NONE
        );
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        List<Page> results = new ArrayList<>();
        try {
            // a budget any page crosses and an own-state minimum the first page's groups reach; the codec pool is not
            // the pool the store completes on, since a codec task waits for an append's completion
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 1, store, threadPool.executor(ThreadPool.Names.SEARCH));
            Stages stages = stages(keys);
            List<Page> partials = partials(() -> spillingDriverContext(ctx), stages, rows, rowsPerPage);
            assertThat(partials, hasSize(2));
            HashAggregationOperator.FreezeStatus freeze;
            try (Operator op = stages.last().get(ctx)) {
                op.addInput(partials.get(0));
                assertTrue("the first page is resident", op.needsInput());
                op.addInput(partials.get(1));
                assertFalse("the second page waits for the writer", op.needsInput());
                op.finish();
                int rounds = 0;
                while (op.isFinished() == false) {
                    assertThat("the operator makes progress", ++rounds, lessThanOrEqualTo(10_000));
                    if (op.isBlocked().listener().isDone() == false) {
                        // what the operator waits for is either held already or on its way from the codec thread
                        assertBusy(() -> assertThat("the store holds an operation", store.heldOperations(), greaterThan(0)));
                        assertTrue("blocked on a held completion", store.releaseRandom());
                        continue;
                    }
                    Page page = op.getOutput();
                    if (page != null) {
                        results.add(page);
                    }
                }
                freeze = freezeOf(op.status());
            }
            ctx.finish();
            PlainActionFuture<Void> quiet = new PlainActionFuture<>();
            ctx.waitForAsyncActions(quiet);
            releaseUntilDone(store, quiet);
            assertNotNull("the final stage froze", freeze);
            assertThat("on the first page's groups", freeze.groupsFrozen(), greaterThan(0L));
            assertThat("the second page's new groups went to a run", freeze.rowsSpilled(), greaterThan(0L));
            assertThat(sums(results, stages.k()), equalTo(expected));
        } finally {
            Releasables.close(results);
            closeSpillingScopes();
            // the store holds the shutdown's own completion as well
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            releaseUntilDone(store, shutdown);
        }
    }

    /**
     * Releases what the store holds until {@code done} completes: an operation may still be on its way from the codec
     * thread when the store holds nothing, so the wait is a deadline rather than a number of rounds.
     */
    private static void releaseUntilDone(MemorySpillStore store, PlainActionFuture<Void> done) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (done.isDone() == false) {
            if (store.releaseAll() > 0) {
                continue;
            }
            assertTrue("the store's operations did not complete in time", System.nanoTime() < deadline);
            Thread.sleep(1);
        }
        done.get();
    }

    /** The two stages of {@code SUM(value) BY keys}: the value is the channel after the {@code k} keys. */
    private record Stages(HashAggregationOperator.Factory initial, HashAggregationOperator.Factory last, int k) {}

    private static Stages stages(List<ElementType> keys) {
        int k = keys.size();
        List<BlockHash.GroupSpec> groups = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            groups.add(new BlockHash.GroupSpec(i, keys.get(i)));
        }
        var supplier = new SumIntAggregatorFunctionSupplier();
        int intermediate = SumIntGroupingAggregatorFunction.intermediateStateDesc().size();
        var initial = new HashAggregationOperator.Builder().groups(groups)
            .mode(AggregatorMode.INITIAL)
            .aggregators(List.of(supplier.groupingAggregatorFactory(AggregatorMode.INITIAL, List.of(k))))
            .build();
        var last = new HashAggregationOperator.Builder().groups(groups)
            .mode(AggregatorMode.FINAL)
            .aggregators(
                List.of(supplier.groupingAggregatorFactory(AggregatorMode.FINAL, IntStream.range(k, k + intermediate).boxed().toList()))
            )
            .build();
        return new Stages(initial, last, k);
    }

    /**
     * The initial stage over each page of {@code rowsPerPage} rows in a driver of its own, as a data node's drivers
     * would run it: one partial page per input page.
     */
    private static List<Page> partials(Supplier<DriverContext> contexts, Stages stages, List<List<Object>> rows, int rowsPerPage) {
        List<Page> partials = new ArrayList<>();
        for (int start = 0; start < rows.size(); start += rowsPerPage) {
            DriverContext context = contexts.get();
            Block[] blocks = BlockUtils.fromList(context.blockFactory(), rows.subList(start, Math.min(rows.size(), start + rowsPerPage)));
            try (
                Driver d = TestDriverFactory.create(
                    context,
                    new CannedSourceOperator(List.of(new Page(blocks)).iterator()),
                    List.of(stages.initial().get(context)),
                    new TestResultPageSinkOperator(partials::add)
                )
            ) {
                new TestDriverRunner().run(d);
            }
        }
        return partials;
    }

    /** The groups and their sums out of the final stage's pages, failing on a group emitted twice; the pages are released. */
    private static Map<List<Object>, Long> sums(List<Page> results, int k) {
        Map<List<Object>, Long> out = new HashMap<>();
        try {
            for (Page page : results) {
                LongBlock sums = page.getBlock(k);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    List<Object> key = new ArrayList<>(k);
                    for (int i = 0; i < k; i++) {
                        key.add(BlockUtils.toJavaObject(page.getBlock(i), p));
                    }
                    Long previous = out.put(key, sums.getLong(p));
                    assertNull("group " + key + " emitted twice", previous);
                }
            }
        } finally {
            Releasables.close(results);
            results.clear();
        }
        return out;
    }

    @Nullable
    private static HashAggregationOperator.FreezeStatus freezeOf(Operator.Status status) {
        if (status instanceof HashAggregationOperator.Status hashStatus) {
            for (Operator.Status.ExtraStatus extra : hashStatus.extraFields()) {
                if (extra instanceof HashAggregationOperator.FreezeStatus freeze) {
                    return freeze;
                }
            }
        }
        return null;
    }
}
