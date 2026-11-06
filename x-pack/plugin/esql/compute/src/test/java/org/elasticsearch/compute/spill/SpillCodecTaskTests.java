/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The serialization fixture of the plan's package A: a codec task over a deterministic sink with completions withheld.
 * It asserts the bounds the design rests on: the source page stays whole until the task completes, bytes in flight never
 * exceed the buffer depth times the chunk size, no thread outside the codec pool waits, a saturated pool still progresses
 * once completions are released, cancellation wakes a waiting task, permits bound the pages detached, and the bytes land
 * once and in order. The relief cases measure, by the breaker, what releasing a page frees when its blocks are aliased
 * or share a dictionary. "No thread outside the codec pool waits" holds by construction and is visible here as the
 * test thread returning from {@code submit} while every completion is withheld.
 */
public class SpillCodecTaskTests extends ComputeTestCase {

    private static final String CODEC = "spill_codec_test";
    private static final TransportVersion VERSION = TransportVersion.current();
    private static final long STALL_SECONDS = 30;

    private TestThreadPool threadPool;
    private int chunkSize;
    private int depth;

    @Before
    public void chooseChunking() {
        chunkSize = between(128, 4096);
        depth = between(1, 4);
    }

    private Executor codecPool(int threads, int queue) {
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, CODEC, threads, queue, "spill", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        return threadPool.executor(CODEC);
    }

    @After
    public void shutdownThreadPool() {
        if (threadPool != null) {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * A page a few chunks long with a long column and a keyword column with nulls and multivalues.
     */
    private Page randomPage(BlockFactory factory, int positions) {
        try (
            LongBlock.Builder longs = factory.newLongBlockBuilder(positions);
            BytesRefBlock.Builder keywords = factory.newBytesRefBlockBuilder(positions)
        ) {
            for (int p = 0; p < positions; p++) {
                if (randomInt(20) == 0) {
                    longs.appendNull();
                } else {
                    longs.appendLong(randomLong());
                }
                int values = randomInt(10) == 0 ? between(2, 4) : (randomInt(20) == 0 ? 0 : 1);
                if (values == 0) {
                    keywords.appendNull();
                } else if (values == 1) {
                    keywords.appendBytesRef(new BytesRef(randomAlphaOfLengthBetween(5, 40)));
                } else {
                    keywords.beginPositionEntry();
                    for (int v = 0; v < values; v++) {
                        keywords.appendBytesRef(new BytesRef(randomAlphaOfLengthBetween(5, 40)));
                    }
                    keywords.endPositionEntry();
                }
            }
            return new Page(longs.build(), keywords.build());
        }
    }

    private static byte[] reference(Page page) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(VERSION);
            page.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    private SpillCodecTask submit(Executor executor, Page page, ChunkSink sink, CircuitBreaker breaker, ActionListener<Long> listener) {
        return SpillCodecTask.submit(executor, page, VERSION, sink, breaker, chunkSize, depth, null, listener);
    }

    private static String describe(List<DeferredChunkSink> sinks, List<PlainActionFuture<Long>> futures) {
        StringBuilder state = new StringBuilder();
        for (int i = 0; i < sinks.size(); i++) {
            state.append("task ")
                .append(i)
                .append(": done=")
                .append(futures.get(i).isDone())
                .append(" appends=")
                .append(sinks.get(i).appends())
                .append(" held=")
                .append(sinks.get(i).heldCount())
                .append("; ");
        }
        return state.toString();
    }

    /**
     * Releases held completions, one per sink per round, until every future is done; fails with the state after a
     * deadline instead of hanging. {@code eachRound} runs on every round, for invariants that must hold throughout.
     */
    private static void releaseUntilDone(List<DeferredChunkSink> sinks, List<PlainActionFuture<Long>> futures, Runnable eachRound) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(STALL_SECONDS);
        while (futures.stream().anyMatch(f -> f.isDone() == false)) {
            if (System.nanoTime() > deadline) {
                fail("tasks stalled: " + describe(sinks, futures));
            }
            eachRound.run();
            for (DeferredChunkSink sink : sinks) {
                sink.release(1);
            }
            Thread.onSpinWait();
        }
    }

    private static void assertContractKept(DeferredChunkSink sink) {
        assertThat(sink.violations(), empty());
        assertThat(sink.maxOutstanding(), lessThanOrEqualTo(1));
    }

    public void testSourceStaysWholeAndBytesInFlightAreBounded() throws Exception {
        BlockFactory factory = blockFactory();
        CircuitBreaker breaker = factory.breaker();
        Executor executor = codecPool(1, 16);
        Page page = randomPage(factory, between(500, 3000));
        long pageBytes = page.ramBytesUsedByBlocks();
        byte[] expected = reference(page);
        assertThat("the page must span several chunks for this test", expected.length, greaterThan(3 * chunkSize));
        DeferredChunkSink sink = new DeferredChunkSink(DeferredChunkSink.Mode.DEFERRED);
        PlainActionFuture<Long> done = new PlainActionFuture<>();
        submit(executor, page, sink, breaker, done);
        assertBusy(() -> assertThat(sink.heldCount(), equalTo(1)));
        // completions withheld: the task waits on the codec pool, the test thread is free, and the source is untouched
        assertFalse(done.isDone());
        assertThat(page.getBlock(0).isReleased(), equalTo(false));
        assertThat(page.ramBytesUsedByBlocks(), equalTo(pageBytes));
        assertThat(breaker.getUsed(), lessThanOrEqualTo(pageBytes + (long) depth * chunkSize));
        int released = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(STALL_SECONDS);
        while (done.isDone() == false) {
            assertBusy(() -> assertTrue(done.isDone() || sink.heldCount() > 0));
            released += sink.release(1);
            assertThat(sink.bytesHeld(), lessThanOrEqualTo((long) chunkSize));
            assertThat(breaker.getUsed(), lessThanOrEqualTo(pageBytes + (long) depth * chunkSize));
            if (System.nanoTime() > deadline) {
                fail("task stalled after " + released + " releases, " + sink.appends() + " appends");
            }
        }
        assertThat(done.get(10, TimeUnit.SECONDS), equalTo((long) expected.length));
        assertThat(released, equalTo(sink.appends()));
        assertContractKept(sink);
        assertThat(sink.maxBytesHeld(), lessThanOrEqualTo((long) chunkSize));
        assertArrayEquals(expected, sink.bytes());
        // the task returned its buffers; the operator still holds the page
        assertThat(breaker.getUsed(), equalTo(pageBytes));
        page.releaseBlocks();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testInlineCompletion() throws Exception {
        BlockFactory factory = blockFactory();
        Executor executor = codecPool(between(1, 3), 16);
        Page page = randomPage(factory, between(100, 2000));
        byte[] expected = reference(page);
        DeferredChunkSink sink = new DeferredChunkSink(DeferredChunkSink.Mode.INLINE);
        PlainActionFuture<Long> done = new PlainActionFuture<>();
        submit(executor, page, sink, factory.breaker(), done);
        assertThat(done.get(10, TimeUnit.SECONDS), equalTo((long) expected.length));
        assertArrayEquals(expected, sink.bytes());
        assertContractKept(sink);
        page.releaseBlocks();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testDirectExecutorCompletesOnTheCallingThread() throws Exception {
        BlockFactory factory = blockFactory();
        Page page = randomPage(factory, between(100, 2000));
        byte[] expected = reference(page);
        DeferredChunkSink sink = new DeferredChunkSink(DeferredChunkSink.Mode.INLINE);
        PlainActionFuture<Long> done = new PlainActionFuture<>();
        submit(EsExecutors.DIRECT_EXECUTOR_SERVICE, page, sink, factory.breaker(), done);
        assertTrue("a direct executor runs the task before submit returns", done.isDone());
        assertThat(done.get(), equalTo((long) expected.length));
        assertArrayEquals(expected, sink.bytes());
        assertContractKept(sink);
        page.releaseBlocks();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testSaturatedPoolProgressesOnceCompletionsAreReleased() throws Exception {
        BlockFactory factory = blockFactory();
        int tasks = between(3, 6);
        Executor executor = codecPool(1, tasks);
        List<Page> pages = new ArrayList<>();
        List<byte[]> expected = new ArrayList<>();
        List<DeferredChunkSink> sinks = new ArrayList<>();
        List<PlainActionFuture<Long>> futures = new ArrayList<>();
        for (int i = 0; i < tasks; i++) {
            Page page = randomPage(factory, between(200, 1500));
            pages.add(page);
            expected.add(reference(page));
            DeferredChunkSink sink = new DeferredChunkSink(DeferredChunkSink.Mode.DEFERRED);
            sinks.add(sink);
            PlainActionFuture<Long> future = new PlainActionFuture<>();
            futures.add(future);
            submit(executor, page, sink, factory.breaker(), future);
        }
        // one codec thread, several tasks, every completion withheld: the test thread, not a codec thread, releases them,
        // one at a time and in random order across the sinks
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(STALL_SECONDS);
        while (futures.stream().anyMatch(f -> f.isDone() == false)) {
            // either a completion is waiting to be released, or the last release just let the final task finish
            assertBusy(
                () -> assertTrue(
                    describe(sinks, futures),
                    futures.stream().allMatch(PlainActionFuture::isDone) || sinks.stream().anyMatch(s -> s.heldCount() > 0)
                )
            );
            for (DeferredChunkSink sink : shuffledList(sinks)) {
                if (sink.release(1) > 0) {
                    break;
                }
            }
            if (System.nanoTime() > deadline) {
                fail("tasks stalled: " + describe(sinks, futures));
            }
        }
        for (int i = 0; i < tasks; i++) {
            assertThat(futures.get(i).get(10, TimeUnit.SECONDS), equalTo((long) expected.get(i).length));
            assertArrayEquals(expected.get(i), sinks.get(i).bytes());
            assertContractKept(sinks.get(i));
            pages.get(i).releaseBlocks();
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testCancellationWakesAWaitingTask() throws Exception {
        BlockFactory factory = blockFactory();
        Executor executor = codecPool(1, 16);
        Page page = randomPage(factory, between(500, 3000));
        long pageBytes = page.ramBytesUsedByBlocks();
        DeferredChunkSink sink = new DeferredChunkSink(DeferredChunkSink.Mode.DEFERRED);
        PlainActionFuture<Long> done = new PlainActionFuture<>();
        SpillCodecTask task = submit(executor, page, sink, factory.breaker(), done);
        assertThat(task, notNullValue());
        assertBusy(() -> assertThat(sink.heldCount(), equalTo(1)));
        task.cancel();
        Exception e = expectThrows(Exception.class, () -> done.get(10, TimeUnit.SECONDS));
        assertThat(unwrap(e), instanceOf(TaskCancelledException.class));
        // the task released its buffers and its page reference; the operator's reference remains
        assertBusy(() -> assertThat(factory.breaker().getUsed(), equalTo(pageBytes)));
        // a late completion from the sink is harmless
        sink.release(1);
        assertContractKept(sink);
        page.releaseBlocks();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testCancellationBeforeTheTaskStarts() throws Exception {
        BlockFactory factory = blockFactory();
        Executor executor = codecPool(1, 16);
        Page blocker = randomPage(factory, between(500, 3000));
        Page page = randomPage(factory, between(100, 1000));
        long pageBytes = blocker.ramBytesUsedByBlocks() + page.ramBytesUsedByBlocks();
        DeferredChunkSink blockingSink = new DeferredChunkSink(DeferredChunkSink.Mode.DEFERRED);
        DeferredChunkSink sink = new DeferredChunkSink(DeferredChunkSink.Mode.DEFERRED);
        PlainActionFuture<Long> blockerDone = new PlainActionFuture<>();
        PlainActionFuture<Long> done = new PlainActionFuture<>();
        submit(executor, blocker, blockingSink, factory.breaker(), blockerDone);
        assertBusy(() -> assertThat(blockingSink.heldCount(), equalTo(1)));
        // the second task is queued behind the first and cancelled before it ever runs
        SpillCodecTask task = submit(executor, page, sink, factory.breaker(), done);
        task.cancel();
        releaseUntilDone(List.of(blockingSink), List.of(blockerDone), () -> {});
        Exception e = expectThrows(Exception.class, () -> done.get(10, TimeUnit.SECONDS));
        assertThat(unwrap(e), instanceOf(TaskCancelledException.class));
        assertThat(sink.appends(), equalTo(0));
        assertBusy(() -> assertThat(factory.breaker().getUsed(), equalTo(pageBytes)));
        blocker.releaseBlocks();
        page.releaseBlocks();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testSinkFailureFailsTheTask() throws Exception {
        BlockFactory factory = blockFactory();
        Executor executor = codecPool(1, 16);
        Page page = randomPage(factory, between(500, 3000));
        long pageBytes = page.ramBytesUsedByBlocks();
        DeferredChunkSink sink = new DeferredChunkSink(randomFrom(DeferredChunkSink.Mode.values()));
        sink.failNext(new IOException("boom"));
        PlainActionFuture<Long> done = new PlainActionFuture<>();
        submit(executor, page, sink, factory.breaker(), done);
        releaseUntilDone(List.of(sink), List.of(done), () -> {});
        Exception e = expectThrows(Exception.class, () -> done.get(10, TimeUnit.SECONDS));
        assertThat(unwrap(e).getMessage(), equalTo("boom"));
        assertBusy(() -> assertThat(factory.breaker().getUsed(), equalTo(pageBytes)));
        assertContractKept(sink);
        page.releaseBlocks();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testRejectionReachesTheListenerAndRetainsNothing() throws Exception {
        BlockFactory factory = blockFactory();
        Executor executor = codecPool(1, 1);
        int tasks = 6;
        List<Page> pages = new ArrayList<>();
        List<DeferredChunkSink> sinks = new ArrayList<>();
        List<PlainActionFuture<Long>> futures = new ArrayList<>();
        long pageBytes = 0;
        for (int i = 0; i < tasks; i++) {
            Page page = randomPage(factory, between(200, 1000));
            pages.add(page);
            pageBytes += page.ramBytesUsedByBlocks();
            DeferredChunkSink sink = new DeferredChunkSink(DeferredChunkSink.Mode.DEFERRED);
            sinks.add(sink);
            PlainActionFuture<Long> future = new PlainActionFuture<>();
            futures.add(future);
            submit(executor, page, sink, factory.breaker(), future);
        }
        releaseUntilDone(sinks, futures, () -> {});
        int rejected = 0;
        for (PlainActionFuture<Long> future : futures) {
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                assertThat(unwrap(e), instanceOf(EsRejectedExecutionException.class));
                rejected++;
            }
        }
        assertThat("a queue of one and one thread must reject some of " + tasks + " tasks", rejected, greaterThan(0));
        assertThat(factory.breaker().getUsed(), equalTo(pageBytes));
        pages.forEach(Page::releaseBlocks);
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testRejectionByAPlainExecutorReachesTheListener() {
        BlockFactory factory = blockFactory();
        Page page = randomPage(factory, between(100, 1000));
        long pageBytes = page.ramBytesUsedByBlocks();
        Executor rejecting = command -> { throw new RejectedExecutionException("no room"); };
        AtomicReference<Exception> failure = new AtomicReference<>();
        SpillCodecTask task = SpillCodecTask.submit(
            rejecting,
            page,
            VERSION,
            new DeferredChunkSink(DeferredChunkSink.Mode.INLINE),
            factory.breaker(),
            chunkSize,
            depth,
            null,
            ActionListener.wrap(bytes -> fail("must not succeed"), failure::set)
        );
        assertThat(task, notNullValue());
        assertThat(failure.get(), instanceOf(RejectedExecutionException.class));
        assertThat(factory.breaker().getUsed(), equalTo(pageBytes));
        page.releaseBlocks();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testPermitsBoundThePagesDetached() throws Exception {
        BlockFactory factory = blockFactory();
        Executor executor = codecPool(2, 16);
        SpillPermits permits = new SpillPermits(2);
        int tasks = 5;
        List<Page> pages = new ArrayList<>();
        List<DeferredChunkSink> sinks = new ArrayList<>();
        List<PlainActionFuture<Long>> futures = new ArrayList<>();
        for (int i = 0; i < tasks; i++) {
            Page page = randomPage(factory, between(200, 1000));
            pages.add(page);
            DeferredChunkSink sink = new DeferredChunkSink(DeferredChunkSink.Mode.DEFERRED);
            sinks.add(sink);
            PlainActionFuture<Long> future = new PlainActionFuture<>();
            futures.add(future);
            permits.acquire(
                future.delegateFailureAndWrap(
                    (l, permit) -> SpillCodecTask.submit(executor, page, VERSION, sink, factory.breaker(), chunkSize, depth, permit, l)
                )
            );
        }
        assertThat(permits.inUse(), equalTo(2));
        assertThat(permits.waiting(), equalTo(3));
        releaseUntilDone(sinks, futures, () -> assertThat(permits.inUse(), lessThanOrEqualTo(2)));
        for (PlainActionFuture<Long> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
        assertThat(permits.inUse(), equalTo(0));
        assertThat(permits.waiting(), equalTo(0));
        sinks.forEach(SpillCodecTaskTests::assertContractKept);
        pages.forEach(Page::releaseBlocks);
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testBuffersThatCannotBeChargedFailBeforeAnythingIsRetained() {
        BlockFactory factory = blockFactory(ByteSizeValue.ofKb(64));
        Executor executor = codecPool(1, 16);
        Page page = randomPage(factory, 100);
        long pageBytes = page.ramBytesUsedByBlocks();
        AtomicReference<Exception> failure = new AtomicReference<>();
        SpillCodecTask task = SpillCodecTask.submit(
            executor,
            page,
            VERSION,
            new DeferredChunkSink(DeferredChunkSink.Mode.INLINE),
            factory.breaker(),
            1024 * 1024,
            4,
            null,
            ActionListener.wrap(bytes -> fail("must not succeed"), failure::set)
        );
        assertThat(task, nullValue());
        assertThat(failure.get(), notNullValue());
        assertThat(factory.breaker().getUsed(), equalTo(pageBytes));
        page.releaseBlocks();
    }

    public void testReliefOfAnAliasedPageWaitsForTheAlias() throws Exception {
        BlockFactory factory = blockFactory();
        Executor executor = codecPool(1, 16);
        Page page = randomPage(factory, between(200, 1000));
        long pageBytes = page.ramBytesUsedByBlocks();
        Page alias = page.shallowCopy();
        PlainActionFuture<Long> done = new PlainActionFuture<>();
        submit(executor, page, new DeferredChunkSink(DeferredChunkSink.Mode.INLINE), factory.breaker(), done);
        done.get(10, TimeUnit.SECONDS);
        page.releaseBlocks();
        assertThat("a shallow copy still held keeps every byte", factory.breaker().getUsed(), equalTo(pageBytes));
        alias.releaseBlocks();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testReliefOfAnOrdinalBlockKeepsASharedDictionary() throws Exception {
        BlockFactory factory = blockFactory();
        Executor executor = codecPool(1, 16);
        int positions = between(200, 1000);
        BytesRefVector dictionary;
        try (BytesRefVector.Builder builder = factory.newBytesRefVectorBuilder(10)) {
            for (int i = 0; i < 10; i++) {
                builder.appendBytesRef(new BytesRef("value-" + i + "-" + randomAlphaOfLength(20)));
            }
            dictionary = builder.build();
        }
        IntBlock ordinals;
        try (IntBlock.Builder builder = factory.newIntBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                builder.appendInt(randomInt(9));
            }
            ordinals = builder.build();
        }
        long dictionaryBytes = dictionary.ramBytesUsed();
        dictionary.incRef(); // a second holder of the dictionary, like a sibling block sharing it
        Page page = new Page(new OrdinalBytesRefBlock(ordinals, dictionary));
        long pageBytes = page.ramBytesUsedByBlocks();
        PlainActionFuture<Long> done = new PlainActionFuture<>();
        submit(executor, page, new DeferredChunkSink(DeferredChunkSink.Mode.INLINE), factory.breaker(), done);
        done.get(10, TimeUnit.SECONDS);
        assertThat(factory.breaker().getUsed(), equalTo(pageBytes));
        page.releaseBlocks();
        assertThat("the ordinals are freed, the shared dictionary is not", factory.breaker().getUsed(), equalTo(dictionaryBytes));
        dictionary.decRef();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    /**
     * The full topology of the plan: the page comes from a driver's child factory over a view of the query breaker, and the
     * task releases its reference on the codec thread after the driver's local breaker and view are gone; the query and
     * the request accounts both return to baseline, with no release landing in a dead reserve.
     */
    public void testPageFromADriverFactoryReleasedAfterTheDriverClosed() throws Exception {
        BlockFactory nodeFactory = blockFactory();
        CircuitBreaker request = nodeFactory.breaker();
        BigArrays nodeArrays = nodeFactory.bigArrays();
        QueryBreaker query = new QueryBreaker(request, ByteSizeValue.ofMb(64).getBytes());
        QueryBreaker.View rootView = query.newView();
        MockBlockFactory queryFactory = new MockBlockFactory(
            BlockFactory.builder(nodeArrays.withBreakerService(rootView.service())).breaker(rootView)
        );
        QueryBreaker.View driverView = query.newView();
        LocalCircuitBreaker local = new LocalCircuitBreaker(driverView, 1024, 4096);
        MockBlockFactory driverFactory = (MockBlockFactory) queryFactory.newChildFactory(
            local,
            nodeArrays.withBreakerService(driverView.service())
        );
        Executor executor = codecPool(1, 16);

        Page page = randomPage(driverFactory, between(500, 3000));
        long pageBytes = page.ramBytesUsedByBlocks();
        assertThat(query.used(), greaterThan(pageBytes - 1));
        DeferredChunkSink sink = new DeferredChunkSink(DeferredChunkSink.Mode.DEFERRED);
        PlainActionFuture<Long> done = new PlainActionFuture<>();
        submit(executor, page, sink, rootView, done);
        assertBusy(() -> assertThat(sink.heldCount(), equalTo(1)));
        // the driver is done: its local breaker and view close while the task still holds the page
        page.releaseBlocks();
        local.close();
        driverView.close();
        releaseUntilDone(List.of(sink), List.of(done), () -> {});
        done.get(10, TimeUnit.SECONDS);
        assertContractKept(sink);
        rootView.close();
        assertThat(query.used(), equalTo(0L));
        assertThat(request.getUsed(), equalTo(0L));
        driverFactory.ensureAllBlocksAreReleased();
        queryFactory.ensureAllBlocksAreReleased();
    }

    private static Throwable unwrap(Exception e) {
        Throwable t = e;
        while (t.getCause() != null
            && t.getCause() != t
            && (t instanceof TaskCancelledException || t instanceof EsRejectedExecutionException) == false) {
            t = t.getCause();
        }
        return t;
    }
}
