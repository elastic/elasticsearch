/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.PartitionSpec;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.DAY_SPEC;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.HOUR_SPEC;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.LOGS_GLOB;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.NOW;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_DAY_END;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_DAY_START;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_GLOB;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_PREFIXES;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_SEVEN_DAY_END;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_SEVEN_DAY_WALK_BEFORE_PEEKS;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.expand;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.expandAsync;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.fieldShapedTree;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.folderRange;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.listingHints;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.wideHiveSevenDayTree;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.wideHiveTree;
import static org.elasticsearch.xpack.esql.datasources.glob.PartitionPruningWalk.LIST_PAGE_SIZE;
import static org.elasticsearch.xpack.esql.datasources.glob.PartitionPruningWalk.cheaperToFinish;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Worked checks for the finish-vs-flat page-count rule. The first survivor is already listed, so remaining
 * cost is {@code (S - 1) * ceil(k / P) <= ceil(L * k / P)}.
 */
public class PartitionPruningWalkTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final Instant LAST_DAY = Instant.parse("2026-10-14T00:00:00Z");

    @After
    public void clearFinishMode() {
        PartitionPruningWalk.clearFinishMode();
    }

    public void testSevenOfOneHundredTwentyFourDailyLeavesFinishes() {
        assertTrue(cheaperToFinish(7, 576, 124));
    }

    public void testOneHundredSixtyEightHourlyLeavesFlatten() {
        assertFalse(cheaperToFinish(168, 5, 2880));
    }

    public void testThreeOfTenLeavesFlatten() {
        assertFalse(cheaperToFinish(3, 5, 10));
    }

    public void testFiveOfOneHundredIdsFinishWhenObjectsFillPages() {
        assertTrue("k=50: 4 remaining pages <= ceil(100*50/1000)", cheaperToFinish(5, 50, 100));
        assertTrue("k=1000: 4 pages vs 100 prefix pages", cheaperToFinish(5, 1000, 100));
    }

    public void testFiveOfOneHundredIdsFlattenWhenEachDirIsOneObject() {
        assertFalse("k=1: 4 remaining pages > ceil(100/1000)=1 prefix page", cheaperToFinish(5, 1, 100));
    }

    public void testSingleSurvivorAlwaysFinishes() {
        assertTrue(cheaperToFinish(1, 1, 10));
        assertTrue(cheaperToFinish(1, 576, 124));
        assertTrue(cheaperToFinish(1, LIST_PAGE_SIZE + 1, 2));
        assertTrue("probe already paid; remaining cost is 0", cheaperToFinish(1, LIST_PAGE_SIZE + 1, 0.5));
    }

    public void testEmptySurvivorsFinish() {
        assertTrue(cheaperToFinish(0, 100, 10));
    }

    public void testPageSizeBoundary() {
        assertEquals(1, PartitionPruningWalk.ceilDiv(1, LIST_PAGE_SIZE));
        assertEquals(1, PartitionPruningWalk.ceilDiv(LIST_PAGE_SIZE, LIST_PAGE_SIZE));
        assertEquals(2, PartitionPruningWalk.ceilDiv(LIST_PAGE_SIZE + 1, LIST_PAGE_SIZE));
        assertEquals(0, PartitionPruningWalk.ceilDiv(0, LIST_PAGE_SIZE));
    }

    public void testEqualCostFinishes() {
        // 7 of 62 days, k=100, P=1000: 6 remaining == ceil(6.2) == 7? 6 <= 7
        assertTrue(cheaperToFinish(7, 100, 62));
    }

    public void testJustOverEqualFlattens() {
        // 8 remaining pages vs ceil(62*100/1000)=7
        assertFalse(cheaperToFinish(9, 100, 62));
    }

    public void testProbeAlreadyPaidFinishesWhenExAnteWouldFlatten() {
        // S=3, k=1001, L=4.5: ex-ante 6 vs 5 flattens; remaining 4 vs 5 finishes (totals 6 vs 7).
        assertTrue(cheaperToFinish(3, LIST_PAGE_SIZE + 1, 4.5));
    }

    public void testTwoSparseSurvivorsFinishAtEqualRemainingCost() {
        assertTrue("1 remaining page == ceil(100/1000)=1", cheaperToFinish(2, 1, 100));
    }

    public void testMultiSurvivorPagesWhenObjectsExceedPageSize() {
        assertTrue("remaining 4 <= ceil(10*1001/1000)=11", cheaperToFinish(3, LIST_PAGE_SIZE + 1, 10));
        assertFalse("remaining 4 > ceil(2*1001/1000)=3", cheaperToFinish(3, LIST_PAGE_SIZE + 1, 2));
    }

    public void testEmptyFirstListingStillCostsOnePage() {
        assertTrue(cheaperToFinish(1, 0, 10));
        assertFalse("S=5 empty dirs vs 1 prefix page", cheaperToFinish(5, 0, 100));
    }

    public void testFractionalPrefixWidth() {
        assertFalse("S=2, remaining two pages vs ceil(0.5*1001/1000)=1", cheaperToFinish(2, LIST_PAGE_SIZE + 1, 0.5));
        assertTrue(cheaperToFinish(1, 1, 1.5));
    }

    public void testSparseWideHiveLeavesStillFlatten() {
        // 154 matching day dirs, k=4: 153 remaining pages vs ~2 prefix pages of the synthetic tree.
        assertFalse(cheaperToFinish(154, 4, 462));
    }

    public void testParallelWalkMatchesSequentialOrderAndIssuedCount() throws Exception {
        List<StorageEntry> tree = fieldShapedTree();
        List<Expression> filters = List.of(window(LAST_DAY, NOW));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        int concurrency = 4;
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);
            for (int i = 0; i < 20; i++) {
                ListingWalkHarness sequential = new ListingWalkHarness(tree);
                PartitionPruningWalk.WalkResult sequentialResult = tryWalk(sequential, hints, pred, () -> false);

                ListingWalkHarness parallel = new ListingWalkHarness(tree);
                parallel.setListChildrenDelayNanos(() -> TimeUnit.MILLISECONDS.toNanos(randomIntBetween(0, 5)));
                PartitionPruningWalk.WalkResult parallelResult = tryWalkAsync(
                    parallel,
                    hints,
                    pred,
                    () -> false,
                    concurrency,
                    threadPool.generic()
                );

                assertNotNull(sequentialResult);
                assertNotNull(parallelResult);
                assertEquals(pathsOf(sequentialResult), pathsOf(parallelResult));
                assertEquals(sequential.listChildrenRequests(), parallel.listChildrenRequests());
                assertThat(parallel.maxListChildrenInFlight(), lessThanOrEqualTo(concurrency));
            }
        } finally {
            terminate(threadPool);
        }
    }

    public void testParallelWalkInFlightNeverExceedsConcurrency() throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(wideHiveTree());
        provider.setListChildrenDelayNanos(TimeUnit.MILLISECONDS.toNanos(5));
        FolderRangePredicate pred = folderRange(List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END)), DAY_SPEC);
        int concurrency = 4;
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            tryWalkAsync(provider, WIDE_HIVE_GLOB, List.of(), pred, () -> false, concurrency, threadPool.generic());
            assertThat(provider.maxListChildrenInFlight(), lessThanOrEqualTo(concurrency));
        } finally {
            terminate(threadPool);
        }
    }

    public void testParallelWalkCancelSkipsUnstartedSlots() throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(wideHiveTree());
        provider.setListChildrenDelayNanos(TimeUnit.MILLISECONDS.toNanos(10));
        FolderRangePredicate pred = folderRange(List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END)), DAY_SPEC);
        AtomicBoolean cancelled = new AtomicBoolean();
        int concurrency = 4;
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            PlainActionFuture<PartitionPruningWalk.WalkResult> future = new PlainActionFuture<>();
            startWalkAsync(provider, WIDE_HIVE_GLOB, List.of(), pred, cancelled::get, concurrency, threadPool.generic(), future);
            assertBusy(() -> assertThat(provider.listChildrenRequests(), greaterThan(0)));
            cancelled.set(true);
            int issuedAtCancel = provider.listChildrenRequests();
            expectThrows(TaskCancelledException.class, future::actionGet);
            int issuedAtComplete = provider.listChildrenRequests();
            assertThat(
                "in-flight may finish after cancel; unstarted must not LIST",
                issuedAtComplete,
                lessThanOrEqualTo(issuedAtCancel + concurrency)
            );
            assertThat("cancel must skip the rest of the tree", issuedAtComplete, lessThan(WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES));
            assertEquals("in-flight listings must release after the future completes", 0, provider.listChildrenInFlight());
        } finally {
            terminate(threadPool);
        }
    }

    public void testAsyncWalkThrowingCancelCheckCompletesListener() throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(wideHiveTree());
        FolderRangePredicate pred = folderRange(List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END)), DAY_SPEC);
        int throwAfter = randomBoolean() ? 0 : 1;
        AtomicInteger checks = new AtomicInteger();
        BooleanSupplier boom = () -> {
            if (checks.getAndIncrement() >= throwAfter) {
                throw new RuntimeException("cancel check");
            }
            return false;
        };
        int concurrency = 4;
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            PlainActionFuture<PartitionPruningWalk.WalkResult> future = new PlainActionFuture<>();
            startWalkAsync(provider, WIDE_HIVE_GLOB, List.of(), pred, boom, concurrency, threadPool.generic(), future);
            RuntimeException e = expectThrows(RuntimeException.class, future::actionGet);
            assertEquals("cancel check", e.getMessage());
            assertEquals("throwing cancel must still release ThrottledIterator slots", 0, provider.listChildrenInFlight());
        } finally {
            terminate(threadPool);
        }
    }

    public void testParallelWalkFailedChildFailsClosed() throws Exception {
        List<StorageEntry> tree = fieldShapedTree();
        List<Expression> filters = List.of(window(LAST_DAY, NOW));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);

        ListingWalkHarness sequential = new ListingWalkHarness(tree);
        sequential.setFailListChildrenContaining("/month=");
        assertInjectedListingFailureWithoutFlatListing(() -> tryWalk(sequential, hints, pred, () -> false), sequential);

        ListingWalkHarness parallel = new ListingWalkHarness(tree);
        parallel.setFailListChildrenContaining("/month=");
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            assertInjectedListingFailureWithoutFlatListing(
                () -> tryWalkAsync(parallel, hints, pred, () -> false, 4, threadPool.generic()),
                parallel
            );
        } finally {
            terminate(threadPool);
        }
    }

    public void testEmptyPathSegmentWithdrawsWithoutFlatListing() throws Exception {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/logs/year=2026/month=10/day=14/f.parquet"),
            ListingWalkHarness.entry("s3://bucket/logs/year=2026/month=10/day=14//ghost.parquet")
        );
        List<Expression> filters = List.of(window(LAST_DAY, NOW));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);
        ListingWalkHarness sequential = new ListingWalkHarness(tree);
        assertNull(tryWalk(sequential, hints, pred, () -> false));
        assertEquals(0, sequential.listObjectPages());

        ListingWalkHarness parallel = new ListingWalkHarness(tree);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            assertNull(tryWalkAsync(parallel, hints, pred, () -> false, 4, threadPool.generic()));
            assertEquals(0, parallel.listObjectPages());
        } finally {
            terminate(threadPool);
        }
    }

    public void testParallelWalkNullChildSkipsUnstarted() throws Exception {
        List<StorageEntry> tree = wideHiveTree();
        FolderRangePredicate pred = folderRange(List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END)), DAY_SPEC);
        ListingWalkHarness sequential = new ListingWalkHarness(tree);
        sequential.setNullListChildrenAfter(1);
        assertNull(tryWalk(sequential, WIDE_HIVE_GLOB, List.of(), pred, () -> false));
        int sequentialIssued = sequential.listChildrenRequests();

        ListingWalkHarness parallel = new ListingWalkHarness(tree);
        parallel.setNullListChildrenAfter(1);
        parallel.setListChildrenDelayNanos(TimeUnit.MILLISECONDS.toNanos(5));
        int concurrency = 4;
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            assertNull(tryWalkAsync(parallel, WIDE_HIVE_GLOB, List.of(), pred, () -> false, concurrency, threadPool.generic()));
            assertThat(
                "null child must skip unstarted siblings, not LIST the rest of the level",
                parallel.listChildrenRequests(),
                lessThanOrEqualTo(sequentialIssued + concurrency - 1)
            );
            assertThat(parallel.listChildrenRequests(), lessThan(WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES));
        } finally {
            terminate(threadPool);
        }
    }

    public void testExpandPropagatesUnavailableFromWalk() {
        ListingWalkHarness provider = new ListingWalkHarness(fieldShapedTree());
        List<Expression> filters = List.of(window(LAST_DAY, NOW));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        provider.setFailListChildrenContaining("/");
        provider.setFailListChildrenUnavailable(true);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            PlainActionFuture<FileList> future = new PlainActionFuture<>();
            expandAsync(provider, LOGS_GLOB, listingHints(filters, DAY_SPEC), Integer.MAX_VALUE, pred, 4, threadPool.generic(), future);
            expectThrows(ExternalUnavailableException.class, future::actionGet);
            assertEquals(0, provider.listObjectPages());
        } finally {
            terminate(threadPool);
        }
    }

    public void testExpandPropagatesIOExceptionFromWalkWithoutFlatListing() throws Exception {
        List<Expression> filters = List.of(window(LAST_DAY, NOW));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);

        ListingWalkHarness sequential = new ListingWalkHarness(fieldShapedTree());
        sequential.setFailListChildrenContaining("/month=");
        assertInjectedListingFailureWithoutFlatListing(
            () -> expand(sequential, LOGS_GLOB, hints, Integer.MAX_VALUE, pred, PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS),
            sequential
        );

        ListingWalkHarness parallel = new ListingWalkHarness(fieldShapedTree());
        parallel.setFailListChildrenContaining("/month=");
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            PlainActionFuture<FileList> future = new PlainActionFuture<>();
            expandAsync(parallel, LOGS_GLOB, hints, Integer.MAX_VALUE, pred, 4, threadPool.generic(), future);
            assertInjectedListingFailureWithoutFlatListing(future::actionGet, parallel);
        } finally {
            terminate(threadPool);
        }
    }

    public void testExpandPropagatesDiscoveredFilesLimitFromWalkWithoutFlatListing() throws Exception {
        List<Expression> filters = List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);
        String prefixListing = ListingWalkHarness.WIDE_HIVE_PREFIX + "/";

        ListingWalkHarness sequential = new ListingWalkHarness(wideHiveTree());
        expectThrows(
            DiscoveredFilesLimitException.class,
            () -> PartitionPruningWalk.withFinishMode(
                PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
                () -> expand(sequential, WIDE_HIVE_GLOB, hints, 0, pred, PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS)
            )
        );
        assertFalse("file cap must not prefix-list the dataset", sequential.listedPrefixes().contains(prefixListing));

        ListingWalkHarness parallel = new ListingWalkHarness(wideHiveTree());
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            PlainActionFuture<FileList> future = new PlainActionFuture<>();
            PartitionPruningWalk.withFinishMode(PartitionPruningWalk.FinishMode.ALWAYS_FINISH, () -> {
                expandAsync(parallel, WIDE_HIVE_GLOB, hints, 0, pred, 4, threadPool.generic(), future);
                return null;
            });
            expectThrows(DiscoveredFilesLimitException.class, future::actionGet);
            assertFalse("async file cap must not prefix-list the dataset", parallel.listedPrefixes().contains(prefixListing));
        } finally {
            terminate(threadPool);
        }
    }

    public void testAsyncWalkDoesNotNeedKeyOrder() throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(fieldShapedTree());
        provider.setListsInKeyOrder(false);
        List<Expression> filters = List.of(window(LAST_DAY, NOW));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        int concurrency = 4;
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            PlainActionFuture<FileList> future = new PlainActionFuture<>();
            expandAsync(
                provider,
                LOGS_GLOB,
                listingHints(filters, DAY_SPEC),
                Integer.MAX_VALUE,
                pred,
                concurrency,
                threadPool.generic(),
                future
            );
            FileList listing = future.actionGet();
            assertNotNull(listing);
            assertThat("walk must run even when listsInKeyOrder is false", provider.listChildrenRequests(), greaterThan(0));
            assertThat(provider.maxListChildrenInFlight(), lessThanOrEqualTo(concurrency));
        } finally {
            terminate(threadPool);
        }
    }

    public void testDirectExecutorWalkStaysSequential() throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(fieldShapedTree());
        List<Expression> filters = List.of(window(LAST_DAY, NOW));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        PartitionPruningWalk.WalkResult result = tryWalkAsync(
            provider,
            listingHints(filters, DAY_SPEC),
            pred,
            () -> false,
            8,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        assertNotNull(result);
        assertEquals(1, provider.maxListChildrenInFlight());
    }

    public void testAsyncListenerCompletesOnceWhenOnResponseThrows() throws Exception {
        // Sequential listChildrenParallel on the calling thread. A throwing listener
        // inside ThrottledIterator completion is a contract violation, not a supported path.
        assertListenerCompletesOnce(EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    public void testDiscoveredFilesLimitPropagatesWithoutFlatListing() throws Exception {
        List<Expression> filters = List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        String prefixListing = ListingWalkHarness.WIDE_HIVE_PREFIX + "/";

        ListingWalkHarness sequential = new ListingWalkHarness(wideHiveTree());
        expectThrows(
            DiscoveredFilesLimitException.class,
            () -> PartitionPruningWalk.withFinishMode(
                PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
                () -> tryWalk(
                    sequential,
                    WIDE_HIVE_GLOB,
                    listingHints(filters, DAY_SPEC),
                    pred,
                    () -> false,
                    0,
                    PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS
                )
            )
        );
        assertFalse("file cap must not prefix-list the dataset", sequential.listedPrefixes().contains(prefixListing));

        ListingWalkHarness parallel = new ListingWalkHarness(wideHiveTree());
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            expectThrows(
                DiscoveredFilesLimitException.class,
                () -> PartitionPruningWalk.withFinishMode(
                    PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
                    () -> tryWalkAsync(
                        parallel,
                        WIDE_HIVE_GLOB,
                        listingHints(filters, DAY_SPEC),
                        pred,
                        () -> false,
                        4,
                        threadPool.generic(),
                        0,
                        PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS
                    )
                )
            );
            assertFalse("async file cap must not prefix-list the dataset", parallel.listedPrefixes().contains(prefixListing));
        } finally {
            terminate(threadPool);
        }
    }

    public void testListChildrenRuntimeExceptionPropagatesWithoutFlatListing() throws Exception {
        List<Expression> filters = List.of(window(LAST_DAY, NOW));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);

        ListingWalkHarness sequential = new ListingWalkHarness(fieldShapedTree());
        sequential.setFailListChildrenContaining("/month=");
        sequential.setFailListChildrenRuntime(true);
        RuntimeException sync = expectThrows(RuntimeException.class, () -> tryWalk(sequential, hints, pred, () -> false));
        assertEquals("injected listing failure", sync.getMessage());
        assertEquals(0, sequential.listObjectPages());

        ListingWalkHarness parallel = new ListingWalkHarness(fieldShapedTree());
        parallel.setFailListChildrenContaining("/month=");
        parallel.setFailListChildrenRuntime(true);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            RuntimeException async = expectThrows(
                RuntimeException.class,
                () -> tryWalkAsync(parallel, hints, pred, () -> false, 4, threadPool.generic())
            );
            assertEquals("injected listing failure", async.getMessage());
            assertEquals(0, parallel.listObjectPages());
        } finally {
            terminate(threadPool);
        }
    }

    public void testParallelWideHiveOneDayMatchesSequential() throws Exception {
        assertParallelWalkMatchesSequential(
            wideHiveTree(),
            List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END)),
            DAY_SPEC,
            WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES,
            5
        );
    }

    public void testParallelWideHiveSevenDayMatchesSequential() throws Exception {
        assertParallelWalkMatchesSequential(
            wideHiveSevenDayTree(),
            List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_SEVEN_DAY_END)),
            HOUR_SPEC,
            WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES,
            5
        );
    }

    public void testParallelCeilingBoundaryCompletesAtOneDayWalk() throws Exception {
        List<Expression> filters = List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        ListingWalkHarness provider = new ListingWalkHarness(wideHiveTree());
        provider.setListChildrenDelayNanos(() -> TimeUnit.MILLISECONDS.toNanos(randomIntBetween(0, 5)));
        int concurrency = randomIntBetween(1, 16);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            PartitionPruningWalk.WalkResult result = PartitionPruningWalk.withFinishMode(
                PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
                () -> tryWalkAsync(
                    provider,
                    WIDE_HIVE_GLOB,
                    List.of(),
                    pred,
                    () -> false,
                    concurrency,
                    threadPool.generic(),
                    Integer.MAX_VALUE,
                    WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES
                )
            );
            assertNotNull("879 listings must complete the 1-day walk", result);
            assertEquals(WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES, provider.listChildrenRequests());
        } finally {
            terminate(threadPool);
        }
    }

    public void testParallelCeilingWithdrawsWithoutIssuingPastBudget() throws Exception {
        int ceiling = WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES - 1;
        List<Expression> filters = List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        ListingWalkHarness provider = new ListingWalkHarness(wideHiveTree());
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            PartitionPruningWalk.WalkResult result = tryWalkAsync(
                provider,
                WIDE_HIVE_GLOB,
                List.of(),
                pred,
                () -> false,
                16,
                threadPool.generic(),
                Integer.MAX_VALUE,
                ceiling
            );
            assertNull("878 listings must withdraw to a flat listing", result);
            assertThat(provider.listChildrenRequests(), lessThanOrEqualTo(ceiling));
            assertEquals(0, provider.listObjectPages());
        } finally {
            terminate(threadPool);
        }
    }

    public void testParallelPeekCeilingIssuesRemainingBudgetThenFailsClosed() throws Exception {
        int ceiling = WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES - 1;
        FolderRangePredicate pred = folderRange(List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_SEVEN_DAY_END)), HOUR_SPEC);

        ListingWalkHarness sequential = new ListingWalkHarness(wideHiveSevenDayTree());
        assertNull(tryWalk(sequential, WIDE_HIVE_GLOB, List.of(), pred, () -> false, Integer.MAX_VALUE, ceiling));
        assertEquals(ceiling, sequential.listChildrenRequests());
        assertEquals(WIDE_HIVE_PREFIXES - 1, julyDayPeekCount(sequential));
        assertEquals(0, sequential.listObjectPages());

        ListingWalkHarness parallel = new ListingWalkHarness(wideHiveSevenDayTree());
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            assertNull(tryWalkAsync(parallel, WIDE_HIVE_GLOB, List.of(), pred, () -> false, 8, executor, Integer.MAX_VALUE, ceiling));
            assertEquals(ceiling, parallel.listChildrenRequests());
            assertEquals(WIDE_HIVE_PREFIXES - 1, julyDayPeekCount(parallel));
            assertEquals(0, parallel.listObjectPages());
        } finally {
            terminate(executor);
        }
    }

    public void testParallelPeeksInFlightNeverExceedsConcurrency() throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(wideHiveSevenDayTree());
        provider.setDelayListChildrenContaining("/month=07/day=");
        CountDownLatch overlapped = new CountDownLatch(2);
        provider.setListChildrenDelayNanos(() -> {
            overlapped.countDown();
            try {
                overlapped.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return 0L;
        });
        FolderRangePredicate pred = folderRange(List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_SEVEN_DAY_END)), HOUR_SPEC);
        int concurrency = 4;
        int poolSize = concurrency * 2;
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        try {
            PartitionPruningWalk.WalkResult result = PartitionPruningWalk.withFinishMode(
                PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
                () -> tryWalkAsync(provider, WIDE_HIVE_GLOB, List.of(), pred, () -> false, concurrency, executor)
            );
            assertNotNull(result);
            assertEquals(WIDE_HIVE_PREFIXES, julyDayPeekCount(provider));
            assertThat(provider.maxListChildrenInFlight(), lessThanOrEqualTo(concurrency));
            assertThat(
                "peek LISTs must overlap when the pool is larger than concurrency",
                provider.maxListChildrenInFlight(),
                greaterThan(1)
            );
            assertEquals(WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES, provider.listChildrenRequests());
            assertEquals(0, provider.listChildrenInFlight());
        } finally {
            terminate(executor);
        }
    }

    public void testParallelPeekCancelSkipsUnstarted() throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(wideHiveSevenDayTree());
        CountDownLatch firstPeek = new CountDownLatch(1);
        CountDownLatch releasePeeks = new CountDownLatch(1);
        provider.setDelayListChildrenContaining("/month=07/day=");
        provider.setListChildrenDelayNanos(() -> {
            firstPeek.countDown();
            awaitQuietly(releasePeeks);
            return 0L;
        });
        FolderRangePredicate pred = folderRange(List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_SEVEN_DAY_END)), HOUR_SPEC);
        AtomicBoolean cancelled = new AtomicBoolean();
        int concurrency = 4;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        try {
            PlainActionFuture<PartitionPruningWalk.WalkResult> future = new PlainActionFuture<>();
            startWalkAsync(provider, WIDE_HIVE_GLOB, List.of(), pred, cancelled::get, concurrency, executor, future);
            assertTrue("July day peek batch must start", firstPeek.await(30, TimeUnit.SECONDS));
            assertThat(julyDayPeekCount(provider), greaterThan(0));
            cancelled.set(true);
            int issuedAtCancel = provider.listChildrenRequests();
            releasePeeks.countDown();
            expectThrows(TaskCancelledException.class, future::actionGet);
            int issuedAtComplete = provider.listChildrenRequests();
            assertThat(issuedAtComplete, lessThanOrEqualTo(issuedAtCancel + concurrency));
            assertThat(issuedAtComplete, lessThan(WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES));
            assertThat(julyDayPeekCount(provider), lessThan(WIDE_HIVE_PREFIXES));
            assertEquals(0, provider.listObjectPages());
            assertEquals(0, provider.listChildrenInFlight());
        } finally {
            releasePeeks.countDown();
            terminate(executor);
        }
    }

    public void testListChildrenParallelCancelAfterAllStartedDoesNotSucceed() throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(
            List.of(ListingWalkHarness.entry("s3://bucket/a/f.parquet"), ListingWalkHarness.entry("s3://bucket/b/f.parquet"))
        );
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        provider.setListChildrenDelayNanos(() -> {
            bothStarted.countDown();
            awaitQuietly(release);
            return 0L;
        });
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger issued = new AtomicInteger();
        List<StoragePath> dirs = List.of(StoragePath.of("s3://bucket/a"), StoragePath.of("s3://bucket/b"));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            PlainActionFuture<StorageChildren[]> future = new PlainActionFuture<>();
            PartitionPruningWalk.listChildrenParallel(provider, dirs, 2, executor, issued, cancelled::get, future);
            assertTrue(bothStarted.await(30, TimeUnit.SECONDS));
            cancelled.set(true);
            release.countDown();
            expectThrows(TaskCancelledException.class, future::actionGet);
            assertEquals("both slots already in-flight; unstarted skip does not apply", 2, issued.get());
            assertEquals(2, provider.listChildrenRequests());
            assertEquals(0, provider.listChildrenInFlight());
        } finally {
            release.countDown();
            terminate(executor);
        }
    }

    public void testParallelPeekFailedChildFailsClosed() throws Exception {
        FolderRangePredicate pred = folderRange(List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_SEVEN_DAY_END)), HOUR_SPEC);
        ListingWalkHarness sequential = new ListingWalkHarness(wideHiveSevenDayTree());
        sequential.setFailListChildrenContaining("/month=07/day=");
        assertInjectedListingFailureWithoutFlatListing(() -> tryWalk(sequential, WIDE_HIVE_GLOB, List.of(), pred, () -> false), sequential);
        assertEquals(WIDE_HIVE_SEVEN_DAY_WALK_BEFORE_PEEKS, sequential.listChildrenRequests());
        assertEquals(0, julyDayPeekCount(sequential));

        ListingWalkHarness parallel = new ListingWalkHarness(wideHiveSevenDayTree());
        parallel.setFailListChildrenContaining("/month=07/day=");
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            assertInjectedListingFailureWithoutFlatListing(
                () -> tryWalkAsync(parallel, WIDE_HIVE_GLOB, List.of(), pred, () -> false, 4, executor),
                parallel
            );
            assertEquals(WIDE_HIVE_SEVEN_DAY_WALK_BEFORE_PEEKS, parallel.listChildrenRequests());
            assertEquals(0, julyDayPeekCount(parallel));
        } finally {
            terminate(executor);
        }
    }

    public void testListChildrenParallelPrefersUnavailableOverNullChild() throws Exception {
        assertPrefersUnavailableOverNullChild(true);
        assertPrefersUnavailableOverNullChild(false);
    }

    private void assertPrefersUnavailableOverNullChild(boolean delayUnavailable) throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(
            List.of(ListingWalkHarness.entry("s3://bucket/a/f.parquet"), ListingWalkHarness.entry("s3://bucket/b/f.parquet"))
        );
        provider.setFailListChildrenContaining("/a");
        provider.setFailListChildrenUnavailable(true);
        provider.setNullListChildrenContaining("/b");
        CountDownLatch startedSlow = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        provider.setDelayListChildrenContaining(delayUnavailable ? "/a" : "/b");
        provider.setListChildrenDelayNanos(() -> {
            startedSlow.countDown();
            awaitQuietly(releaseSlow);
            return 0L;
        });
        AtomicInteger issued = new AtomicInteger();
        List<StoragePath> dirs = List.of(StoragePath.of("s3://bucket/a"), StoragePath.of("s3://bucket/b"));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            PlainActionFuture<StorageChildren[]> future = new PlainActionFuture<>();
            PartitionPruningWalk.listChildrenParallel(provider, dirs, 2, executor, issued, () -> false, future);
            assertTrue(startedSlow.await(30, TimeUnit.SECONDS));
            if (delayUnavailable) {
                assertBusy(() -> assertTrue(provider.childListedPrefixes().stream().anyMatch(p -> p.contains("/b"))));
            } else {
                assertBusy(() -> assertThat(issued.get(), greaterThan(1)));
            }
            releaseSlow.countDown();
            expectThrows(ExternalUnavailableException.class, future::actionGet);
            assertEquals(0, provider.listObjectPages());
        } finally {
            releaseSlow.countDown();
            terminate(executor);
        }
    }

    public void testParallelPeeksMatchSequentialUnderPeekJitter() throws Exception {
        FolderRangePredicate pred = folderRange(List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_SEVEN_DAY_END)), HOUR_SPEC);
        ListingWalkHarness sequentialProvider = new ListingWalkHarness(wideHiveSevenDayTree());
        PartitionPruningWalk.WalkResult sequential = PartitionPruningWalk.withFinishMode(
            PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
            () -> tryWalk(sequentialProvider, WIDE_HIVE_GLOB, List.of(), pred, () -> false)
        );
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            for (int i = 0; i < 5; i++) {
                ListingWalkHarness parallelProvider = new ListingWalkHarness(wideHiveSevenDayTree());
                parallelProvider.setDelayListChildrenContaining("/month=07/day=");
                parallelProvider.setListChildrenDelayNanos(() -> TimeUnit.MILLISECONDS.toNanos(randomIntBetween(0, 2)));
                int concurrency = randomIntBetween(2, 8);
                PartitionPruningWalk.WalkResult parallel = PartitionPruningWalk.withFinishMode(
                    PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
                    () -> tryWalkAsync(parallelProvider, WIDE_HIVE_GLOB, List.of(), pred, () -> false, concurrency, executor)
                );
                assertEquals(sequential, parallel);
                assertEquals(WIDE_HIVE_PREFIXES, julyDayPeekCount(parallelProvider));
                assertEquals(WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES, parallelProvider.listChildrenRequests());
            }
        } finally {
            terminate(executor);
        }
    }

    private void assertListenerCompletesOnce(Executor executor) throws Exception {
        ListingWalkHarness provider = new ListingWalkHarness(fieldShapedTree());
        List<Expression> filters = List.of(window(LAST_DAY, NOW));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        AtomicInteger completions = new AtomicInteger();
        ActionListener<PartitionPruningWalk.WalkResult> listener = new ActionListener<>() {
            @Override
            public void onResponse(PartitionPruningWalk.WalkResult walk) {
                completions.incrementAndGet();
                throw new RuntimeException("downstream onResponse");
            }

            @Override
            public void onFailure(Exception e) {
                completions.incrementAndGet();
            }
        };
        try {
            startWalkAsync(
                provider,
                LOGS_GLOB,
                listingHints(filters, DAY_SPEC),
                pred,
                () -> false,
                1,
                executor,
                listener,
                Integer.MAX_VALUE,
                PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS
            );
            fail("throwing onResponse should surface to the caller on the sequential path");
        } catch (AssertionError e) {
            // ActionListener.completeWith asserts when onResponse throws.
            assertTrue(e.getMessage(), e.getMessage().contains("downstream onResponse"));
        } catch (RuntimeException e) {
            assertEquals("downstream onResponse", e.getMessage());
        }
        assertEquals("throwing onResponse must not also complete onFailure", 1, completions.get());
    }

    private void assertParallelWalkMatchesSequential(
        List<StorageEntry> tree,
        List<Expression> filters,
        PartitionSpec spec,
        int expectedListings,
        int iterations
    ) throws Exception {
        FolderRangePredicate pred = folderRange(filters, spec);
        ListingWalkHarness sequentialProvider = new ListingWalkHarness(tree);
        PartitionPruningWalk.WalkResult sequential = PartitionPruningWalk.withFinishMode(
            PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
            () -> tryWalk(sequentialProvider, WIDE_HIVE_GLOB, List.of(), pred, () -> false)
        );
        assertNotNull(sequential);
        assertEquals(expectedListings, sequentialProvider.listChildrenRequests());

        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            for (int i = 0; i < iterations; i++) {
                ListingWalkHarness parallelProvider = new ListingWalkHarness(tree);
                parallelProvider.setListChildrenDelayNanos(() -> TimeUnit.MILLISECONDS.toNanos(randomIntBetween(0, 1)));
                int concurrency = randomIntBetween(2, 16);
                PartitionPruningWalk.WalkResult parallel = PartitionPruningWalk.withFinishMode(
                    PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
                    () -> tryWalkAsync(parallelProvider, WIDE_HIVE_GLOB, List.of(), pred, () -> false, concurrency, threadPool.generic())
                );
                assertEquals(sequential, parallel);
                assertEquals(expectedListings, parallelProvider.listChildrenRequests());
            }
        } finally {
            terminate(threadPool);
        }
    }

    private static void assertInjectedListingFailureWithoutFlatListing(CheckedRunnable<Exception> walk, ListingWalkHarness provider) {
        Exception thrown = expectThrows(Exception.class, walk::run);
        Throwable io = thrown instanceof IOException ? thrown : ExceptionsHelper.unwrap(thrown, IOException.class);
        assertNotNull("expected IOException, got " + thrown, io);
        assertEquals("injected listing failure", io.getMessage());
        assertEquals(0, provider.listObjectPages());
    }

    private static PartitionPruningWalk.WalkResult tryWalk(
        ListingWalkHarness provider,
        List<PartitionFilterHint> hints,
        FolderRangePredicate pred,
        BooleanSupplier cancelled
    ) throws IOException {
        return tryWalk(provider, LOGS_GLOB, hints, pred, cancelled);
    }

    private static PartitionPruningWalk.WalkResult tryWalk(
        ListingWalkHarness provider,
        String glob,
        List<PartitionFilterHint> hints,
        FolderRangePredicate pred,
        BooleanSupplier cancelled
    ) throws IOException {
        return tryWalk(provider, glob, hints, pred, cancelled, Integer.MAX_VALUE, PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS);
    }

    private static PartitionPruningWalk.WalkResult tryWalk(
        ListingWalkHarness provider,
        String glob,
        List<PartitionFilterHint> hints,
        FolderRangePredicate pred,
        BooleanSupplier cancelled,
        int maxDiscoveredFiles,
        int maxDirectoryListings
    ) throws IOException {
        StoragePath globPath = StoragePath.of(glob);
        return PartitionPruningWalk.tryWalk(
            provider,
            globPath.patternPrefix(),
            new GlobMatcher(globPath.globPart()),
            ExclusionConfig.fromConfig(null).compile(),
            hints,
            maxDiscoveredFiles,
            globPath.globPart(),
            pred,
            null,
            Integer.MAX_VALUE,
            maxDirectoryListings,
            cancelled
        );
    }

    private static PartitionPruningWalk.WalkResult tryWalkAsync(
        ListingWalkHarness provider,
        List<PartitionFilterHint> hints,
        FolderRangePredicate pred,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor
    ) {
        return tryWalkAsync(provider, LOGS_GLOB, hints, pred, cancelled, concurrency, executor);
    }

    private static PartitionPruningWalk.WalkResult tryWalkAsync(
        ListingWalkHarness provider,
        String glob,
        List<PartitionFilterHint> hints,
        FolderRangePredicate pred,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor
    ) {
        return tryWalkAsync(
            provider,
            glob,
            hints,
            pred,
            cancelled,
            concurrency,
            executor,
            Integer.MAX_VALUE,
            PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS
        );
    }

    private static PartitionPruningWalk.WalkResult tryWalkAsync(
        ListingWalkHarness provider,
        String glob,
        List<PartitionFilterHint> hints,
        FolderRangePredicate pred,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor,
        int maxDiscoveredFiles,
        int maxDirectoryListings
    ) {
        PlainActionFuture<PartitionPruningWalk.WalkResult> future = new PlainActionFuture<>();
        startWalkAsync(provider, glob, hints, pred, cancelled, concurrency, executor, future, maxDiscoveredFiles, maxDirectoryListings);
        return future.actionGet();
    }

    private static void startWalkAsync(
        ListingWalkHarness provider,
        String glob,
        List<PartitionFilterHint> hints,
        FolderRangePredicate pred,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor,
        ActionListener<PartitionPruningWalk.WalkResult> listener
    ) {
        startWalkAsync(
            provider,
            glob,
            hints,
            pred,
            cancelled,
            concurrency,
            executor,
            listener,
            Integer.MAX_VALUE,
            PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS
        );
    }

    private static void startWalkAsync(
        ListingWalkHarness provider,
        String glob,
        List<PartitionFilterHint> hints,
        FolderRangePredicate pred,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor,
        ActionListener<PartitionPruningWalk.WalkResult> listener,
        int maxDiscoveredFiles,
        int maxDirectoryListings
    ) {
        StoragePath globPath = StoragePath.of(glob);
        PartitionPruningWalk.tryWalkAsync(
            provider,
            globPath.patternPrefix(),
            new GlobMatcher(globPath.globPart()),
            ExclusionConfig.fromConfig(null).compile(),
            hints,
            maxDiscoveredFiles,
            globPath.globPart(),
            pred,
            null,
            Integer.MAX_VALUE,
            maxDirectoryListings,
            cancelled,
            concurrency,
            executor,
            listener
        );
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            if (latch.await(30, TimeUnit.SECONDS) == false) {
                throw new AssertionError("timed out waiting for peek latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for peek latch", e);
        }
    }

    private static int julyDayPeekCount(ListingWalkHarness provider) {
        int n = 0;
        for (String prefix : provider.childListedPrefixes()) {
            if (prefix.contains("/month=07/day=")) {
                n++;
            }
        }
        return n;
    }

    private static List<String> pathsOf(PartitionPruningWalk.WalkResult result) {
        List<String> paths = new ArrayList<>(result.matched().size());
        for (StorageEntry entry : result.matched()) {
            paths.add(entry.path().toString());
        }
        return paths;
    }

    private static Expression window(Instant start, Instant end) {
        return new And(SRC, gte(start), lt(end));
    }

    private static Expression gte(Instant start) {
        return new GreaterThanOrEqual(SRC, timestamp(), new Literal(SRC, start.toEpochMilli(), DataType.DATETIME));
    }

    private static Expression lt(Instant end) {
        return new LessThan(SRC, timestamp(), new Literal(SRC, end.toEpochMilli(), DataType.DATETIME));
    }

    private static FieldAttribute timestamp() {
        return new FieldAttribute(
            SRC,
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }
}
