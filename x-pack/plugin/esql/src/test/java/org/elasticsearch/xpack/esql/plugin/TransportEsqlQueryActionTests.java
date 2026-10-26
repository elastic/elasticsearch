/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.EXTERNAL_IO_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;

public class TransportEsqlQueryActionTests extends ComputeTestCase {

    /**
     * All external blob-store access — {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver} glob
     * expansion, footer reads, and schema reconciliation, plus the blocking data reads and streaming parse pipeline
     * routed through {@link org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry#fileReadExecutor()} —
     * runs on the dedicated pool named by {@link EsqlPlugin#externalBlobStorePool()} ({@code esql_external_io}), with
     * in-flight cloud calls additionally bounded by the per-scheme permit semaphore in {@code StorageProviderRegistry}.
     * It must never be {@link ThreadPool.Names#SEARCH} (a single wildcard query over thousands of files previously
     * consumed nearly the entire SEARCH pool, starving unrelated searches) nor {@link ThreadPool.Names#GENERIC} (which
     * lets a single heavy external query starve the rest of the node); the explicit assertions catch either
     * regression.
     */
    public void testExternalBlobStorePoolIsTheDedicatedExternalIoPool() {
        assertEquals(EXTERNAL_IO_THREAD_POOL_NAME, EsqlPlugin.externalBlobStorePool());
        assertNotEquals(
            "external blob-store access must not run on the shared search pool",
            ThreadPool.Names.SEARCH,
            EsqlPlugin.externalBlobStorePool()
        );
        assertNotEquals(
            "external blob-store access must not run on the shared generic pool",
            ThreadPool.Names.GENERIC,
            EsqlPlugin.externalBlobStorePool()
        );
    }

    /**
     * ES|QL compute — driver execution and the parallel worker fan-out — runs on {@link EsqlPlugin#computePool()}
     * ({@code esql_worker}). This must be a <em>different</em> pool from {@link EsqlPlugin#externalBlobStorePool()}:
     * the blocking external I/O and streaming parse pipeline (segmentator + parser tasks) would otherwise occupy the
     * same threads as the compute drivers that consume their output and deadlock the query (the heap-attack external
     * stall). This test pins that separation.
     */
    public void testComputePoolIsSeparateFromExternalBlobStorePool() {
        assertEquals(ESQL_WORKER_THREAD_POOL_NAME, EsqlPlugin.computePool());
        assertNotEquals(
            "compute must not share the external blob-store pool, or the parse pipeline can starve the drivers",
            EsqlPlugin.externalBlobStorePool(),
            EsqlPlugin.computePool()
        );
    }

    /**
     * A cancellation that lands after the drivers finished but before the response is built must not leak the result's
     * blocks: nothing upstream releases them once the exception is thrown here. {@link ComputeTestCase#allBreakersEmpty()}
     * fails the test if either page is still live.
     */
    public void testFailIfCancelledReleasesPages() {
        Result result = resultWithTwoPages();
        CancellableTask task = newTask();
        TaskCancelHelper.cancel(task, "by user request");
        var e = expectThrows(TaskCancelledException.class, () -> TransportEsqlQueryAction.failIfCancelled(task, result));
        assertThat(e.getMessage(), equalTo("by user request"));
    }

    public void testFailIfCancelledKeepsPagesWhenNotCancelled() {
        Result result = resultWithTwoPages();
        TransportEsqlQueryAction.failIfCancelled(newTask(), result);
        for (Page page : result.pages()) {
            page.releaseBlocks();
        }
    }

    private Result resultWithTwoPages() {
        BlockFactory blockFactory = blockFactory();
        List<Page> pages = List.of(
            new Page(blockFactory.newConstantIntBlockWith(1, 10)),
            new Page(blockFactory.newConstantIntBlockWith(2, 10))
        );
        return new Result(List.of(), pages, Map.of(), EsqlTestUtils.TEST_CFG, DriverCompletionInfo.EMPTY, null, null);
    }

    private static CancellableTask newTask() {
        return new CancellableTask(1, "transport", EsqlQueryAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
    }
}
