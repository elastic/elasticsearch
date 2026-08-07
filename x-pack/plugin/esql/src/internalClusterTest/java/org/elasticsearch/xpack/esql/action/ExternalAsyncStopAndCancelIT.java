/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.startAsyncQueryWithPragmas;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Locks the cancel / STOP / DELETE contract for a long-running EXTERNAL query. The four sanctioned exits
 * are tested below; each diverges on whether the response body is partial, fully gone, or a hard error.
 * Backing the tests is a controllable {@code slowfile://} fixture that serves a fast leading prefix and
 * then trickles one byte at a time, so the query is observably mid-stream when STOP / cancel / DELETE
 * land. Follow-up to elastic/esql-planning#535.
 */
@SuppressForbidden(reason = "test fixture uses local file paths via PathUtils.get to back the slowfile:// scheme")
public class ExternalAsyncStopAndCancelIT extends AbstractEsqlIntegTestCase {

    private static final String SCHEME = "slowfile";
    private static final int LEADING_ROWS = 20;
    /** Extra rows past the leading prefix; their bytes trickle one at a time. Sized so the natural file EOF happens
     *  within a few seconds of test start (no indefinite blocking) but the stream is still producing when STOP /
     *  cancel land — STOP marks the response partial only while the task is alive, so the task must outlive STOP
     *  dispatch. */
    private static final int TRICKLE_ROWS = 200;
    /** Per-byte sleep on every read past the leading prefix. Combined with {@link #TRICKLE_ROWS} this gives a trickle
     *  window of a few seconds. Short enough that page emission keeps progressing (one full row trickles in roughly
     *  a tenth of a second), long enough that the source has not naturally EOFed by the time STOP arrives. */
    private static final long TRICKLE_MILLIS = 10L;

    /** Counts down once the storage provider has been asked for the first read of the fixture; the test uses this to
     *  observe that the query has actually started consuming bytes before it issues STOP / cancel. */
    static volatile CountDownLatch requestArrived;
    /** Counted down by the test (in the {@code finally} block and by {@link #releaseSlowFile()}) to let the slow stream
     *  return EOF and shut its loop down rather than trickling forever. */
    static volatile CountDownLatch releaseHandler;
    /** Backing file path served by the {@code slowfile://} scheme. Set per-test in {@link #setUpSlowFile()}. */
    static volatile Path slowFile;
    /** Total bytes the leading prefix occupies in {@link #slowFile}; everything beyond this offset trickles one byte at
     *  a time. Lets the slow stream serve the prefix at full speed so the format reader has complete record boundaries
     *  immediately. */
    static volatile int leadingBytes;
    /** Diagnostic counter — number of {@link SlowFileStorageProvider} reads that were forced into the trickle path.
     *  Non-zero means the source was actually slow at STOP / cancel time, not just slow on paper. */
    static final AtomicInteger trickleBytesServed = new AtomicInteger();

    /**
     * Reopens {@code loadExtensions} so the SPI-registered {@link SlowFileDataSourcePlugin} reaches the format
     * reader registry. The base test plugin suppresses extensions to keep heavy deps out of the standard IT
     * classpath.
     */
    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    /**
     * Registers a {@link StorageProvider} under the {@code slowfile} scheme. Reads delegate to the local file the test
     * wrote to {@link #slowFile} — but each read past the leading prefix is throttled to one byte per
     * {@link #TRICKLE_MILLIS} (or returns EOF if {@link #releaseHandler} has been counted down). The class is
     * SPI-registered (see {@code META-INF/services/org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin}),
     * which is the only way a custom scheme reaches the {@code StorageProviderRegistry}.
     */
    public static final class SlowFileDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Set<String> supportedSchemes() {
            return Set.of(SCHEME);
        }

        @Override
        public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
            return Map.of(SCHEME, StorageProviderFactory.noConfigKeys(SlowFileStorageProvider::new));
        }
    }

    /** Storage provider that delegates to the local filesystem but inserts a trickle on every read past
     *  {@link #leadingBytes}; see {@link SlowInputStream} for the read semantics. */
    public static final class SlowFileStorageProvider implements StorageProvider {

        @Override
        public StorageObject newObject(StoragePath path) {
            return new SlowFileObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new SlowFileObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new SlowFileObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            return new StorageIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public StorageEntry next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public boolean exists(StoragePath path) {
            return Files.exists(asLocal(path));
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of(SCHEME);
        }

        @Override
        public void close() {}
    }

    @SuppressForbidden(reason = "converts a slowfile:// StoragePath to a real local Path for the file-backed fixture")
    private static Path asLocal(StoragePath path) {
        return org.elasticsearch.core.PathUtils.get(path.localPath());
    }

    private static final class SlowFileObject implements StorageObject {
        private final StoragePath path;
        private final Path file;

        SlowFileObject(StoragePath path) {
            this.path = path;
            this.file = asLocal(path);
        }

        @Override
        public InputStream newStream() throws IOException {
            return new SlowInputStream(Files.newInputStream(file));
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            InputStream in = Files.newInputStream(file);
            long skipped = 0;
            while (skipped < position) {
                long n = in.skip(position - skipped);
                if (n <= 0) {
                    break;
                }
                skipped += n;
            }
            return new BoundedSlowInputStream(in, length, (int) Math.max(0, leadingBytes - position));
        }

        @Override
        public long length() throws IOException {
            return Files.size(file);
        }

        @Override
        public Instant lastModified() throws IOException {
            return Files.getLastModifiedTime(file).toInstant();
        }

        @Override
        public boolean exists() {
            return Files.exists(file);
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }

    /**
     * Stream wrapper that serves the leading prefix at native speed (single underlying file read), then trickles one
     * byte at a time with a {@link #TRICKLE_MILLIS} delay. Returns EOF when {@link #releaseHandler} fires so the test
     * can tear down without leaking threads. Single-byte reads (rather than blocking longer waits) keep the producer
     * thread responsive — the drain loop and the operator close path both want the producer to be observable.
     */
    private static final class SlowInputStream extends InputStream {
        private final InputStream delegate;
        private int bytesServed;

        SlowInputStream(InputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read() throws IOException {
            if (bytesServed >= leadingBytes) {
                if (releaseHandler != null && releaseHandler.getCount() == 0) {
                    return -1;
                }
                try {
                    Thread.sleep(TRICKLE_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted reading from slow fixture", e);
                }
                trickleBytesServed.incrementAndGet();
            }
            int b = delegate.read();
            if (b < 0) {
                return -1;
            }
            if (bytesServed == 0) {
                requestArrived.countDown();
            }
            bytesServed++;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            // Within the leading prefix: a normal bulk read (cheap, completes in microseconds). Past the prefix:
            // one-byte-at-a-time via read() so the trickle delay applies per byte and the abort signal (releaseHandler)
            // is observed between bytes rather than only at the next bulk boundary.
            if (bytesServed >= leadingBytes) {
                int b = read();
                if (b < 0) {
                    return -1;
                }
                buf[off] = (byte) b;
                return 1;
            }
            int remainingPrefix = leadingBytes - bytesServed;
            int toRead = Math.min(len, remainingPrefix);
            int n = delegate.read(buf, off, toRead);
            if (n <= 0) {
                return n;
            }
            if (bytesServed == 0) {
                requestArrived.countDown();
            }
            bytesServed += n;
            return n;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /** {@link SlowInputStream} variant for range reads: caps the byte count and accepts a precomputed
     *  remaining-leading-bytes window so that a range starting past the leading prefix trickles from the first byte. */
    private static final class BoundedSlowInputStream extends InputStream {
        private final InputStream delegate;
        private long remaining;
        private int bytesServed;
        private final int prefixWindow;

        BoundedSlowInputStream(InputStream delegate, long limit, int prefixWindow) {
            this.delegate = delegate;
            this.remaining = limit < 0 ? Long.MAX_VALUE : limit;
            this.prefixWindow = prefixWindow;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            if (bytesServed >= prefixWindow) {
                if (releaseHandler != null && releaseHandler.getCount() == 0) {
                    return -1;
                }
                try {
                    Thread.sleep(TRICKLE_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted reading from slow fixture", e);
                }
                trickleBytesServed.incrementAndGet();
            }
            int b = delegate.read();
            if (b < 0) {
                return -1;
            }
            if (bytesServed == 0) {
                requestArrived.countDown();
            }
            bytesServed++;
            remaining--;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            if (bytesServed >= prefixWindow) {
                int b = read();
                if (b < 0) {
                    return -1;
                }
                buf[off] = (byte) b;
                return 1;
            }
            int prefixRemaining = prefixWindow - bytesServed;
            int cap = (int) Math.min(len, Math.min(remaining, prefixRemaining));
            int n = delegate.read(buf, off, cap);
            if (n <= 0) {
                return n;
            }
            if (bytesServed == 0) {
                requestArrived.countDown();
            }
            bytesServed += n;
            remaining -= n;
            return n;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        // SlowFileDataSourcePlugin (slowfile scheme) is registered via META-INF/services, not here — see the class
        // Javadoc on SlowFileDataSourcePlugin for why.
        plugins.add(CsvDataSourcePlugin.class);
        // Wires up the async-search DELETE action; without it deleteAsyncId / TransportDeleteAsyncResultAction
        // fail at action-lookup time.
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        return plugins;
    }

    @Before
    public void setUpSlowFile() throws Exception {
        requestArrived = new CountDownLatch(1);
        releaseHandler = new CountDownLatch(1);
        trickleBytesServed.set(0);

        slowFile = createTempFile("external-slow-", ".csv");
        StringBuilder body = new StringBuilder("id:integer,value:integer\n");
        for (int i = 0; i < LEADING_ROWS; i++) {
            body.append(i).append(',').append(i * 10).append('\n');
        }
        // Padding past the leading prefix: enough rows so the trickle keeps the source alive until STOP / cancel
        // lands, but bounded so the natural file EOF wins if the test fails to release the latch. Without the bound,
        // the producer parks forever inside read() and the STOP listener (which waits on asyncTask completion) blocks
        // the whole test.
        for (int i = 0; i < TRICKLE_ROWS; i++) {
            int n = LEADING_ROWS + i;
            body.append(n).append(',').append(n * 10).append('\n');
        }
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        Files.write(slowFile, bytes);

        // Find the byte offset just after the LEADING_ROWS-th newline; everything past that is trickled.
        int seenNewlines = 0;
        int leading = bytes.length;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '\n') {
                seenNewlines++;
                if (seenNewlines == LEADING_ROWS + 1) {
                    leading = i + 1;
                    break;
                }
            }
        }
        leadingBytes = leading;
    }

    @After
    public void releaseSlowFile() {
        if (releaseHandler != null) {
            releaseHandler.countDown();
        }
    }

    /**
     * Long EXTERNAL query + async STOP must return what the source had already produced and flag the response
     * {@code is_partial=true}. Uses the default plan (no distribution pragma), so the test pins the contract for the
     * shape the user actually gets — coordinator-only EXTERNAL by default, single split. STOP wires up two
     * complementary cut paths: the exchange-close cascade (covers distributed plans) and the per-task stop-hook list
     * fed by {@code AsyncExternalSourceOperatorFactory} (covers coordinator-only plans by flipping
     * {@code AsyncExternalSourceBuffer.noMoreInputs}). Pages already accepted into the buffer drain through normally;
     * the producer thread exits before adding more.
     *
     * <p>The latch is <em>not</em> released inside the body — STOP itself must unblock the pipeline. If STOP fails to
     * cut the source the test times out, which is the right failure mode (a passing test must not be the latch
     * masquerading as STOP). Assertions pin:
     * <ul>
     *   <li>{@code is_partial=true} — STOP transitioned a still-running unit of work, not raced natural completion.</li>
     *   <li>Row count strictly below the full fixture — STOP cut the stream short of natural EOF.</li>
     *   <li>{@code trickleBytesServed > 0} — STOP arrived during active trickle, not before the source had work.</li>
     * </ul>
     */
    public void testAsyncStopReturnsBufferedRowsAndMarksPartial() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("parsing_parallelism / page_size pragmas are snapshot-only", Build.current().isSnapshot());

        String uri = SCHEME + "://" + StoragePath.fileUri(slowFile).substring("file://".length());
        // LIMIT well above LEADING_ROWS so natural completion never wins the race with STOP. parsing_parallelism=1
        // pins the deterministic single-thread CSV path; page_size=5 keeps pages small so several pages emit from the
        // leading prefix and STOP's cut is observably below the total fixture row count.
        String query = "EXTERNAL \"" + uri + "\" | LIMIT 1000000";

        final String asyncId = startAsyncQueryWithPragmas(
            client(),
            query,
            null,
            Map.of(QueryPragmas.PARSING_PARALLELISM.getKey(), 1, QueryPragmas.PAGE_SIZE.getKey(), 5)
        );
        try {
            assertTrue("storage fixture must receive the EXTERNAL read request", requestArrived.await(30, TimeUnit.SECONDS));
            // Strong gate: wait until the producer has visibly crossed into the trickle (consumed the entire fast
            // leading prefix and is now reading byte-by-byte). Without this, STOP could fire before the producer is
            // past the prefix and the test would not exercise "STOP while EXTERNAL is actively producing".
            assertBusy(
                () -> assertThat("producer must be in the trickle phase before STOP fires", trickleBytesServed.get(), greaterThan(0)),
                30,
                TimeUnit.SECONDS
            );

            AsyncStopRequest stopRequest = new AsyncStopRequest(asyncId);
            try (EsqlQueryResponse response = client().execute(EsqlAsyncStopAction.INSTANCE, stopRequest).actionGet(60, TimeUnit.SECONDS)) {
                assertThat(response.isRunning(), is(false));
                assertThat("STOP must return the schema even when the source was cut short", response.columns().size(), equalTo(2));
                assertThat(
                    "STOP must flip is_partial — TransportEsqlAsyncStopAction marks the response partial when either "
                        + "finishSessionEarly closed an active exchange (distributed plans) or a task-local stop hook "
                        + "transitioned a running driver into early-finishing (coordinator-only plans). Either signal "
                        + "is hard proof that STOP cut live dataflow.",
                    response.isPartial(),
                    is(true)
                );

                int rows = 0;
                Iterator<Iterator<Object>> values = response.values();
                while (values.hasNext()) {
                    Iterator<Object> row = values.next();
                    while (row.hasNext()) {
                        row.next();
                    }
                    rows++;
                }
                assertThat(
                    "STOP must truncate strictly below the total fixture row count — otherwise the source was not "
                        + "cut short and STOP degenerated into 'wait for natural completion'",
                    rows,
                    lessThan(LEADING_ROWS + TRICKLE_ROWS)
                );
                assertThat(
                    "the producer must have been in the trickle phase when STOP fired — proves the response was "
                        + "captured mid-stream, not at natural EOF",
                    trickleBytesServed.get(),
                    greaterThan(0)
                );
            }
        } finally {
            releaseHandler.countDown();
            deleteAsyncId(client(), asyncId);
        }
    }

    /**
     * Sync task-cancel: hard-fails with {@link TaskCancelledException}, no response body. Same path a client
     * disconnect takes via {@code RestCancellableNodeClient}. {@code setWaitForCompletion(true)} sequences cancel
     * after task teardown so the queryFuture has observably failed before we release the trickle.
     */
    public void testSyncTaskCancelHardFailsWithNoRows() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("parsing_parallelism pragma is snapshot-only", Build.current().isSnapshot());

        String uri = SCHEME + "://" + StoragePath.fileUri(slowFile).substring("file://".length());
        // Embed a unique marker in the query so findEsqlTask can latch onto this specific task even when other ES|QL
        // traffic (test-internal probes, background tasks) shows up in the listing.
        String marker = "stop_cancel_sync_" + System.nanoTime();
        String query = "// " + marker + "\nEXTERNAL \"" + uri + "\" | LIMIT 1000000";
        EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.PARSING_PARALLELISM.getKey(), 1).build())
        );

        ActionFuture<EsqlQueryResponse> queryFuture = client().execute(EsqlQueryAction.INSTANCE, request);
        try {
            assertTrue("storage fixture must receive the EXTERNAL read request", requestArrived.await(30, TimeUnit.SECONDS));
            // Strong gate: ensure cancel lands while the producer is actively trickling, not during the fast leading
            // prefix scan (where the cancel might race with the natural prefix read).
            assertBusy(
                () -> assertThat("producer must be in the trickle phase before cancel fires", trickleBytesServed.get(), greaterThan(0)),
                30,
                TimeUnit.SECONDS
            );

            TaskId esqlTaskId = findEsqlTask(marker);
            assertThat("the EsqlQueryAction task must be cancellable while the source is still trickling", esqlTaskId, notNullValue());

            // setWaitForCompletion(true): cancel does not return until the task is dead. That gives us a deterministic
            // ordering — when cancel returns, the queryFuture has already failed (or is about to fail) with the cancel
            // exception, and the producer wound down via the cancel cascade rather than via the trickle EOF.
            CancelTasksRequest cancel = new CancelTasksRequest().setTargetTaskId(esqlTaskId).setReason("test cancel");
            cancel.setWaitForCompletion(true);
            ActionFuture<?> cancelFuture = client().admin().cluster().execute(TransportCancelTasksAction.TYPE, cancel);
            cancelFuture.actionGet(60, TimeUnit.SECONDS);

            Exception thrown = expectThrows(Exception.class, () -> {
                try (var ignored = queryFuture.actionGet(60, TimeUnit.SECONDS)) {
                    fail("cancel must hard-fail the EXTERNAL query, not return a (partial) response body");
                }
            });
            Throwable cancelException = ExceptionsHelper.unwrap(thrown, TaskCancelledException.class);
            assertThat(
                "ES convention: cancel always surfaces as TaskCancelledException, never masked as partial-stats — see "
                    + "ExternalSourceResolver.throwIfCancelled",
                cancelException,
                notNullValue()
            );
            assertThat(
                "the producer must have been in the trickle phase when cancel fired — proves cancel raced active "
                    + "production, not natural EOF",
                trickleBytesServed.get(),
                greaterThan(0)
            );
        } finally {
            releaseHandler.countDown();
        }
    }

    /**
     * Async DELETE on a running query rides the cancel side: it must cancel the underlying task and then delete the
     * saved entry, not let the query complete naturally and serve a partial body. Proof shape: gate on
     * {@link #trickleBytesServed} > 0, confirm the task is alive before DELETE and gone after, and require the
     * post-DELETE GET to surface {@link org.elasticsearch.ResourceNotFoundException}.
     */
    public void testAsyncDeleteHardFailsWithNoRows() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("parsing_parallelism / page_size pragmas are snapshot-only", Build.current().isSnapshot());

        String uri = SCHEME + "://" + StoragePath.fileUri(slowFile).substring("file://".length());
        String query = "EXTERNAL \"" + uri + "\" | LIMIT 1000000";

        final String asyncId = startAsyncQueryWithPragmas(
            client(),
            query,
            null,
            Map.of(QueryPragmas.PARSING_PARALLELISM.getKey(), 1, QueryPragmas.PAGE_SIZE.getKey(), 5)
        );
        try {
            assertTrue("storage fixture must receive the EXTERNAL read request", requestArrived.await(30, TimeUnit.SECONDS));
            assertBusy(
                () -> assertThat(
                    "producer must be in the trickle phase before DELETE fires — otherwise the test cannot "
                        + "distinguish 'DELETE cancelled an active query' from 'query completed naturally then DELETE "
                        + "removed the entry'",
                    trickleBytesServed.get(),
                    greaterThan(0)
                ),
                30,
                TimeUnit.SECONDS
            );

            // Pre-DELETE: the async task must be present and live. The async task id is encoded inside asyncId, so
            // we can look it up directly rather than scanning the task listing (async tasks live under a
            // {@code AsyncExecutionId}-derived task id, not the EsqlQueryAction listing where the sync submission
            // appears).
            TaskId asyncTaskId = AsyncExecutionId.decode(asyncId).getTaskId();
            assertTrue("the async task must be alive while the trickle is still feeding bytes", isTaskRunning(asyncTaskId));

            ActionFuture<?> deleteFuture = client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest(asyncId));
            // DELETE blocks on task cancellation before removing the entry, then the producer's drain loop observes
            // noMoreInputs and exits — the trickle yields per-byte so the producer never parks long. Release in
            // finally guarantees no leak if cancellation cascade has a regression.
            deleteFuture.actionGet(30, TimeUnit.SECONDS);

            // Post-DELETE: task is gone from the registry. Natural-completion would have taken ~TRICKLE_ROWS *
            // TRICKLE_MILLIS milliseconds of pure I/O plus parsing; DELETE returning within the 30s timeout *and*
            // the task disappearing is the joint signature of cancel-driven termination.
            assertBusy(
                () -> assertFalse(
                    "after DELETE the async task must be gone from the registry — cancel cascade should have torn "
                        + "it down, otherwise DELETE did not ride the cancel side of the contract",
                    isTaskRunning(asyncTaskId)
                ),
                30,
                TimeUnit.SECONDS
            );

            Exception thrown = expectThrows(Exception.class, () -> {
                try (var ignored = EsqlAsyncTestUtils.getAsyncResponse(client(), asyncId)) {
                    fail("DELETE must cancel the running query, not let it complete with a partial body");
                }
            });
            assertThat(
                "DELETE removed the async entry, so GET must surface ResourceNotFoundException — a successful body "
                    + "would mean DELETE silently partial-completed the query rather than cancelling it. Got: "
                    + thrown,
                ExceptionsHelper.unwrap(thrown, org.elasticsearch.ResourceNotFoundException.class),
                notNullValue()
            );
        } finally {
            releaseHandler.countDown();
        }
    }

    /**
     * Async task-cancel: direct {@code _tasks/{id}/_cancel} on the still-running async task. Cancel-side outcome —
     * the saved async result must surface as a hard failure or be wiped entirely on GET; never a partial body.
     */
    public void testAsyncTaskCancelHardFailsWithNoRows() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("parsing_parallelism / page_size pragmas are snapshot-only", Build.current().isSnapshot());

        String uri = SCHEME + "://" + StoragePath.fileUri(slowFile).substring("file://".length());
        String query = "EXTERNAL \"" + uri + "\" | LIMIT 1000000";

        final String asyncId = startAsyncQueryWithPragmas(
            client(),
            query,
            null,
            Map.of(QueryPragmas.PARSING_PARALLELISM.getKey(), 1, QueryPragmas.PAGE_SIZE.getKey(), 5)
        );
        try {
            assertTrue("storage fixture must receive the EXTERNAL read request", requestArrived.await(30, TimeUnit.SECONDS));
            assertBusy(
                () -> assertThat("producer must be in the trickle phase before cancel fires", trickleBytesServed.get(), greaterThan(0)),
                30,
                TimeUnit.SECONDS
            );

            TaskId asyncTaskId = AsyncExecutionId.decode(asyncId).getTaskId();
            assertTrue("the async task must be cancellable while the source is still trickling", isTaskRunning(asyncTaskId));

            CancelTasksRequest cancel = new CancelTasksRequest().setTargetTaskId(asyncTaskId).setReason("test async cancel");
            cancel.setWaitForCompletion(true);
            client().admin().cluster().execute(TransportCancelTasksAction.TYPE, cancel).actionGet(60, TimeUnit.SECONDS);

            // After cancel returns (waitForCompletion=true), the task should be torn down and the persisted async
            // result should surface a cancel-side failure. Two valid shapes:
            // * TaskCancelledException — the cancellation reached the stored response
            // * ResourceNotFoundException — the cancel cascade wiped the async entry before the GET landed
            // A successful body with rows would be a regression: async cancel must never silently partial-complete.
            Exception thrown = expectThrows(Exception.class, () -> {
                try (var ignored = EsqlAsyncTestUtils.getAsyncResponse(client(), asyncId)) {
                    fail("async task cancel must hard-fail the query, not return a partial body");
                }
            });
            boolean cancelled = ExceptionsHelper.unwrap(thrown, TaskCancelledException.class) != null;
            boolean notFound = ExceptionsHelper.unwrap(thrown, org.elasticsearch.ResourceNotFoundException.class) != null;
            assertTrue(
                "async cancel must surface as TaskCancelledException or ResourceNotFoundException — never a partial "
                    + "body. Got: "
                    + thrown,
                cancelled || notFound
            );
            assertThat("the producer must have been in the trickle phase when cancel fired", trickleBytesServed.get(), greaterThan(0));
        } finally {
            releaseHandler.countDown();
            // best-effort cleanup of the saved async entry if cancel didn't wipe it
            try {
                deleteAsyncId(client(), asyncId);
            } catch (Exception ignored) {}
        }
    }

    /**
     * Resolves the {@link EsqlQueryAction} task that belongs to <em>this</em> test's submitted <em>sync</em> query by
     * matching the caller's marker against the task description (which {@code EsqlQueryAction} sets to the query
     * text). Without the marker filter, {@code listTasks} could return any concurrent ES|QL task — e.g. an unrelated
     * probe submitted by a background test runner — and the cancel would target the wrong one. Picking the first task
     * in the listing was the previous behaviour; this version is task-identity-safe.
     *
     * <p>For async submissions the task id is encoded in the {@code asyncId}; resolve it directly via
     * {@code AsyncExecutionId.decode(asyncId).getTaskId()} instead of going through this helper.
     */
    private TaskId findEsqlTask(String marker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            ListTasksResponse tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME)
                .setDetailed(true)
                .get();
            for (TaskInfo info : tasks.getTasks()) {
                String description = info.description();
                if (description != null && description.contains(marker)) {
                    return info.taskId();
                }
            }
            Thread.sleep(50);
        }
        return null;
    }

    /** Returns true if the cluster's task registry currently lists the given task id. Used by the async tests to
     *  confirm pre-cancellation liveness and post-cancellation absence without scanning by description. */
    private boolean isTaskRunning(TaskId taskId) {
        ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setTargetTaskId(taskId).get();
        return tasks.getTasks().isEmpty() == false;
    }

}
