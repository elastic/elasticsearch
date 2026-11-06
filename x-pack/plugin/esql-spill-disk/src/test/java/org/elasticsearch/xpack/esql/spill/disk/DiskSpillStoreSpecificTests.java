/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill.disk;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.spill.spi.SpillLimitExceededException;
import org.elasticsearch.compute.spill.spi.SpillObject;
import org.elasticsearch.compute.spill.spi.SpillRead;
import org.elasticsearch.compute.spill.spi.SpillReader;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.spill.spi.SpillStoreServices;
import org.elasticsearch.compute.spill.spi.SpillWriter;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * What only a disk can show: the startup sweep, the path validation, a rejecting pool, a file that vanishes or is cut
 * under a reader, an abort that meets an append in flight, the cap raced by several scopes, a lowered cap, a delete
 * that fails and is retried, cancellation of a queued write, and the shutdown. The entitlement policy is not exercised
 * here: the test framework resolves neither a data directory nor the path setting for it, so only a real node checks
 * the grants.
 */
public class DiskSpillStoreSpecificTests extends ComputeTestCase {

    private static final String IO = "spill_io_test";

    private TestThreadPool threadPool;
    private CircuitBreaker breaker;

    @Before
    public void startPool() {
        breaker = blockFactory().breaker();
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, IO, between(1, 3), 16, "spill", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void stopPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private Executor io() {
        return threadPool.executor(IO);
    }

    private <T> T await(Consumer<ActionListener<T>> operation) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        operation.accept(future);
        return future.actionGet(30, TimeUnit.SECONDS);
    }

    private <T> Throwable awaitFailure(Consumer<ActionListener<T>> operation) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        operation.accept(future);
        // get rather than actionGet: the latter wraps a checked failure twice
        ExecutionException e = expectThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
        return e.getCause();
    }

    private DiskSpillStore store(Path spill, long cap) {
        return (DiskSpillStore) new DiskSpillPlugin().create(DiskSpillStoreTests.services(spill, createTempDir(), cap, io()));
    }

    private static ReleasableBytesReference chunk(int length) {
        return ReleasableBytesReference.wrap(new BytesArray(new byte[length]));
    }

    private SpillObject write(SpillScope scope, int bytes) {
        SpillWriter writer = this.<SpillWriter>await(l -> scope.createWriter("t", l));
        try (ReleasableBytesReference chunk = chunk(bytes)) {
            this.<Void>await(l -> writer.append(chunk, l));
        }
        return await(writer::finish);
    }

    public void testTheRootIsSweptAtStart() throws IOException {
        Path spill = createTempDir();
        Path root = spill.resolve("node-x");
        Files.createDirectories(root.resolve("3-oldquery"));
        Files.write(root.resolve("3-oldquery").resolve("1.run"), new byte[100]);
        Files.write(root.resolve("stray"), new byte[10]);
        Settings settings = Settings.builder().put(DiskSpillSettings.PATH.getKey(), spill.toString()).build();
        SpillStoreServices services = new SpillStoreServices(
            settings,
            new ClusterSettings(settings, Set.of(DiskSpillSettings.PATH, DiskSpillSettings.CAP)),
            List.of(createTempDir()),
            "node-x",
            io()
        );
        DiskSpillStore store = (DiskSpillStore) new DiskSpillPlugin().create(services);
        try (var entries = Files.list(root)) {
            assertThat("orphans of a previous process are gone", entries.count(), equalTo(0L));
        }
        assertThat(store.bytesInUse(), equalTo(0L));
        await(store::shutdown);
    }

    public void testThePathMayNotBeADataPathOrASymbolicLink() throws IOException {
        Path data = createTempDir();
        Settings onData = Settings.builder().put(DiskSpillSettings.PATH.getKey(), data.toString()).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DiskSpillStore.resolveRoot(
                new SpillStoreServices(
                    onData,
                    new ClusterSettings(onData, Set.of(DiskSpillSettings.PATH, DiskSpillSettings.CAP)),
                    List.of(data),
                    "n",
                    io()
                )
            )
        );
        assertThat(e.getMessage(), containsString("data path"));
        Path target = createTempDir();
        Path link = createTempDir().resolve("link");
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException unsupported) {
            assumeNoException("symbolic links are not supported here", unsupported);
        }
        Settings viaLink = Settings.builder().put(DiskSpillSettings.PATH.getKey(), link.toString()).build();
        Exception linkFailure = expectThrows(
            Exception.class,
            () -> new DiskSpillPlugin().create(
                new SpillStoreServices(
                    viaLink,
                    new ClusterSettings(viaLink, Set.of(DiskSpillSettings.PATH, DiskSpillSettings.CAP)),
                    List.of(data),
                    "n",
                    io()
                )
            )
        );
        assertThat(linkFailure.getMessage(), containsString("symbolic link"));
        Settings byDefault = Settings.EMPTY;
        Path root = DiskSpillStore.resolveRoot(
            new SpillStoreServices(
                byDefault,
                new ClusterSettings(byDefault, Set.of(DiskSpillSettings.PATH, DiskSpillSettings.CAP)),
                List.of(data),
                "n",
                io()
            )
        );
        assertThat(root, equalTo(data.resolve(DiskSpillSettings.DEFAULT_DIRECTORY).resolve("n")));
    }

    public void testARejectingPoolCompletesEveryListener() {
        Executor rejecting = command -> { throw new EsRejectedExecutionException("rejected", false); };
        DiskSpillStore store = (DiskSpillStore) new DiskSpillPlugin().create(
            DiskSpillStoreTests.services(createTempDir(), createTempDir(), 1 << 20, rejecting)
        );
        SpillScope scope = store.newScope("q", breaker);
        assertThat(this.<SpillWriter>awaitFailure(l -> scope.createWriter("t", l)), instanceOf(EsRejectedExecutionException.class));
        assertThat(this.<Void>awaitFailure(scope::close), instanceOf(EsRejectedExecutionException.class));
        assertThat(this.<Void>awaitFailure(store::shutdown), instanceOf(EsRejectedExecutionException.class));
    }

    public void testAFileThatVanishesFailsTheReaderCleanly() throws IOException {
        DiskSpillStore store = store(createTempDir(), 1 << 20);
        SpillScope scope = store.newScope("q", breaker);
        DiskSpillObject object = (DiskSpillObject) write(scope, between(100, 1000));
        SpillReader reader = this.<SpillReader>await(l -> object.openRange(0, object.sizeInBytes(), l));
        try {
            Files.delete(object.path());
        } catch (IOException e) {
            // the mock Windows file system refuses to delete a file that is still open; nothing else can fail the delete
            // here. Let go of the reader and the store before giving up, or the temporary directory cannot be removed
            this.<Void>await(reader::close);
            await(scope::close);
            await(store::shutdown);
            assumeNoException("this file system keeps an open file from being deleted", e);
        }
        // on a file system that keeps an open file readable the first read may still succeed; the reader must not crash
        try {
            SpillRead read = this.<SpillRead>await(l -> reader.read(16, l));
            if (read.isEof() == false) {
                read.data().close();
            }
        } catch (Exception e) {
            // a clean failure is the other allowed outcome
        }
        this.<Void>await(reader::close);
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
        await(store::shutdown);
    }

    public void testAFileCutUnderAReaderFailsTheReadAndPoisonsIt() throws IOException {
        DiskSpillStore store = store(createTempDir(), 1 << 20);
        SpillScope scope = store.newScope("q", breaker);
        int bytes = between(100, 1000);
        DiskSpillObject object = (DiskSpillObject) write(scope, bytes);
        SpillReader reader = this.<SpillReader>await(l -> object.openRange(0, bytes, l));
        Files.write(object.path(), new byte[between(0, bytes - 1)]);
        Throwable e = this.<SpillRead>awaitFailure(l -> reader.read(bytes, l));
        assertThat(e, instanceOf(IOException.class));
        assertThat(e.getMessage(), containsString("ended before"));
        assertThat("the read's buffer is returned", breaker.getUsed(), equalTo(0L));
        Throwable again = this.<SpillRead>awaitFailure(l -> reader.read(16, l));
        assertThat("the reader is poisoned", again, instanceOf(IllegalStateException.class));
        assertThat(again.getMessage(), containsString("failed earlier"));
        this.<Void>await(reader::close);
        await(scope::close);
        await(store::shutdown);
    }

    public void testAnAbortThatMeetsAnAppendInFlightReturnsEverything() throws Exception {
        TestThreadPool single = new TestThreadPool(
            getTestName() + "-single",
            new FixedExecutorBuilder(Settings.EMPTY, IO, 1, 16, "spill", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        try {
            DiskSpillStore store = (DiskSpillStore) new DiskSpillPlugin().create(
                DiskSpillStoreTests.services(createTempDir(), createTempDir(), 1 << 20, single.executor(IO))
            );
            SpillScope scope = store.newScope("q", breaker);
            DiskSpillWriter writer = (DiskSpillWriter) this.<SpillWriter>await(l -> scope.createWriter("t", l));
            CountDownLatch block = new CountDownLatch(1);
            single.executor(IO).execute(() -> {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
            int length = between(100, 1000);
            PlainActionFuture<Void> appended = new PlainActionFuture<>();
            try (ReleasableBytesReference chunk = chunk(length)) {
                writer.append(chunk, appended);
                assertThat("the chunk is reserved", store.bytesInUse(), equalTo((long) length));
                // what an abort on the pool does when it finds the append in flight, taken off the queue so that the
                // order is certain: the append then writes into a file nobody wants
                writer.discard();
                block.countDown();
                Exception e = expectThrows(Exception.class, () -> appended.actionGet(30, TimeUnit.SECONDS));
                assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(IllegalStateException.class));
            }
            assertThat("nothing stays reserved or committed", store.bytesInUse(), equalTo(0L));
            assertFalse("the file is gone", Files.exists(writer.path()));
            await(writer::abort);
            await(scope::close);
            await(store::shutdown);
        } finally {
            ThreadPool.terminate(single, 10, TimeUnit.SECONDS);
        }
    }

    public void testTheCapIsNeverExceededByRacingScopes() throws Exception {
        long cap = between(2000, 20_000);
        DiskSpillStore store = store(createTempDir(), cap);
        int scopes = between(2, 4);
        List<SpillScope> scopeList = new ArrayList<>();
        for (int i = 0; i < scopes; i++) {
            scopeList.add(store.newScope("q" + i, breaker));
        }
        AtomicInteger admitted = new AtomicInteger();
        AtomicInteger refused = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(scopes);
        List<Thread> racers = new ArrayList<>();
        int[] sizes = new int[50];
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = between(1, (int) (cap / 10));
        }
        for (SpillScope scope : scopeList) {
            // on threads of their own: the I/O pool must stay free to complete what they wait for
            Thread racer = new Thread(() -> {
                try {
                    SpillWriter writer = this.<SpillWriter>await(l -> scope.createWriter("t", l));
                    for (int i = 0; i < 50; i++) {
                        try (ReleasableBytesReference chunk = chunk(sizes[i])) {
                            PlainActionFuture<Void> future = new PlainActionFuture<>();
                            writer.append(chunk, future);
                            future.actionGet(30, TimeUnit.SECONDS);
                            admitted.incrementAndGet();
                        } catch (SpillLimitExceededException e) {
                            refused.incrementAndGet();
                            break;
                        }
                        assertThat(store.bytesInUse(), lessThanOrEqualTo(cap));
                    }
                } catch (Throwable t) {
                    // reported on the test thread, where an assertion counts
                    failure.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            }, "racer-" + scope.hashCode());
            racers.add(racer);
            racer.start();
        }
        assertTrue(done.await(60, TimeUnit.SECONDS));
        for (Thread racer : racers) {
            racer.join();
        }
        if (failure.get() != null) {
            throw new AssertionError("a racer failed", failure.get());
        }
        assertThat(store.bytesInUse(), lessThanOrEqualTo(cap));
        assertThat("the cap was reached", refused.get(), greaterThan(0));
        for (SpillScope scope : scopeList) {
            await(scope::close);
        }
        assertThat(store.bytesInUse(), equalTo(0L));
        await(store::shutdown);
    }

    public void testALoweredCapFailsTheNextAppend() {
        Path spill = createTempDir();
        SpillStoreServices services = DiskSpillStoreTests.services(spill, createTempDir(), 1 << 20, io());
        DiskSpillStore store = (DiskSpillStore) new DiskSpillPlugin().create(services);
        SpillScope scope = store.newScope("q", breaker);
        write(scope, 1000);
        services.clusterSettings().applySettings(Settings.builder().put(DiskSpillSettings.CAP.getKey(), "1500b").build());
        assertThat(store.bytesLimit(), equalTo(1500L));
        SpillWriter writer = this.<SpillWriter>await(l -> scope.createWriter("t", l));
        try (ReleasableBytesReference chunk = chunk(1000)) {
            Throwable e = this.<Void>awaitFailure(l -> writer.append(chunk, l));
            assertThat(e, instanceOf(SpillLimitExceededException.class));
            assertThat(((SpillLimitExceededException) e).limitBytes(), equalTo(1500L));
        }
        await(writer::abort);
        await(scope::close);
        await(store::shutdown);
    }

    public void testAFailedDeleteIsADebtRetriedLater() throws IOException {
        assumeTrue("needs POSIX permissions", Environment.getFileStore(createTempDir()).supportsFileAttributeView("posix"));
        assumeFalse("root is not bound by permissions", "root".equals(System.getProperty("user.name")));
        DiskSpillStore store = store(createTempDir(), 1 << 20);
        DiskSpillScope scope = (DiskSpillScope) store.newScope("q", breaker);
        int bytes = between(100, 1000);
        SpillObject object = write(scope, bytes);
        Path directory = scope.directory();
        Set<PosixFilePermission> original = Files.getPosixFilePermissions(directory);
        Files.setPosixFilePermissions(directory, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
        try {
            await(object::delete);
            assertThat("the file remains, so its bytes stay counted", store.bytesInUse(), equalTo((long) bytes));
            assertThat(store.cleanupDebtBytes(), equalTo((long) bytes));
        } finally {
            Files.setPosixFilePermissions(directory, original);
        }
        await(scope::close);
        assertThat("the close retried the debt", store.cleanupDebtBytes(), equalTo(0L));
        assertThat(store.bytesInUse(), equalTo(0L));
        await(store::shutdown);
    }

    public void testCancellationFailsAQueuedWrite() throws Exception {
        TestThreadPool single = new TestThreadPool(
            getTestName() + "-single",
            new FixedExecutorBuilder(Settings.EMPTY, IO, 1, 16, "spill", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        try {
            DiskSpillStore store = (DiskSpillStore) new DiskSpillPlugin().create(
                DiskSpillStoreTests.services(createTempDir(), createTempDir(), 1 << 20, single.executor(IO))
            );
            SpillScope scope = store.newScope("q", breaker);
            SpillWriter writer = this.<SpillWriter>await(l -> scope.createWriter("t", l));
            CountDownLatch block = new CountDownLatch(1);
            single.executor(IO).execute(() -> {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
            PlainActionFuture<Void> appended = new PlainActionFuture<>();
            try (ReleasableBytesReference chunk = chunk(100)) {
                writer.append(chunk, appended);
                scope.cancel();
                block.countDown();
                Exception e = expectThrows(Exception.class, () -> appended.actionGet(30, TimeUnit.SECONDS));
                assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(TaskCancelledException.class));
            }
            assertThat("a cancelled append reserved nothing", store.bytesInUse(), equalTo(0L));
            await(scope::close);
            await(store::shutdown);
        } finally {
            ThreadPool.terminate(single, 10, TimeUnit.SECONDS);
        }
    }

    public void testShutdownDeletesEverything() throws IOException {
        Path spill = createTempDir();
        DiskSpillStore store = store(spill, 1 << 20);
        SpillScope first = store.newScope("q1", breaker);
        SpillScope second = store.newScope("q2", breaker);
        write(first, 500);
        write(second, 700);
        assertThat(store.bytesInUse(), equalTo(1200L));
        await(store::shutdown);
        assertThat(store.bytesInUse(), equalTo(0L));
        try (var entries = Files.list(store.root())) {
            assertThat(entries.count(), equalTo(0L));
        }
        expectThrows(IllegalStateException.class, () -> store.newScope("q3", breaker));
    }
}
