/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill.disk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.compute.spill.spi.SpillLimitExceededException;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillStoreServices;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Runs as files under {@code <path>/<node id>}, one directory per query scope, swept clean when the store starts
 * since no query survives a restart. Admission is a cap on bytes held, reserved ones counted the moment an append is
 * accepted and committed ones once it is written, with a free-space margin checked on the file store every few
 * megabytes, so a full volume fails the query rather than the node. Deletion never returns quota for a file that
 * remains: a failed delete goes into a cleanup debt that the next scope close and the shutdown retry. Every filesystem
 * operation, data and metadata alike, runs on the spill I/O pool; the one exception is the sizing of the file store at
 * start, done once before the store exists.
 * <p>
 * Orphans: the sweep covers this node id's directory only. Under a configured path shared by several nodes, or after a
 * node id changed, the directories of other ids are left alone, since they may belong to a node that is running; an
 * operator removes them.
 */
public final class DiskSpillStore implements SpillStore {

    private static final Logger logger = LogManager.getLogger(DiskSpillStore.class);
    /** How many admitted bytes go by between two looks at the file store's usable space. */
    static final long FREE_SPACE_CHECK_INTERVAL_BYTES = ByteSizeValue.ofMb(4).getBytes();

    private final Path root;
    private final String capSettingName;
    private final Executor io;
    private final AtomicLong reserved = new AtomicLong();
    private final AtomicLong committed = new AtomicLong();
    private final AtomicLong admittedSinceCheck = new AtomicLong();
    private final AtomicLong scopeCounter = new AtomicLong();
    private final Map<String, DiskSpillScope> scopes = new ConcurrentHashMap<>();
    private final List<Debt> debts = new ArrayList<>();
    private volatile long capBytes;
    private volatile long freeSpaceMarginBytes;
    private boolean shutdown;

    private record Debt(Path path, long bytes) {}

    DiskSpillStore(Path root, long capBytes, String capSettingName, Executor io) {
        this.root = root;
        this.capBytes = capBytes;
        this.capSettingName = capSettingName;
        this.freeSpaceMarginBytes = marginFor(capBytes);
        this.io = io;
    }

    /**
     * Resolves the root, validates it, sweeps it and watches the cap setting. Fails the node at start with a clear
     * message when the configured path is unusable.
     */
    public static DiskSpillStore create(SpillStoreServices services) {
        Path root = resolveRoot(services);
        final long totalBytes;
        try {
            Files.createDirectories(root);
            if (Files.isSymbolicLink(root) || Files.isSymbolicLink(root.getParent())) {
                // the sweep removes everything under the root; it must never follow a link to somewhere else
                throw new IllegalArgumentException("spill path [" + root + "] must not be a symbolic link");
            }
            sweep(root);
            // sized once here, so that a later cap update needs no file store call on the cluster applier thread
            totalBytes = Environment.getFileStore(root).getTotalSpace();
        } catch (IOException e) {
            throw new UncheckedIOException("could not prepare spill path [" + root + "]", e);
        }
        RelativeByteSizeValue cap = DiskSpillSettings.CAP.get(services.settings());
        DiskSpillStore store = new DiskSpillStore(root, resolveCap(cap, totalBytes), DiskSpillSettings.CAP.getKey(), services.ioExecutor());
        services.clusterSettings()
            .addSettingsUpdateConsumer(DiskSpillSettings.CAP, updated -> store.setCap(resolveCap(updated, totalBytes)));
        return store;
    }

    static Path resolveRoot(SpillStoreServices services) {
        String configured = DiskSpillSettings.PATH.get(services.settings());
        Path base;
        if (DiskSpillSettings.PATH.exists(services.settings()) && configured.isBlank()) {
            // the entitlement policy resolves the setting too, and an empty value would grant the data root
            throw new IllegalArgumentException("[" + DiskSpillSettings.PATH.getKey() + "] must not be blank");
        }
        if (configured.isEmpty()) {
            if (services.dataPaths().isEmpty()) {
                throw new IllegalArgumentException("no data path to put [" + DiskSpillSettings.DEFAULT_DIRECTORY + "] under");
            }
            base = services.dataPaths().get(0).resolve(DiskSpillSettings.DEFAULT_DIRECTORY);
        } else {
            if (services.dataPaths().isEmpty()) {
                throw new IllegalArgumentException("[" + DiskSpillSettings.PATH.getKey() + "] needs a data path to resolve against");
            }
            // on the data paths' file system, which is the node's, and the test framework's wrapper in tests
            base = services.dataPaths().get(0).getFileSystem().getPath(configured);
            if (base.isAbsolute() == false) {
                base = services.dataPaths().get(0).resolve(base);
            }
            base = base.toAbsolutePath().normalize();
            for (Path dataPath : services.dataPaths()) {
                Path data = dataPath.toAbsolutePath().normalize();
                if (base.equals(data) || data.startsWith(base)) {
                    throw new IllegalArgumentException(
                        "[" + DiskSpillSettings.PATH.getKey() + "] must not be a data path or contain one, got [" + base + "]"
                    );
                }
            }
        }
        return base.resolve(services.nodeId());
    }

    static long resolveCap(RelativeByteSizeValue cap, long fileStoreBytes) {
        if (cap.isAbsolute()) {
            return cap.getAbsolute().getBytes();
        }
        return cap.calculateValue(ByteSizeValue.ofBytes(fileStoreBytes), null).getBytes();
    }

    private static long marginFor(long capBytes) {
        return Math.max(ByteSizeValue.ofMb(16).getBytes(), capBytes / 20);
    }

    /**
     * Deletes everything under the root: whatever a previous process left is an orphan, since no query survives a restart.
     */
    static void sweep(Path root) throws IOException {
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(root)) {
            for (Path entry : entries) {
                IOUtils.rm(entry);
            }
        }
    }

    void setCap(long capBytes) {
        this.capBytes = capBytes;
        this.freeSpaceMarginBytes = marginFor(capBytes);
    }

    Path root() {
        return root;
    }

    Executor io() {
        return io;
    }

    @Override
    public SpillScope newScope(String queryId, CircuitBreaker breaker) {
        Path directory = root.resolve(scopeCounter.incrementAndGet() + "-" + safeName(queryId));
        DiskSpillScope scope = new DiskSpillScope(this, queryId, directory, breaker);
        // under the same lock as the shutdown's snapshot, so that no scope slips in after it
        synchronized (scopes) {
            if (shutdown) {
                throw new IllegalStateException("spill store is shut down");
            }
            if (scopes.putIfAbsent(queryId, scope) != null) {
                throw new IllegalArgumentException("spill scope [" + queryId + "] exists");
            }
        }
        return scope;
    }

    private static String safeName(String queryId) {
        StringBuilder safe = new StringBuilder(queryId.length());
        for (int i = 0; i < queryId.length() && safe.length() < 64; i++) {
            char c = queryId.charAt(i);
            safe.append(Character.isLetterOrDigit(c) || c == '-' || c == '_' ? c : '_');
        }
        return safe.toString();
    }

    void scopeClosed(DiskSpillScope scope) {
        scopes.remove(scope.queryId(), scope);
        retryDebts();
    }

    @Override
    public long bytesInUse() {
        return reserved.get() + committed.get();
    }

    @Override
    public long bytesLimit() {
        return capBytes;
    }

    /**
     * Bytes of files that could not be deleted and are still counted against the cap.
     */
    @Override
    public long cleanupDebtBytes() {
        synchronized (debts) {
            long sum = 0;
            for (Debt debt : debts) {
                sum += debt.bytes;
            }
            return sum;
        }
    }

    // ---- admission

    /**
     * Admits {@code bytes} against the cap; they are reserved until {@link #commit} or {@link #unreserve}. Atomics only,
     * since this runs on the caller's thread.
     */
    void reserve(long bytes) throws SpillLimitExceededException {
        long cap = capBytes;
        while (true) {
            long current = reserved.get();
            long inUse = current + committed.get();
            if (inUse + bytes > cap) {
                throw new SpillLimitExceededException(capSettingName, cap, inUse, bytes);
            }
            if (reserved.compareAndSet(current, current + bytes)) {
                return;
            }
        }
    }

    /**
     * Every few megabytes of admitted bytes, checks the file store's usable space against the margin; a file store call,
     * so it runs on the I/O pool, right before the write of the bytes it was asked about.
     */
    void checkFreeSpace(long bytes) throws IOException {
        if (admittedSinceCheck.addAndGet(bytes) < FREE_SPACE_CHECK_INTERVAL_BYTES) {
            return;
        }
        admittedSinceCheck.set(0);
        long usable = Environment.getFileStore(root).getUsableSpace();
        long margin = freeSpaceMarginBytes;
        if (usable < margin + bytes) {
            throw new SpillLimitExceededException(
                capSettingName,
                capBytes,
                bytesInUse(),
                bytes,
                "usable space on ["
                    + root
                    + "] is ["
                    + ByteSizeValue.ofBytes(usable)
                    + "], below the margin of ["
                    + ByteSizeValue.ofBytes(margin)
                    + "]"
            );
        }
    }

    void commit(long bytes) {
        // committed first: a concurrent reader of the two counters then over-reports, never under-reports
        committed.addAndGet(bytes);
        reserved.addAndGet(-bytes);
    }

    void unreserve(long bytes) {
        reserved.addAndGet(-bytes);
    }

    /**
     * Returns committed bytes once their file is gone; a file that remains, {@code failure} saying why, keeps its bytes
     * counted and goes into the debt.
     */
    void deleted(Path path, long bytes, @Nullable IOException failure) {
        if (failure == null) {
            committed.addAndGet(-bytes);
            return;
        }
        synchronized (debts) {
            debts.add(new Debt(path, bytes));
        }
        logger.warn(
            "could not delete spill file [{}] of [{}]; its bytes stay counted until it is gone",
            path,
            ByteSizeValue.ofBytes(bytes),
            failure
        );
    }

    private void retryDebts() {
        List<Debt> retry;
        synchronized (debts) {
            if (debts.isEmpty()) {
                return;
            }
            retry = new ArrayList<>(debts);
            debts.clear();
        }
        for (Debt debt : retry) {
            try {
                Files.deleteIfExists(debt.path);
                committed.addAndGet(-debt.bytes);
            } catch (IOException e) {
                synchronized (debts) {
                    debts.add(debt);
                }
                logger.debug("spill file [{}] still cannot be deleted", debt.path, e);
            }
        }
    }

    // ---- the I/O pool

    /**
     * Runs {@code work} on the I/O pool and completes the listener from there; a rejection is the operation's failure.
     */
    <T> void onIo(ActionListener<T> listener, CheckedSupplier<T, Exception> work) {
        ActionRunnable<T> task = ActionRunnable.supply(listener, work);
        try {
            io.execute(task);
        } catch (RejectedExecutionException e) {
            task.onRejection(e);
        }
    }

    @Override
    public void shutdown(ActionListener<Void> listener) {
        List<DiskSpillScope> open;
        synchronized (scopes) {
            shutdown = true;
            open = new ArrayList<>(scopes.values());
        }
        Consumer<ActionListener<Void>> finish = l -> onIo(l, () -> {
            retryDebts();
            return null;
        });
        if (open.isEmpty()) {
            finish.accept(listener);
            return;
        }
        GroupedActionListener<Void> closed = new GroupedActionListener<>(
            open.size(),
            listener.delegateFailureAndWrap((l, ignored) -> finish.accept(l))
        );
        for (DiskSpillScope scope : open) {
            scope.close(closed);
        }
    }
}
