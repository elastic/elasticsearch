/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.fs.quotaaware;

import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

/**
 * QuotaAwareFileSystemProvider is intended to be used as a wrapper around the
 * default file system provider of the JVM.
 *
 * It augments the default file system by allowing to report total size and
 * remaining capacity according to some externally defined quota that is not
 * readily available to the JVM. Essentially it is a workaround for containers
 * that see the size of the host volume and not the effective quota available to
 * them. It will however never report larger capacity than what the underlying
 * file system sees.
 *
 * In application usage:
 * <ol>
 * <li>Include this project in the class path</li>
 * <li>Specify this argument at JVM boot:
 * <code>-Djava.nio.file.spi.DefaultFileSystemProvider=co.elastic.cloud.quotaawarefs.QuotaAwareFileSystemProvider</code>
 * </li>
 * <li>Have some external system check the quota usage and
 * write it to the path specified by the system property {@value #QUOTA_PATH_KEY}.</li>
 * </ol>
 *
 * In any case the quota file must be a {@link Properties} file with the
 * properties <code>total</code> and <code>remaining</code>. Both properties are
 * {@link Long} values, parsed according to {@link Long#parseLong(String)} and
 * assumed to be in bytes.
 *
 * Sample format:
 *
 * <pre>
 * {@code
 * total=5000
 * remaining=200
 * }
 * </pre>
 */
public class QuotaAwareFileSystemProvider extends FileSystemProvider implements AutoCloseable {

    private static final int CHECK_PERIOD = 1000;

    private final class RefreshLimitsTask extends TimerTask {
        @Override
        public void run() {
            try {
                assert timerThread == Thread.currentThread() : "QuotaAwareFileSystemProvider doesn't support multithreaded timer.";
                refreshLimits();
            } catch (Exception e) {
                // Canceling from the timer Thread guarantees last execution,
                // so no need to check for duplicate error
                error.set(e);
                timer.cancel();
            }
        }

    }

    static final String QUOTA_PATH_KEY = "es.fs.quota.file";

    private final FileSystemProvider delegate;
    private final Path configPath;

    private volatile long total = Long.MAX_VALUE;
    private volatile long remaining = Long.MAX_VALUE;

    private final Timer timer;

    private final ConcurrentHashMap<FileSystem, QuotaAwareFileSystem> systemsCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<FileStore, QuotaAwareFileStore> storesCache = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private static final AtomicInteger NEXT_SERIAL = new AtomicInteger(0);
    private final String timerThreadName = "QuotaAwareFSTimer-" + NEXT_SERIAL.getAndIncrement();
    private final Thread timerThread;

    public QuotaAwareFileSystemProvider(FileSystemProvider delegate) throws Exception {
        this(delegate, URI.create(getUri()));
    }

    private static String getUri() {
        final String property = System.getProperty(QUOTA_PATH_KEY);
        if (property == null) {
            throw new IllegalArgumentException(
                "Property "
                    + QUOTA_PATH_KEY
                    + " must be set to a URI in order to use the quota filesystem provider, e.g. using -D"
                    + QUOTA_PATH_KEY
                    + "=file://path/to/fsquota.properties"
            );
        }

        return property;
    }

    public QuotaAwareFileSystemProvider(FileSystemProvider delegate, URI config) throws Exception {
        if (delegate instanceof QuotaAwareFileSystemProvider) {
            throw new IllegalArgumentException("Delegate provider cannot be an instance of QuotaAwareFileSystemProvider");
        }
        this.delegate = delegate;
        configPath = delegate.getPath(config);
        refreshLimits(); // Ensures that a parseable file exists before
                         // timer is created
        timer = new Timer(timerThreadName, true);
        try {
            timerThread = getThreadFromTimer(timer).get();
            timer.schedule(new RefreshLimitsTask(), CHECK_PERIOD, CHECK_PERIOD);
        } catch (Exception e1) {
            if (e1 instanceof InterruptedException) {
                Thread.currentThread().interrupt(); // Restore interrupted flag
            }
            try {
                // Avoid thread leak if this failed to start
                timer.cancel();
            } catch (Exception e2) {
                e1.addSuppressed(e2);
            }
            throw e1;
        }
    }

    /**
     * Extracts the {@link Thread} used by a {@link Timer}.
     *
     * Ideally {@link Timer} would provide necessary health checks or error
     * handling support that this would not be required.
     *
     * @param timer
     *            The {@link Timer} instance to extract thread from
     * @return the Thread used by the given timer
     * @throws IllegalStateException
     *             if Timer is cancelled.
     */
    private static Future<Thread> getThreadFromTimer(Timer timer) {
        FutureTask<Thread> timerThreadFuture = new FutureTask<>(() -> Thread.currentThread());
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                timerThreadFuture.run();
            }
        }, 0);
        return timerThreadFuture;
    }

    /**
     * Performs a single attempt at reading the required files.
     *
     * @throws PrivilegedActionException if something goes wrong
     * @throws IOException if something goes wrong
     *
     * @throws IllegalStateException
     *             if security manager denies reading the property file
     * @throws IllegalStateException
     *             if the property file cannot be parsed
     */
    private void refreshLimits() throws IOException, PrivilegedActionException {
        try (InputStream newInputStream = AccessController.doPrivileged(new PrivilegedExceptionAction<InputStream>() {
            @Override
            public InputStream run() throws Exception {
                return delegate.newInputStream(configPath, StandardOpenOption.READ);
            }
        })) {
            Properties properties = new Properties();
            properties.load(newInputStream);
            total = Long.parseLong(properties.getProperty("total"));
            remaining = Long.parseLong(properties.getProperty("remaining"));
        }
    }

    @Override
    public String getScheme() {
        return "file";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        // Delegate handles throwing exception if filesystem already exists
        return getFS(delegate.newFileSystem(uri, env));

    }

    private QuotaAwareFileSystem getFS(FileSystem fileSystem) {
        return systemsCache.computeIfAbsent(fileSystem, (delegate) -> new QuotaAwareFileSystem(this, delegate));
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        // Delegate handles throwing exception if filesystem doesn't exist, but
        // delegate may also have precreated filesystems, like the default file
        // system.
        return getFS(delegate.getFileSystem(uri));
    }

    @Override
    public Path getPath(URI uri) {
        return ensureWrapped(delegate.getPath(uri));
    }

    private Path ensureWrapped(Path path) {
        if (path instanceof QuotaAwarePath) {
            assert path.getFileSystem().provider() == this;
            // Delegate may use this instance to create the path when this
            // instance is installed as the default provider.
            // This is safe because nested QuotaAwareProviders are prohibited,
            // otherwise this would require unwrapping.
            return path;
        } else {
            return new QuotaAwarePath(getFS(path.getFileSystem()), path);
        }
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        return delegate.newByteChannel(QuotaAwarePath.unwrap(path), options, attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
        return new DirectoryStream<Path>() {
            DirectoryStream<Path> stream = delegate.newDirectoryStream(QuotaAwarePath.unwrap(dir), filter);

            @Override
            public void close() throws IOException {
                stream.close();
            }

            @Override
            public Iterator<Path> iterator() {
                return StreamSupport.stream(stream.spliterator(), false).map(QuotaAwareFileSystemProvider.this::ensureWrapped).iterator();
            }
        };
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        delegate.createDirectory(QuotaAwarePath.unwrap(dir), attrs);
    }

    @Override
    public void delete(Path path) throws IOException {
        delegate.delete(QuotaAwarePath.unwrap(path));
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        delegate.copy(QuotaAwarePath.unwrap(source), QuotaAwarePath.unwrap(target), options);
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        delegate.move(QuotaAwarePath.unwrap(source), QuotaAwarePath.unwrap(target), options);
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return delegate.isSameFile(QuotaAwarePath.unwrap(path), QuotaAwarePath.unwrap(path2));
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return delegate.isHidden(QuotaAwarePath.unwrap(path));
    }

    @Override
    public QuotaAwareFileStore getFileStore(Path path) throws IOException {
        return getFileStore(delegate.getFileStore(QuotaAwarePath.unwrap(path)));
    }

    QuotaAwareFileStore getFileStore(FileStore store) {
        return storesCache.computeIfAbsent(store, (fs) -> new QuotaAwareFileStore(QuotaAwareFileSystemProvider.this, fs));
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        delegate.checkAccess(QuotaAwarePath.unwrap(path), modes);
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return delegate.getFileAttributeView(QuotaAwarePath.unwrap(path), type, options);
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        try {
            return delegate.readAttributes(QuotaAwarePath.unwrap(path), type, options);
        } catch (ProviderMismatchException e) {
            throw new IllegalArgumentException("Failed to read attributes for path: [" + path + "]", e);
        }
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        return delegate.readAttributes(QuotaAwarePath.unwrap(path), attributes, options);
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        delegate.setAttribute(QuotaAwarePath.unwrap(path), attribute, value, options);
    }

    long getTotal() {
        ensureHealth();
        return total;
    }

    void ensureHealth() throws AssertionError {
        boolean timerIsAlive = timerThread.isAlive();
        Throwable cause = error.get();
        if (cause != null || timerIsAlive == false) {
            throw new AssertionError("The quota aware filesystem has failed", cause);
        }
    }

    long getRemaining() {
        ensureHealth();
        return remaining;
    }

    @Override
    public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        return delegate.newFileChannel(QuotaAwarePath.unwrap(path), options, attrs);
    }

    @Override
    public AsynchronousFileChannel newAsynchronousFileChannel(
        Path path,
        Set<? extends OpenOption> options,
        ExecutorService executor,
        FileAttribute<?>... attrs
    ) throws IOException {
        return delegate.newAsynchronousFileChannel(QuotaAwarePath.unwrap(path), options, executor, attrs);
    }

    /**
     * Normally only used in testing. Avoids thread leak when life cycle of this
     * object doesn't follow that of the JVM.
     *
     * @throws IOException if something goes wrong
     */
    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            timer.cancel();

            // If there was a currently executing task wait for it, to
            // avoid false positives on file handle leak.
            try {
                timerThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted flag
            }

            try {
                IOUtils.close(systemsCache.values());
            } finally {
                storesCache.clear();
            }
        }
    }

    @Override
    public void createLink(Path link, Path existing) throws IOException {
        delegate.createLink(QuotaAwarePath.unwrap(link), QuotaAwarePath.unwrap(existing));
    }

    @Override
    public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws IOException {
        delegate.createSymbolicLink(QuotaAwarePath.unwrap(link), QuotaAwarePath.unwrap(target), attrs);
    }

    void purge(FileSystem delegateFileSystem) {
        systemsCache.remove(delegateFileSystem);
    }
}
