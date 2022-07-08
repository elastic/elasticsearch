/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */

package org.elasticsearch.core;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utilities for common I/O methods. Borrowed heavily from Lucene (org.apache.lucene.util.IOUtils).
 */
public final class IOUtils {

    /**
     * UTF-8 charset string.
     * <p>Where possible, use {@link StandardCharsets#UTF_8} instead,
     * as using the String constant may slow things down.
     * @see StandardCharsets#UTF_8
     */
    public static final String UTF_8 = StandardCharsets.UTF_8.name();

    private IOUtils() {
        // Static utils methods
    }

    /**
     * Closes all given {@link Closeable}s. Some of the {@linkplain Closeable}s may be null; they are
     * ignored. After everything is closed, the method either throws the first exception it hit
     * while closing with other exceptions added as suppressed, or completes normally if there were
     * no exceptions.
     *
     * @param objects objects to close
     */
    public static void close(final Closeable... objects) throws IOException {
        close(null, objects);
    }

    /**
     * @see #close(Closeable...)
     */
    public static void close(@Nullable Closeable closeable) throws IOException {
        if (closeable != null) {
            closeable.close();
        }
    }

    /**
     * Closes all given {@link Closeable}s. Some of the {@linkplain Closeable}s may be null; they are
     * ignored. After everything is closed, the method adds any exceptions as suppressed to the
     * original exception, or throws the first exception it hit if {@code Exception} is null. If
     * no exceptions are encountered and the passed in exception is null, it completes normally.
     *
     * @param objects objects to close
     */
    public static void close(final Exception ex, final Closeable... objects) throws IOException {
        Exception firstException = ex;
        for (final Closeable object : objects) {
            try {
                close(object);
            } catch (final IOException | RuntimeException e) {
                firstException = addOrSuppress(firstException, e);
            }
        }

        if (firstException != null) {
            throwRuntimeOrIOException(firstException);
        }
    }

    private static void throwRuntimeOrIOException(Exception firstException) throws IOException {
        if (firstException instanceof IOException) {
            throw (IOException) firstException;
        } else {
            // since we only assigned an IOException or a RuntimeException to ex above, in this case ex must be a RuntimeException
            throw (RuntimeException) firstException;
        }
    }

    /**
     * Closes all given {@link Closeable}s. Some of the {@linkplain Closeable}s may be null; they are
     * ignored. After everything is closed, the method either throws the first exception it hit
     * while closing with other exceptions added as suppressed, or completes normally if there were
     * no exceptions.
     *
     * @param objects objects to close
     */
    public static void close(final Iterable<? extends Closeable> objects) throws IOException {
        Exception firstException = null;
        for (final Closeable object : objects) {
            try {
                close(object);
            } catch (final IOException | RuntimeException e) {
                firstException = addOrSuppress(firstException, e);
            }
        }

        if (firstException != null) {
            throwRuntimeOrIOException(firstException);
        }
    }

    private static Exception addOrSuppress(Exception firstException, Exception e) {
        if (firstException == null) {
            firstException = e;
        } else {
            firstException.addSuppressed(e);
        }
        return firstException;
    }

    /**
     * Closes all given {@link Closeable}s, suppressing all thrown exceptions. Some of the {@link Closeable}s may be null, they are ignored.
     *
     * @param objects objects to close
     */
    public static void closeWhileHandlingException(final Closeable... objects) {
        for (final Closeable object : objects) {
            closeWhileHandlingException(object);
        }
    }

    /**
     * Closes all given {@link Closeable}s, suppressing all thrown exceptions.
     *
     * @param objects objects to close
     *
     * @see #closeWhileHandlingException(Closeable...)
     */
    public static void closeWhileHandlingException(final Iterable<? extends Closeable> objects) {
        for (final Closeable object : objects) {
            closeWhileHandlingException(object);
        }
    }

    /**
     * @see #closeWhileHandlingException(Closeable...)
     */
    public static void closeWhileHandlingException(final Closeable closeable) {
        // noinspection EmptyCatchBlock
        try {
            close(closeable);
        } catch (final IOException | RuntimeException e) {}
    }

    /**
     * Deletes all given files, suppressing all thrown {@link IOException}s. Some of the files may be null, if so they are ignored.
     *
     * @param files the paths of files to delete
     */
    public static void deleteFilesIgnoringExceptions(final Path... files) {
        for (final Path name : files) {
            if (name != null) {
                // noinspection EmptyCatchBlock
                try {
                    Files.delete(name);
                } catch (final IOException ignored) {

                }
            }
        }
    }

    /**
     * Deletes one or more files or directories (and everything underneath it).
     *
     * @throws IOException if any of the given files (or their sub-hierarchy files in case of directories) cannot be removed.
     */
    public static void rm(final Path... locations) throws IOException {
        final LinkedHashMap<Path, Throwable> unremoved = rm(new LinkedHashMap<>(), locations);
        if (unremoved.isEmpty() == false) {
            final StringBuilder b = new StringBuilder("could not remove the following files (in the order of attempts):\n");
            for (final Map.Entry<Path, Throwable> kv : unremoved.entrySet()) {
                b.append("   ").append(kv.getKey().toAbsolutePath()).append(": ").append(kv.getValue()).append("\n");
            }
            throw new IOException(b.toString());
        }
    }

    private static LinkedHashMap<Path, Throwable> rm(final LinkedHashMap<Path, Throwable> unremoved, final Path... locations) {
        if (locations != null) {
            for (final Path location : locations) {
                // TODO: remove this leniency
                if (location != null && Files.exists(location)) {
                    try {
                        Files.walkFileTree(location, new FileVisitor<Path>() {
                            @Override
                            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult postVisitDirectory(final Path dir, final IOException impossible) throws IOException {
                                assert impossible == null;

                                try {
                                    Files.delete(dir);
                                } catch (final IOException e) {
                                    unremoved.put(dir, e);
                                }
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                                try {
                                    Files.delete(file);
                                } catch (final IOException exc) {
                                    unremoved.put(file, exc);
                                }
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
                                if (exc != null) {
                                    unremoved.put(file, exc);
                                }
                                return FileVisitResult.CONTINUE;
                            }
                        });
                    } catch (final IOException impossible) {
                        throw new AssertionError("visitor threw exception", impossible);
                    }
                }
            }
        }
        return unremoved;
    }

    // TODO: replace with constants class if needed (cf. org.apache.lucene.util.Constants)
    public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");
    public static final boolean LINUX = System.getProperty("os.name").startsWith("Linux");
    public static final boolean MAC_OS_X = System.getProperty("os.name").startsWith("Mac OS X");

    /**
     * Ensure that any writes to the given file is written to the storage device that contains it. The {@code isDir} parameter specifies
     * whether or not the path to sync is a directory. This is needed because we open for read and ignore an {@link IOException} since not
     * all filesystems and operating systems support fsyncing on a directory. For regular files we must open for write for the fsync to have
     * an effect.
     *
     * @param fileToSync the file to fsync
     * @param isDir      if true, the given file is a directory (we open for read and ignore {@link IOException}s, because not all file
     *                   systems and operating systems allow to fsync on a directory)
     */
    public static void fsync(final Path fileToSync, final boolean isDir) throws IOException {
        fsync(fileToSync, isDir, true);
    }

    /**
     * Ensure that any writes to the given file is written to the storage device that contains it. The {@code isDir} parameter specifies
     * whether or not the path to sync is a directory. This is needed because we open for read and ignore an {@link IOException} since not
     * all filesystems and operating systems support fsyncing on a directory. For regular files we must open for write for the fsync to have
     * an effect.
     *
     * @param fileToSync the file to fsync
     * @param isDir      if true, the given file is a directory (we open for read and ignore {@link IOException}s, because not all file
     *                   systems and operating systems allow to fsync on a directory)
     * @param metaData   if {@code true} both the file's content and metadata will be sync, otherwise only the file's content will be sync
     */
    public static void fsync(final Path fileToSync, final boolean isDir, final boolean metaData) throws IOException {
        if (isDir && WINDOWS) {
            // opening a directory on Windows fails, directories can not be fsynced there
            if (Files.exists(fileToSync) == false) {
                // yet do not suppress trying to fsync directories that do not exist
                throw new NoSuchFileException(fileToSync.toString());
            }
            return;
        }
        try (FileChannel file = FileChannel.open(fileToSync, isDir ? StandardOpenOption.READ : StandardOpenOption.WRITE)) {
            try {
                file.force(metaData);
            } catch (final IOException e) {
                if (isDir) {
                    assert (LINUX || MAC_OS_X) == false
                        : "on Linux and MacOSX fsyncing a directory should not throw IOException, "
                            + "we just don't want to rely on that in production (undocumented); got: "
                            + e;
                    // ignore exception if it is a directory
                    return;
                }
                // throw original exception
                throw e;
            }
        }
    }
}
