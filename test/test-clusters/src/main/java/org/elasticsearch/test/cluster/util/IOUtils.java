/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public final class IOUtils {
    private static final Logger LOGGER = LogManager.getLogger(IOUtils.class);
    private static final int RETRY_DELETE_MILLIS = OS.current() == OS.WINDOWS ? 500 : 0;
    private static final int MAX_RETRY_DELETE_TIMES = OS.current() == OS.WINDOWS ? 15 : 0;

    private IOUtils() {}

    /**
     * Deletes a path, retrying if necessary.
     *
     * @param path  the path to delete
     * @throws IOException
     *         if an I/O error occurs
     */
    public static void deleteWithRetry(Path path) throws IOException {
        try {
            deleteWithRetry0(path);
        } catch (InterruptedException x) {
            throw new IOException("Interrupted while deleting.", x);
        }
    }

    /** Unchecked variant of deleteWithRetry. */
    public static void uncheckedDeleteWithRetry(Path path) {
        try {
            deleteWithRetry0(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException x) {
            throw new UncheckedIOException("Interrupted while deleting.", new IOException());
        }
    }

    /**
     * Attempts to do a copy via linking, falling back to a normal copy if an exception is encountered.
     *
     * @see #syncWithLinks(Path, Path)
     * @see #syncWithCopy(Path, Path)
     * @param sourceRoot      where to copy from
     * @param destinationRoot destination to link to
     */
    public static void syncMaybeWithLinks(Path sourceRoot, Path destinationRoot) {
        try {
            syncWithLinks(sourceRoot, destinationRoot);
        } catch (LinkCreationException e) {
            // Note does not work for network drives, e.g. Vagrant
            LOGGER.info("Failed to sync using hard links. Falling back to copy.", e);
            // ensure we get a clean copy
            try {
                deleteWithRetry(destinationRoot);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
            syncWithCopy(sourceRoot, destinationRoot);
        }
    }

    /**
     * Does the equivalent of `cp -lr` and `chmod -r a-w` to save space and improve speed.
     * We remove write permissions to make sure files are note mistakenly edited ( e.x. the config file ) and changes
     * reflected across all copies. Permissions are retained to be able to replace the links.
     *
     * @param sourceRoot      where to copy from
     * @param destinationRoot destination to link to
     */
    public static void syncWithLinks(Path sourceRoot, Path destinationRoot) {
        sync(sourceRoot, destinationRoot, (Path d, Path s) -> {
            try {
                Files.createLink(d, s);
            } catch (IOException e) {
                // Note does not work for network drives, e.g. Vagrant
                throw new LinkCreationException("Failed to create hard link " + d + " pointing to " + s, e);
            }
        });
    }

    /**
     * Sync source folder to destination folder. This method does an actual copy of file contents. When possible,
     * {@link #syncWithLinks(Path, Path)} is preferred for better performance when the synced contents don't need to be subsequently
     * modified.
     *
     * @param sourceRoot      where to copy from
     * @param destinationRoot destination to link to
     */
    public static void syncWithCopy(Path sourceRoot, Path destinationRoot) {
        sync(sourceRoot, destinationRoot, (Path d, Path s) -> {
            try {
                Files.copy(s, d);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to copy " + s + " to " + d, e);
            }
        });
    }

    private static void sync(Path sourceRoot, Path destinationRoot, BiConsumer<Path, Path> syncMethod) {
        assert Files.exists(destinationRoot) == false;
        try {
            Files.walkFileTree(sourceRoot, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Path relativeDestination = sourceRoot.relativize(dir);
                    Path destination = destinationRoot.resolve(relativeDestination);
                    Files.createDirectories(destination);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path source, BasicFileAttributes attrs) throws IOException {
                    Path relativeDestination = sourceRoot.relativize(source);
                    Path destination = destinationRoot.resolve(relativeDestination);
                    Files.createDirectories(destination.getParent());
                    syncMethod.accept(destination, source);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    if (exc instanceof NoSuchFileException noFileException) {
                        // Ignore these files that are sometimes left behind by the JVM
                        if (noFileException.getFile() != null && noFileException.getFile().contains(".attach_pid")) {
                            LOGGER.info("Ignoring file left behind by JVM: {}", noFileException.getFile());
                            return FileVisitResult.CONTINUE;
                        }
                    }
                    throw exc;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Can't walk source " + sourceRoot, e);
        }
    }

    // The exception handling here is loathsome, but necessary!
    // TODO: Some of the loathsomeness here was copied from our Gradle plugin that was required because of Gradle exception wrapping. That
    // may no longer be strictly necessary in this context.
    private static void deleteWithRetry0(Path path) throws IOException, InterruptedException {
        int times = 0;
        IOException ioe = null;
        while (true) {
            try {
                recursiveDelete(path);
                times++;
                // Checks for absence of the file. Semantics of Files.exists() is not the same.
                while (Files.notExists(path) == false) {
                    if (times > MAX_RETRY_DELETE_TIMES) {
                        throw new IOException("File still exists after " + times + " waits.");
                    }
                    Thread.sleep(RETRY_DELETE_MILLIS);
                    // retry
                    recursiveDelete(path);
                    times++;
                }
                break;
            } catch (NoSuchFileException ignore) {
                // already deleted, ignore
                break;
            } catch (IOException x) {
                if (x.getCause() instanceof NoSuchFileException) {
                    // already deleted, ignore
                    break;
                }
                // Backoff/retry in case another process is accessing the file
                times++;
                if (ioe == null) ioe = new IOException();
                ioe.addSuppressed(x);
                if (times > MAX_RETRY_DELETE_TIMES) throw ioe;
                Thread.sleep(RETRY_DELETE_MILLIS);
            }
        }
    }

    private static void recursiveDelete(Path path) throws IOException {
        try (Stream<Path> files = Files.walk(path)) {
            files.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    public static class LinkCreationException extends UncheckedIOException {
        LinkCreationException(String message, IOException cause) {
            super(message, cause);
        }
    }
}
