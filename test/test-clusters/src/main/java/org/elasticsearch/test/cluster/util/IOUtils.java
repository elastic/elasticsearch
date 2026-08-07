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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public final class IOUtils {
    private static final Logger LOGGER = LogManager.getLogger(IOUtils.class);
    private static final int RETRY_DELETE_MILLIS = OS.current() == OS.WINDOWS ? 500 : 0;
    private static final int MAX_RETRY_DELETE_TIMES = OS.current() == OS.WINDOWS ? 15 : 0;

    /**
     * Marker prefixed to every log line and error message emitted by the distribution-copy self-heal in
     * {@link #syncMaybeWithLinks(Path, Path)}. Grep for this token when diagnosing incomplete test-cluster
     * distribution copies (see https://github.com/elastic/elasticsearch/issues/149129).
     */
    static final String SELF_HEAL_MARKER = "[distribution-copy-self-heal]";

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
            cleanCopy(sourceRoot, destinationRoot);
        }

        verifyDistributionCopyComplete(sourceRoot, destinationRoot);
    }

    /**
     * Verifies that {@code destinationRoot} contains every file present under {@code sourceRoot} and, when it does not,
     * self-heals with a clean copy. An incomplete distribution copy otherwise manifests much later as a cryptic failure
     * that is very hard to triage, e.g. a CLI tool such as {@code elasticsearch-keystore.bat} failing with a
     * {@code ClassNotFoundException} because a jar was silently dropped from its classpath
     * (see https://github.com/elastic/elasticsearch/issues/149129).
     * <p>
     * Every log line and the failure message are tagged with {@link #SELF_HEAL_MARKER} so they are trivial to grep for
     * when diagnosing distribution-copy issues.
     *
     * @throws UncheckedIOException if files are still missing after a clean copy (i.e. the source itself is incomplete)
     */
    static void verifyDistributionCopyComplete(Path sourceRoot, Path destinationRoot) {
        List<String> missing = findMissingFiles(sourceRoot, destinationRoot);
        if (missing.isEmpty()) {
            return;
        }

        LOGGER.warn(
            "{} incomplete distribution copy detected from [{}] to [{}]: {} file(s) missing, self-healing with a clean copy. "
                + "Missing files: {}",
            SELF_HEAL_MARKER,
            sourceRoot,
            destinationRoot,
            missing.size(),
            missing
        );
        cleanCopy(sourceRoot, destinationRoot);
        missing = findMissingFiles(sourceRoot, destinationRoot);
        if (missing.isEmpty() == false) {
            throw new UncheckedIOException(
                new IOException(
                    SELF_HEAL_MARKER
                        + " self-heal FAILED: incomplete distribution copy from "
                        + sourceRoot
                        + " to "
                        + destinationRoot
                        + "; "
                        + missing.size()
                        + " file(s) still missing after a clean copy: "
                        + missing
                )
            );
        }
        LOGGER.warn(
            "{} self-heal succeeded: distribution copy to [{}] is now complete after a clean copy.",
            SELF_HEAL_MARKER,
            destinationRoot
        );
    }

    /** Deletes {@code destinationRoot} if present and re-populates it with a full content copy of {@code sourceRoot}. */
    private static void cleanCopy(Path sourceRoot, Path destinationRoot) {
        try {
            deleteWithRetry(destinationRoot);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        syncWithCopy(sourceRoot, destinationRoot);
    }

    /**
     * Returns the source-relative paths of all regular files present under {@code sourceRoot} but absent from
     * {@code destinationRoot}. Transient JVM artifacts (e.g. {@code .attach_pid} files) are ignored since they are
     * not part of the distribution and may appear or vanish between walks.
     */
    static List<String> findMissingFiles(Path sourceRoot, Path destinationRoot) {
        List<String> missing = new ArrayList<>();
        try {
            Files.walkFileTree(sourceRoot, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path source, BasicFileAttributes attrs) {
                    Path relative = sourceRoot.relativize(source);
                    if (relative.toString().contains(".attach_pid")) {
                        return FileVisitResult.CONTINUE;
                    }
                    if (Files.exists(destinationRoot.resolve(relative)) == false) {
                        missing.add(relative.toString());
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    // Ignore files that disappear mid-walk (e.g. JVM .attach_pid files); they aren't part of the distribution.
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Can't walk source " + sourceRoot, e);
        }
        return missing;
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
