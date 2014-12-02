/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.logging.ESLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;

/**
 * Elasticsearch utils to work with {@link java.nio.file.Path}
 */
public final class FileSystemUtils {

    private FileSystemUtils() {} // only static methods

    /**
     * Returns <code>true</code> iff a file under the given root has one of the given extensions. This method
     * will travers directories recursively and will terminate once any of the extensions was found. This
     * methods will not follow any links.
     *
     * @param root the root directory to travers. Must be a directory
     * @param extensions the file extensions to look for
     * @return <code>true</code> iff a file under the given root has one of the given extensions, otherwise <code>false</code>
     * @throws IOException if an IOException occurs or if the given root path is not a directory.
     */
    public static boolean hasExtensions(Path root, final String... extensions) throws IOException {
        final AtomicBoolean retVal = new AtomicBoolean(false);
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                for (String extension : extensions) {
                    if (file.getFileName().toString().endsWith(extension)) {
                        retVal.set(true);
                        return FileVisitResult.TERMINATE;
                    }
                }
                return super.visitFile(file, attrs);
            }
        });
        return retVal.get();
    }

    /**
     * Returns <code>true</code> iff one of the files exists otherwise <code>false</code>
     */
    public static boolean exists(Path... files) {
        for (Path file : files) {
            if (Files.exists(file)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Appends the path to the given base and strips N elements off the path if strip is > 0.
     */
    public static Path append(Path base, Path path, int strip) {
        for (Path subPath : path) {
            if (strip-- > 0) {
                continue;
            }
            base = base.resolve(subPath.toString());
        }
        return base;
    }

    /**
     * Deletes all subdirectories in the given path recursively
     * @throws java.lang.IllegalArgumentException if the given path is not a directory
     */
    public static void deleteSubDirectories(Path... paths) throws IOException {
        for (Path path : paths) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path subPath : stream) {
                    if (Files.isDirectory(subPath)) {
                        IOUtils.rm(subPath);
                    }
                }
            }
        }
    }


    /**
     * Check that a directory exists, is a directory and is readable
     * by the current user
     */
    public static boolean isAccessibleDirectory(Path directory, ESLogger logger) {
        assert directory != null && logger != null;

        if (!Files.exists(directory)) {
            logger.debug("[{}] directory does not exist.", directory.toAbsolutePath());
            return false;
        }
        if (!Files.isDirectory(directory)) {
            logger.debug("[{}] should be a directory but is not.", directory.toAbsolutePath());
            return false;
        }
        if (!Files.isReadable(directory)) {
            logger.debug("[{}] directory is not readable.", directory.toAbsolutePath());
            return false;
        }
        return true;
    }

    /**
     * Opens the given url for reading returning a {@code BufferedReader} that may be
     * used to read text from the URL in an efficient manner. Bytes from the
     * file are decoded into characters using the specified charset.
     */
    public static BufferedReader newBufferedReader(URL url, Charset cs) throws IOException {
        CharsetDecoder decoder = cs.newDecoder();
        Reader reader = new InputStreamReader(url.openStream(), decoder);
        return new BufferedReader(reader);
    }

    /**
     * This utility copy a full directory content (excluded) under
     * a new directory but without overwriting existing files.
     *
     * When a file already exists in destination dir, the source file is copied under
     * destination directory but with a suffix appended if set or source file is ignored
     * if suffix is not set (null).
     * @param source Source directory (for example /tmp/es/src)
     * @param destination Destination directory (destination directory /tmp/es/dst)
     * @param suffix When not null, files are copied with a suffix appended to the original name (eg: ".new")
     *               When null, files are ignored
     */
    public static void moveFilesWithoutOverwriting(Path source, final Path destination, final String suffix) throws IOException {

        // Create destination dir
        Files.createDirectories(destination);

        final int configPathRootLevel = source.getNameCount();

        // We walk through the file tree from
        Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
            private Path buildPath(Path path) {
                return destination.resolve(path);
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                // We are now in dir. We need to remove root of config files to have a relative path

                // If we are not walking in root dir, we might be able to copy its content
                // if it does not already exist
                if (configPathRootLevel != dir.getNameCount()) {
                    Path subpath = dir.subpath(configPathRootLevel, dir.getNameCount());
                    Path path = buildPath(subpath);
                    if (!Files.exists(path)) {
                        // We just move the structure to new dir
                        // we can't do atomic move here since src / dest might be on different mounts?
                        Files.move(dir, path);
                        // We just ignore sub files from here
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path subpath = null;

                if (configPathRootLevel != file.getNameCount()) {
                    subpath = file.subpath(configPathRootLevel, file.getNameCount());
                }
                Path path = buildPath(subpath);

                if (!Files.exists(path)) {
                    // We just move the new file to new dir
                    Files.move(file, path);
                } else if (suffix != null) {
                    // If it already exists we try to copy this new version appending suffix to its name
                    path = Paths.get(path.toString().concat(suffix));
                    // We just move the file to new dir but with a new name (appended with suffix)
                    Files.move(file, path, StandardCopyOption.REPLACE_EXISTING);
                }

                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Copy recursively a dir to a new location
     * @param source source dir
     * @param destination destination dir
     */
    public static void copyDirectoryRecursively(Path source, Path destination) throws IOException {
        Files.walkFileTree(source, new TreeCopier(source, destination));
    }

    static class TreeCopier extends SimpleFileVisitor<Path> {
        private final Path source;
        private final Path target;

        TreeCopier(Path source, Path target) {
            this.source = source;
            this.target = target;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            Path newDir = target.resolve(source.relativize(dir));
            try {
                Files.copy(dir, newDir);
            } catch (FileAlreadyExistsException x) {
                // We ignore this
            } catch (IOException x) {
                return SKIP_SUBTREE;
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Path newFile = target.resolve(source.relativize(file));
            try {
                Files.copy(file, newFile);
            } catch (IOException x) {
                // We ignore this
            }
            return CONTINUE;
        }
    }

    /**
     * Returns an array of all files in the given directory matching.
     */
    public static Path[] files(Path from, DirectoryStream.Filter<Path> filter) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(from, filter)) {
            return Iterators.toArray(stream.iterator(), Path.class);
        }
    }

    /**
     * Returns an array of all files in the given directory.
     */
    public static Path[] files(Path directory) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            return Iterators.toArray(stream.iterator(), Path.class);
        }
    }

    /**
     * Returns an array of all files in the given directory matching the glob.
     */
    public static Path[] files(Path directory, String glob) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, glob)) {
            return Iterators.toArray(stream.iterator(), Path.class);
        }
    }
}
