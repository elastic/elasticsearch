/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.StreamSupport;

/**
 * Elasticsearch utils to work with {@link java.nio.file.Path}
 */
public final class FileSystemUtils {

    private FileSystemUtils() {} // only static methods

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
     * Check whether the file denoted by the given path is hidden.
     * In practice, this will check if the file name starts with a dot.
     * This should be preferred to {@link Files#isHidden(Path)} as this
     * does not depend on the operating system.
     */
    public static boolean isHidden(Path path) {
        Path fileName = path.getFileName();
        if (fileName == null) {
            return false;
        }
        return fileName.toString().startsWith(".");
    }

    /**
     * Check whether the file denoted by the given path is a desktop services store created by Finder on macOS.
     *
     * @param path the path
     * @return true if the current system is macOS and the specified file appears to be a desktop services store file
     */
    public static boolean isDesktopServicesStore(final Path path) {
        return Constants.MAC_OS_X && Files.isRegularFile(path) && ".DS_Store".equals(path.getFileName().toString());
    }

    /**
     * Appends the path to the given base and strips N elements off the path if strip is &gt; 0.
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
    public static boolean isAccessibleDirectory(Path directory, Logger logger) {
        assert directory != null && logger != null;

        if (Files.exists(directory) == false) {
            logger.debug("[{}] directory does not exist.", directory.toAbsolutePath());
            return false;
        }
        if (Files.isDirectory(directory) == false) {
            logger.debug("[{}] should be a directory but is not.", directory.toAbsolutePath());
            return false;
        }
        if (Files.isReadable(directory) == false) {
            logger.debug("[{}] directory is not readable.", directory.toAbsolutePath());
            return false;
        }
        return true;
    }

    /**
     * Returns an InputStream the given url if the url has a protocol of 'file' or 'jar', no host, and no port.
     */
    @SuppressForbidden(reason = "Will only open url streams for local files")
    public static InputStream openFileURLStream(URL url) throws IOException {
        String protocol = url.getProtocol();
        if ("file".equals(protocol) == false && "jar".equals(protocol) == false) {
            throw new IllegalArgumentException("Invalid protocol [" + protocol + "], must be [file] or [jar]");
        }
        if (Strings.isEmpty(url.getHost()) == false) {
            throw new IllegalArgumentException("URL cannot have host. Found: [" + url.getHost() + ']');
        }
        if (url.getPort() != -1) {
            throw new IllegalArgumentException("URL cannot have port. Found: [" + url.getPort() + ']');
        }
        return url.openStream();
    }

    /**
     * Returns an array of all files in the given directory matching.
     */
    public static Path[] files(Path from, DirectoryStream.Filter<Path> filter) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(from, filter)) {
            return toArray(stream);
        }
    }

    /**
     * Returns an array of all files in the given directory.
     */
    public static Path[] files(Path directory) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            return toArray(stream);
        }
    }

    /**
     * Returns an array of all files in the given directory matching the glob.
     */
    public static Path[] files(Path directory, String glob) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, glob)) {
            return toArray(stream);
        }
    }

    private static Path[] toArray(DirectoryStream<Path> stream) {
        return StreamSupport.stream(stream.spliterator(), false).toArray(length -> new Path[length]);
    }

}
