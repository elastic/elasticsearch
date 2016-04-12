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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.logging.ESLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
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
