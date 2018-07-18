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

package org.elasticsearch.packaging.util;

import org.elasticsearch.core.internal.io.IOUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.text.IsEmptyString.isEmptyOrNullString;

/**
 * Wrappers and convenience methods for common filesystem operations
 */
public class FileUtils {

    public static List<Path> lsGlob(Path directory, String glob) {
        List<Path> paths = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, glob)) {

            for (Path path : stream) {
                paths.add(path);
            }
            return paths;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void rm(Path... paths) {
        try {
            IOUtils.rm(paths);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path mkdir(Path path) {
        try {
            return Files.createDirectories(path);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
     }

     public static Path cp(Path source, Path target) {
        try {
            return Files.copy(source, target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path mv(Path source, Path target) {
        try {
            return Files.move(source, target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void append(Path file, String text) {
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {

            writer.write(text);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String slurp(Path file) {
        try {
            return String.join("\n", Files.readAllLines(file, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the owner of a file in a way that should be supported by all filesystems that have a concept of file owner
     */
    public static String getFileOwner(Path path) {
        try {
            FileOwnerAttributeView view = Files.getFileAttributeView(path, FileOwnerAttributeView.class);
            return view.getOwner().getName();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets attributes that are supported by all filesystems
     */
    public static BasicFileAttributes getBasicFileAttributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets attributes that are supported by posix filesystems
     */
    public static PosixFileAttributes getPosixFileAttributes(Path path) {
        try {
            return Files.readAttributes(path, PosixFileAttributes.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // vagrant creates /tmp for us in windows so we use that to avoid long paths
    public static Path getTempDir() {
        return Paths.get("/tmp");
    }

    public static Path getDefaultArchiveInstallPath() {
        return getTempDir().resolve("elasticsearch");
    }

    public static String getCurrentVersion() {
        return slurp(getPackagingArchivesDir().resolve("version"));
    }

    public static Path getPackagingArchivesDir() {
        String fromEnv = System.getenv("PACKAGING_ARCHIVES");
        assertThat(fromEnv, not(isEmptyOrNullString()));
        return Paths.get(fromEnv);
    }
}
