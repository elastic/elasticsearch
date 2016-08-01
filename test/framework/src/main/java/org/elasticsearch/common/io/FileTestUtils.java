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

import org.elasticsearch.common.Nullable;
import org.junit.Assert;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

/** test helper methods for working with files */
public class FileTestUtils {

    /**
     * Check that a file contains a given String
     * @param dir root dir for file
     * @param filename relative path from root dir to file
     * @param expected expected content (if null, we don't expect any file)
     */
    public static void assertFileContent(Path dir, String filename, String expected) throws IOException {
        Assert.assertThat(Files.exists(dir), is(true));
        Path file = dir.resolve(filename);
        if (expected == null) {
            Assert.assertThat("file [" + file + "] should not exist.", Files.exists(file), is(false));
        } else {
            assertFileExists(file);
            String fileContent = new String(Files.readAllBytes(file), java.nio.charset.StandardCharsets.UTF_8);
            // trim the string content to prevent different handling on windows vs. unix and CR chars...
            Assert.assertThat(fileContent.trim(), equalTo(expected.trim()));
        }
    }

    /**
     * Unzip a zip file to a destination directory.  If the zip file does not exist, an IOException is thrown.
     * If the destination directory does not exist, it will be created.
     *
     * @param zip      zip file to unzip
     * @param destDir  directory to unzip the file to
     * @param prefixToRemove  the (optional) prefix in the zip file path to remove when writing to the destination directory
     * @throws IOException if zip file does not exist, or there was an error reading from the zip file or
     *                     writing to the destination directory
     */
    public static void unzip(final Path zip, final Path destDir, @Nullable final String prefixToRemove) throws IOException {
        if (Files.notExists(zip)) {
            throw new IOException("[" + zip + "] zip file must exist");
        }
        Files.createDirectories(destDir);

        try (final ZipInputStream zipInput = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            while ((entry = zipInput.getNextEntry()) != null) {
                final String entryPath;
                if (prefixToRemove != null) {
                    if (entry.getName().startsWith(prefixToRemove)) {
                        entryPath = entry.getName().substring(prefixToRemove.length());
                    } else {
                        throw new IOException("prefix not found: " + prefixToRemove);
                    }
                } else {
                    entryPath = entry.getName();
                }
                final Path path = Paths.get(destDir.toString(), entryPath);
                if (entry.isDirectory()) {
                    Files.createDirectories(path);
                } else {
                    Files.copy(zipInput, path);
                }
                zipInput.closeEntry();
            }
        }
    }
}
