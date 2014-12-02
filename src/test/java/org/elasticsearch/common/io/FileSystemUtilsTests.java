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

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

/**
 * Unit tests for {@link org.elasticsearch.common.io.FileSystemUtils}.
 */
public class FileSystemUtilsTests extends ElasticsearchTestCase {

    Path src;
    Path dst;

    @Before
    public void copySourceFilesToTarget() throws IOException {
        Path globalTempDir = globalTempDir().toPath();
        src = globalTempDir.resolve("iocopyappend-src");
        dst = globalTempDir.resolve("iocopyappend-dst");
        Files.createDirectories(src);
        Files.createDirectories(dst);

        // We first copy sources test files from src/test/resources
        // Because after when the test runs, src files are moved to their destination
        Properties props = new Properties();
        try (InputStream is = FileSystemUtilsTests.class.getResource("rootdir.properties").openStream()) {
            props.load(is);
        }

        FileSystemUtils.copyDirectoryRecursively(Paths.get(props.getProperty("copyappend.root.dir")), src);
    }

    @Test
    public void testMoveOverExistingFileAndAppend() throws IOException {

        FileSystemUtils.moveFilesWithoutOverwriting(src.resolve("v1"), dst, ".new");
        assertFileContent(dst, "file1.txt", "version1");
        assertFileContent(dst, "dir/file2.txt", "version1");

        FileSystemUtils.moveFilesWithoutOverwriting(src.resolve("v2"), dst, ".new");
        assertFileContent(dst, "file1.txt", "version1");
        assertFileContent(dst, "dir/file2.txt", "version1");
        assertFileContent(dst, "file1.txt.new", "version2");
        assertFileContent(dst, "dir/file2.txt.new", "version2");
        assertFileContent(dst, "file3.txt", "version1");
        assertFileContent(dst, "dir/subdir/file4.txt", "version1");

        FileSystemUtils.moveFilesWithoutOverwriting(src.resolve("v3"), dst, ".new");
        assertFileContent(dst, "file1.txt", "version1");
        assertFileContent(dst, "dir/file2.txt", "version1");
        assertFileContent(dst, "file1.txt.new", "version3");
        assertFileContent(dst, "dir/file2.txt.new", "version3");
        assertFileContent(dst, "file3.txt", "version1");
        assertFileContent(dst, "dir/subdir/file4.txt", "version1");
        assertFileContent(dst, "file3.txt.new", "version2");
        assertFileContent(dst, "dir/subdir/file4.txt.new", "version2");
        assertFileContent(dst, "dir/subdir/file5.txt", "version1");
    }

    @Test
    public void testMoveOverExistingFileAndIgnore() throws IOException {
        Path dest = globalTempDir().toPath();

        FileSystemUtils.moveFilesWithoutOverwriting(src.resolve("v1"), dest, null);
        assertFileContent(dest, "file1.txt", "version1");
        assertFileContent(dest, "dir/file2.txt", "version1");

        FileSystemUtils.moveFilesWithoutOverwriting(src.resolve("v2"), dest, null);
        assertFileContent(dest, "file1.txt", "version1");
        assertFileContent(dest, "dir/file2.txt", "version1");
        assertFileContent(dest, "file1.txt.new", null);
        assertFileContent(dest, "dir/file2.txt.new", null);
        assertFileContent(dest, "file3.txt", "version1");
        assertFileContent(dest, "dir/subdir/file4.txt", "version1");

        FileSystemUtils.moveFilesWithoutOverwriting(src.resolve("v3"), dest, null);
        assertFileContent(dest, "file1.txt", "version1");
        assertFileContent(dest, "dir/file2.txt", "version1");
        assertFileContent(dest, "file1.txt.new", null);
        assertFileContent(dest, "dir/file2.txt.new", null);
        assertFileContent(dest, "file3.txt", "version1");
        assertFileContent(dest, "dir/subdir/file4.txt", "version1");
        assertFileContent(dest, "file3.txt.new", null);
        assertFileContent(dest, "dir/subdir/file4.txt.new", null);
        assertFileContent(dest, "dir/subdir/file5.txt", "version1");
    }


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
            String fileContent = new String(Files.readAllBytes(file), UTF8);
            // trim the string content to prevent different handling on windows vs. unix and CR chars...
            Assert.assertThat(fileContent.trim(), equalTo(expected.trim()));
        }
    }

    @Test
    public void testAppend() {
        assertEquals(FileSystemUtils.append(Paths.get("/foo/bar"), Paths.get("/hello/world/this_is/awesome"), 0),
                Paths.get("/foo/bar/hello/world/this_is/awesome"));

        assertEquals(FileSystemUtils.append(Paths.get("/foo/bar"), Paths.get("/hello/world/this_is/awesome"), 2),
                Paths.get("/foo/bar/this_is/awesome"));

        assertEquals(FileSystemUtils.append(Paths.get("/foo/bar"), Paths.get("/hello/world/this_is/awesome"), 1),
                Paths.get("/foo/bar/world/this_is/awesome"));
    }
}
