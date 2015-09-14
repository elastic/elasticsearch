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

import java.nio.charset.StandardCharsets;

import org.elasticsearch.test.ESTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileNotExists;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

/**
 * Unit tests for {@link org.elasticsearch.common.io.FileSystemUtils}.
 */
@SuppressFileSystems("WindowsFS") // tries to move away open file handles
public class FileSystemUtilsTests extends ESTestCase {

    private Path src;
    private Path dst;

    @Before
    public void copySourceFilesToTarget() throws IOException, URISyntaxException {
        src = createTempDir();
        dst = createTempDir();
        Files.createDirectories(src);
        Files.createDirectories(dst);

        // We first copy sources test files from src/test/resources
        // Because after when the test runs, src files are moved to their destination
        final Path path = getDataPath("/org/elasticsearch/common/io/copyappend");
        FileSystemUtils.copyDirectoryRecursively(path, src);
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
        Path dest = createTempDir();

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

    @Test
    public void testMoveFilesDoesNotCreateSameFileWithSuffix() throws Exception {
        Path[] dirs = new Path[] { createTempDir(), createTempDir(), createTempDir()};
        for (Path dir : dirs) {
            Files.write(dir.resolve("file1.txt"), "file1".getBytes(StandardCharsets.UTF_8));
            Files.createDirectory(dir.resolve("dir"));
            Files.write(dir.resolve("dir").resolve("file2.txt"), "file2".getBytes(StandardCharsets.UTF_8));
        }

        FileSystemUtils.moveFilesWithoutOverwriting(dirs[0], dst, ".new");
        assertFileContent(dst, "file1.txt", "file1");
        assertFileContent(dst, "dir/file2.txt", "file2");

        // do the same operation again, make sure, no .new files have been added
        FileSystemUtils.moveFilesWithoutOverwriting(dirs[1], dst, ".new");
        assertFileContent(dst, "file1.txt", "file1");
        assertFileContent(dst, "dir/file2.txt", "file2");
        assertFileNotExists(dst.resolve("file1.txt.new"));
        assertFileNotExists(dst.resolve("dir").resolve("file2.txt.new"));

        // change file content, make sure it gets updated
        Files.write(dirs[2].resolve("dir").resolve("file2.txt"), "UPDATED".getBytes(StandardCharsets.UTF_8));
        FileSystemUtils.moveFilesWithoutOverwriting(dirs[2], dst, ".new");
        assertFileContent(dst, "file1.txt", "file1");
        assertFileContent(dst, "dir/file2.txt", "file2");
        assertFileContent(dst, "dir/file2.txt.new", "UPDATED");
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
            String fileContent = new String(Files.readAllBytes(file), java.nio.charset.StandardCharsets.UTF_8);
            // trim the string content to prevent different handling on windows vs. unix and CR chars...
            Assert.assertThat(fileContent.trim(), equalTo(expected.trim()));
        }
    }

    @Test
    public void testAppend() {
        assertEquals(FileSystemUtils.append(PathUtils.get("/foo/bar"), PathUtils.get("/hello/world/this_is/awesome"), 0),
            PathUtils.get("/foo/bar/hello/world/this_is/awesome"));

        assertEquals(FileSystemUtils.append(PathUtils.get("/foo/bar"), PathUtils.get("/hello/world/this_is/awesome"), 2),
                PathUtils.get("/foo/bar/this_is/awesome"));

        assertEquals(FileSystemUtils.append(PathUtils.get("/foo/bar"), PathUtils.get("/hello/world/this_is/awesome"), 1),
                PathUtils.get("/foo/bar/world/this_is/awesome"));
    }

    public void testIsHidden() {
        for (String p : Arrays.asList(
                "/",
                "foo",
                "/foo",
                "foo.bar",
                "/foo.bar",
                "foo/bar",
                "foo/./bar",
                "foo/../bar",
                "/foo/./bar",
                "/foo/../bar"
                )) {
            Path path = PathUtils.get(p);
            assertFalse(FileSystemUtils.isHidden(path));
        }
        for (String p : Arrays.asList(
                ".hidden",
                ".hidden.ext",
                "/.hidden",
                "/.hidden.ext",
                "foo/.hidden",
                "foo/.hidden.ext",
                "/foo/.hidden",
                "/foo/.hidden.ext",
                ".",
                "..",
                "foo/.",
                "foo/.."
                )) {
            Path path = PathUtils.get(p);
            assertTrue(FileSystemUtils.isHidden(path));
        }
    }
}
