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

import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.elasticsearch.common.io.FileTestUtils.assertFileContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileNotExists;

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
    }

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
