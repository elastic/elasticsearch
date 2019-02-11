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

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link org.elasticsearch.common.io.FileSystemUtils}.
 */
@SuppressFileSystems("WindowsFS") // tries to move away open file handles
public class FileSystemUtilsTests extends ESTestCase {

    private Path src;
    private Path dst;
    private Path txtFile;
    private byte[] expectedBytes;

    @Before
    public void copySourceFilesToTarget() throws IOException, URISyntaxException {
        src = createTempDir();
        dst = createTempDir();
        Files.createDirectories(src);
        Files.createDirectories(dst);
        txtFile = src.resolve("text-file.txt");

        try (ByteChannel byteChannel = Files.newByteChannel(txtFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            expectedBytes = new byte[3];
            expectedBytes[0] = randomByte();
            expectedBytes[1] = randomByte();
            expectedBytes[2] = randomByte();
            byteChannel.write(ByteBuffer.wrap(expectedBytes));
        }
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

    public void testOpenFileURLStream() throws IOException {
        URL urlWithWrongProtocol = new URL("http://www.google.com");
        try (InputStream is = FileSystemUtils.openFileURLStream(urlWithWrongProtocol)) {
            fail("Should throw IllegalArgumentException due to invalid protocol");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid protocol [http], must be [file] or [jar]", e.getMessage());
        }

        URL urlWithHost = new URL("file", "localhost", txtFile.toString());
        try (InputStream is = FileSystemUtils.openFileURLStream(urlWithHost)) {
            fail("Should throw IllegalArgumentException due to host");
        } catch (IllegalArgumentException e) {
            assertEquals("URL cannot have host. Found: [localhost]", e.getMessage());
        }

        URL urlWithPort = new URL("file", "", 80, txtFile.toString());
        try (InputStream is = FileSystemUtils.openFileURLStream(urlWithPort)) {
            fail("Should throw IllegalArgumentException due to port");
        } catch (IllegalArgumentException e) {
            assertEquals("URL cannot have port. Found: [80]", e.getMessage());
        }

        URL validUrl = txtFile.toUri().toURL();
        try (InputStream is = FileSystemUtils.openFileURLStream(validUrl)) {
            byte[] actualBytes = new byte[3];
            is.read(actualBytes);
            assertArrayEquals(expectedBytes, actualBytes);
        }
    }

    public void testIsDesktopServicesStoreFile() throws IOException {
        final Path path = createTempDir();
        final Path desktopServicesStore = path.resolve(".DS_Store");
        Files.createFile(desktopServicesStore);
        assertThat(FileSystemUtils.isDesktopServicesStore(desktopServicesStore), equalTo(Constants.MAC_OS_X));

        Files.delete(desktopServicesStore);
        Files.createDirectory(desktopServicesStore);
        assertFalse(FileSystemUtils.isDesktopServicesStore(desktopServicesStore));
    }

}
