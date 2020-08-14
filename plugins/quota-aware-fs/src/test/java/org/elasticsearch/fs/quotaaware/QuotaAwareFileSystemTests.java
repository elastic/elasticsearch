/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.fs.quotaaware;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.lucene.mockfile.MockFileSystemTestCase;
import org.elasticsearch.common.SuppressForbidden;

public class QuotaAwareFileSystemTests extends MockFileSystemTestCase {

    private QuotaAwareFileSystemProvider quotaAwareFileSystemProvider;

    @Override
    @SuppressForbidden(reason = "accesses the default filesystem by design")
    public void setUp() throws Exception {
        super.setUp();
        Path quotaPath = createTempDir().resolve("fsquota.properties");
        Properties properties = new Properties();
        properties.put("total", Long.toString(Long.MAX_VALUE));
        properties.put("remaining", Long.toString(Long.MAX_VALUE));
        try (OutputStream outputStream = Files.newOutputStream(quotaPath)) {
            properties.store(outputStream, "");
        }
        quotaAwareFileSystemProvider = new QuotaAwareFileSystemProvider(FileSystems.getDefault().provider(), quotaPath.toUri());
    }

    @Override
    protected Path wrap(Path path) {
        return new QuotaAwarePath(new QuotaAwareFileSystem(quotaAwareFileSystemProvider, path.getFileSystem()), path);
    }

    @Override
    public void tearDown() throws Exception {
        quotaAwareFileSystemProvider.close();
        super.tearDown();
    }

    /** Tests that newDirectoryStream with a filter works correctly */
    @Override
    public void testDirectoryStreamFiltered() throws IOException {
        Path dir = wrap(createTempDir());

        OutputStream file = Files.newOutputStream(dir.resolve("file1"));
        file.write(5);
        file.close();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            int count = 0;
            for (Path path : stream) {
                assertTrue(path instanceof QuotaAwarePath);
                if (!path.getFileName().toString().startsWith("extra")) {
                    count++;
                }
            }
            assertEquals(1, count);
        }
        dir.getFileSystem().close();
    }

    /** Tests that newDirectoryStream with globbing works correctly */
    @Override
    public void testDirectoryStreamGlobFiltered() throws IOException {
        Path dir = wrap(createTempDir());

        OutputStream file = Files.newOutputStream(dir.resolve("foo"));
        file.write(5);
        file.close();
        file = Files.newOutputStream(dir.resolve("bar"));
        file.write(5);
        file.close();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "f*")) {
            int count = 0;
            for (Path path : stream) {
                assertTrue(path instanceof QuotaAwarePath);
                ++count;
            }
            assertEquals(1, count);
        }
        dir.getFileSystem().close();
    }

}
