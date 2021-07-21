/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.fs.quotaaware;

import org.apache.lucene.mockfile.MockFileSystemTestCase;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

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
                if (path.getFileName().toString().startsWith("extra") == false) {
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
