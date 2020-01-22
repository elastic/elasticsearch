/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class CacheBufferedIndexInputTests extends ESIndexInputTestCase {

    private Directory directory;
    private CacheService cacheService;
    private CacheDirectory cacheDirectory;

    @Before
    public void setUpCache() throws IOException {
        final Settings.Builder cacheSettings = Settings.builder();
        if (randomBoolean()) {
            cacheSettings.put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(),
                new ByteSizeValue(randomIntBetween(1, 100), randomFrom(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB)));
        }
        cacheService = new CacheService(cacheSettings.build());
        cacheService.start();

        final Path tmpDir = createTempDir();
        directory = FSDirectory.open(tmpDir.resolve("source"));
        cacheDirectory = new CacheDirectory(directory, cacheService, tmpDir.resolve("cache"));
    }

    @After
    public void tearDownCache() throws IOException {
        cacheDirectory.close();
        cacheService.close();
    }

    private IndexInput createIndexInput(final byte[] input) throws IOException {
        final String fileName = randomAlphaOfLength(10);
        final IOContext context = newIOContext(random());
        try (IndexOutput indexOutput = directory.createOutput(fileName, context)) {
            indexOutput.writeBytes(input, input.length);
        }
        return cacheDirectory.openInput(fileName, context);
    }

    public void testRandomReads() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            try (IndexInput indexInput = createIndexInput(input)) {
                assertEquals(input.length, indexInput.length());
                assertEquals(0, indexInput.getFilePointer());
                byte[] output = randomReadAndSlice(indexInput, input.length);
                assertArrayEquals(input, output);
            }
        }
    }
}
