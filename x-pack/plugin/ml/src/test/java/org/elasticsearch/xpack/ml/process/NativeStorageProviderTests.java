/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class NativeStorageProviderTests extends ESTestCase {

    public void testTmpStorage() throws IOException {
        Map<Path, Long> storage = new HashMap<>();
        Path tmpDir = createTempDir();

        storage.put(tmpDir, new ByteSizeValue(6, ByteSizeUnit.GB).getBytes());
        NativeStorageProvider storageProvider = createNativeStorageProvider(storage);

        Assert.assertNotNull(
                storageProvider.tryGetLocalTmpStorage(randomAlphaOfLengthBetween(4, 10), new ByteSizeValue(100, ByteSizeUnit.BYTES)));
        Assert.assertNull(storageProvider.tryGetLocalTmpStorage(randomAlphaOfLengthBetween(4, 10),
                new ByteSizeValue(1024 * 1024 * 1024 + 1, ByteSizeUnit.BYTES)));

        String id = randomAlphaOfLengthBetween(4, 10);
        Path path = storageProvider.tryGetLocalTmpStorage(id, new ByteSizeValue(1, ByteSizeUnit.GB));
        Assert.assertNotNull(path);

        Assert.assertEquals(tmpDir.resolve("ml-local-data").resolve("tmp").resolve(id).toString(), path.toString());
    }

    public void testTmpStorageChooseDisk() throws IOException {
        Map<Path, Long> storage = new HashMap<>();
        Path tmpDir = createTempDir();

        // low disk space
        Path disk1 = tmpDir.resolve(randomAlphaOfLengthBetween(4, 10));
        storage.put(disk1, new ByteSizeValue(1, ByteSizeUnit.GB).getBytes());

        // sufficient disk space
        Path disk2 = tmpDir.resolve(randomAlphaOfLengthBetween(4, 10));
        storage.put(disk2, new ByteSizeValue(20, ByteSizeUnit.GB).getBytes());

        NativeStorageProvider storageProvider = createNativeStorageProvider(storage);

        String id = randomAlphaOfLengthBetween(4, 10);
        Path path = storageProvider.tryGetLocalTmpStorage(id, new ByteSizeValue(1, ByteSizeUnit.GB));
        Assert.assertNotNull(path);

        // should resolve to disk2 as disk1 is low on space
        Assert.assertEquals(disk2.resolve("ml-local-data").resolve("tmp").resolve(id).toString(), path.toString());
    }

    public void testTmpStorageCleanup() throws IOException {
        Map<Path, Long> storage = new HashMap<>();
        Path tmpDir = createTempDir();
        storage.put(tmpDir, new ByteSizeValue(6, ByteSizeUnit.GB).getBytes());
        NativeStorageProvider storageProvider = createNativeStorageProvider(storage);
        String id = randomAlphaOfLengthBetween(4, 10);

        Path path = storageProvider.tryGetLocalTmpStorage(id, new ByteSizeValue(1, ByteSizeUnit.KB));

        Assert.assertTrue(Files.exists(path));
        Path testFile = PathUtils.get(path.toString(), "testFile");
        BufferedWriter writer = Files.newBufferedWriter(testFile, StandardCharsets.UTF_8);
        writer.write("created by NativeStorageProviderTests::testTmpStorageDelete");

        writer.close();
        Assert.assertTrue(Files.exists(testFile));
        Assert.assertTrue(Files.isRegularFile(testFile));

        // the native component should cleanup itself, but assume it has crashed
        storageProvider.cleanupLocalTmpStorage(id);
        Assert.assertFalse(Files.exists(testFile));
        Assert.assertFalse(Files.exists(path));
    }

    public void testTmpStorageCleanupOnStart() throws IOException {
        Map<Path, Long> storage = new HashMap<>();
        Path tmpDir = createTempDir();
        storage.put(tmpDir, new ByteSizeValue(6, ByteSizeUnit.GB).getBytes());
        NativeStorageProvider storageProvider = createNativeStorageProvider(storage);
        String id = randomAlphaOfLengthBetween(4, 10);

        Path path = storageProvider.tryGetLocalTmpStorage(id, new ByteSizeValue(1, ByteSizeUnit.KB));

        Assert.assertTrue(Files.exists(path));
        Path testFile = PathUtils.get(path.toString(), "testFile");

        BufferedWriter writer = Files.newBufferedWriter(testFile, StandardCharsets.UTF_8);
        writer.write("created by NativeStorageProviderTests::testTmpStorageWipe");

        writer.close();
        Assert.assertTrue(Files.exists(testFile));
        Assert.assertTrue(Files.isRegularFile(testFile));

        // create a new storage provider to test the case of a crashed node
        storageProvider = createNativeStorageProvider(storage);
        storageProvider.cleanupLocalTmpStorageInCaseOfUncleanShutdown();
        Assert.assertFalse(Files.exists(testFile));
        Assert.assertFalse(Files.exists(path));
    }

    private NativeStorageProvider createNativeStorageProvider(Map<Path, Long> paths) throws IOException {
        Environment environment = mock(Environment.class);

        when(environment.dataFiles()).thenReturn(paths.keySet().toArray(new Path[paths.size()]));
        NativeStorageProvider storageProvider = spy(new NativeStorageProvider(environment, new ByteSizeValue(5, ByteSizeUnit.GB)));

        doAnswer(invocation -> {
            return paths.getOrDefault(invocation.getArguments()[0], Long.valueOf(0)).longValue();
        }

        ).when(storageProvider).getUsableSpace(any(Path.class));

        return storageProvider;
    }

}
