/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DiskCursorStorageServiceTests extends ESTestCase {

    private DiskCursorStorageService service;
    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path dataPath = createTempDir();
        blockFactory = TestBlockFactory.getNonBreakingInstance();
        service = new DiskCursorStorageService(dataPath, new NamedWriteableRegistry(List.of()), blockFactory, Runnable::run);
    }

    public void testStoreAndGetMetadata() throws Exception {
        String cursorId = "test-cursor-1";
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("val", "integer", null));
        long expiration = System.currentTimeMillis() + 60_000;

        PlainActionFuture<String> storeFuture = new PlainActionFuture<>();
        service.storeMetadataIncomplete(
            cursorId,
            columns,
            expiration,
            ZoneOffset.UTC,
            false,
            new TaskId("node1", 1),
            Map.of("_security_header", "test"),
            storeFuture
        );
        assertEquals(cursorId, storeFuture.get(5, TimeUnit.SECONDS));

        PlainActionFuture<CursorStorageService.CursorMetadata> getFuture = new PlainActionFuture<>();
        service.getMetadata(cursorId, getFuture);
        CursorStorageService.CursorMetadata metadata = getFuture.get(5, TimeUnit.SECONDS);

        assertEquals(1, metadata.columns().size());
        assertEquals("val", metadata.columns().get(0).name());
        assertTrue(metadata.isInProgress());
        assertEquals(expiration, metadata.expirationTimeMillis());
        assertEquals(ZoneOffset.UTC, metadata.zoneId());
        assertEquals(false, metadata.columnar());
        assertNotNull(metadata.taskId());
        assertEquals("test", metadata.headers().get("_security_header"));
    }

    public void testStoreAndGetPages() throws Exception {
        String cursorId = "test-cursor-2";
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("val", "integer", null));

        PlainActionFuture<String> storeFuture = new PlainActionFuture<>();
        service.storeMetadataIncomplete(cursorId, columns, Long.MAX_VALUE, ZoneOffset.UTC, false, null, Map.of(), storeFuture);
        storeFuture.get(5, TimeUnit.SECONDS);

        Page page0 = new Page(3, blockFactory.newIntArrayVector(new int[] { 1, 2, 3 }, 3).asBlock());
        Page page1 = new Page(2, blockFactory.newIntArrayVector(new int[] { 4, 5 }, 2).asBlock());

        PlainActionFuture<Void> sp0 = new PlainActionFuture<>();
        service.storePage(cursorId, 0, List.of(page0), false, sp0);
        sp0.get(5, TimeUnit.SECONDS);

        PlainActionFuture<Void> sp1 = new PlainActionFuture<>();
        service.storePage(cursorId, 1, List.of(page1), false, sp1);
        sp1.get(5, TimeUnit.SECONDS);

        PlainActionFuture<Void> completeFuture = new PlainActionFuture<>();
        service.updateMetadataComplete(cursorId, 5, 2, completeFuture);
        completeFuture.get(5, TimeUnit.SECONDS);

        PlainActionFuture<CursorStorageService.CursorMetadata> metaFuture = new PlainActionFuture<>();
        service.getMetadata(cursorId, metaFuture);
        CursorStorageService.CursorMetadata metadata = metaFuture.get(5, TimeUnit.SECONDS);
        assertTrue(metadata.isComplete());
        assertEquals(5, metadata.totalRows());
        assertEquals(2, metadata.totalPages());

        PlainActionFuture<List<Page>> gp0 = new PlainActionFuture<>();
        service.getPage(cursorId, 0, gp0);
        List<Page> pages0 = gp0.get(5, TimeUnit.SECONDS);
        assertEquals(1, pages0.size());
        assertEquals(3, pages0.get(0).getPositionCount());
        IntBlock block0 = pages0.get(0).getBlock(0);
        assertEquals(1, block0.getInt(0));
        assertEquals(2, block0.getInt(1));
        assertEquals(3, block0.getInt(2));

        PlainActionFuture<List<Page>> gp1 = new PlainActionFuture<>();
        service.getPage(cursorId, 1, gp1);
        List<Page> pages1 = gp1.get(5, TimeUnit.SECONDS);
        assertEquals(1, pages1.size());
        assertEquals(2, pages1.get(0).getPositionCount());
    }

    public void testDelete() throws Exception {
        String cursorId = "test-cursor-3";
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("val", "integer", null));

        PlainActionFuture<String> storeFuture = new PlainActionFuture<>();
        service.storeMetadataIncomplete(cursorId, columns, Long.MAX_VALUE, ZoneOffset.UTC, false, null, Map.of(), storeFuture);
        storeFuture.get(5, TimeUnit.SECONDS);

        assertTrue(Files.exists(service.basePath().resolve(cursorId)));

        PlainActionFuture<Boolean> deleteFuture = new PlainActionFuture<>();
        service.delete(cursorId, deleteFuture);
        assertTrue(deleteFuture.get(5, TimeUnit.SECONDS));

        assertFalse(Files.exists(service.basePath().resolve(cursorId)));

        PlainActionFuture<Boolean> deleteFuture2 = new PlainActionFuture<>();
        service.delete(cursorId, deleteFuture2);
        assertFalse(deleteFuture2.get(5, TimeUnit.SECONDS));
    }

    public void testExpiredMetadataReturnsNotFound() throws Exception {
        String cursorId = "test-cursor-4";
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("val", "integer", null));
        long expiredTime = System.currentTimeMillis() - 1000;

        PlainActionFuture<String> storeFuture = new PlainActionFuture<>();
        service.storeMetadataIncomplete(cursorId, columns, expiredTime, ZoneOffset.UTC, false, null, Map.of(), storeFuture);
        storeFuture.get(5, TimeUnit.SECONDS);

        PlainActionFuture<CursorStorageService.CursorMetadata> getFuture = new PlainActionFuture<>();
        service.getMetadata(cursorId, getFuture);
        expectThrows(Exception.class, () -> getFuture.get(5, TimeUnit.SECONDS));
    }

    public void testUpdateMetadataFailed() throws Exception {
        String cursorId = "test-cursor-5";
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("val", "integer", null));

        PlainActionFuture<String> storeFuture = new PlainActionFuture<>();
        service.storeMetadataIncomplete(cursorId, columns, Long.MAX_VALUE, ZoneOffset.UTC, false, null, Map.of(), storeFuture);
        storeFuture.get(5, TimeUnit.SECONDS);

        PlainActionFuture<Void> failFuture = new PlainActionFuture<>();
        service.updateMetadataFailed(cursorId, failFuture);
        failFuture.get(5, TimeUnit.SECONDS);

        PlainActionFuture<CursorStorageService.CursorMetadata> metaFuture = new PlainActionFuture<>();
        service.getMetadata(cursorId, metaFuture);
        CursorStorageService.CursorMetadata metadata = metaFuture.get(5, TimeUnit.SECONDS);
        assertTrue(metadata.isFailed());
    }

    public void testGetNonExistentCursorReturnsNotFound() throws Exception {
        PlainActionFuture<CursorStorageService.CursorMetadata> getFuture = new PlainActionFuture<>();
        service.getMetadata("nonexistent", getFuture);
        expectThrows(Exception.class, () -> getFuture.get(5, TimeUnit.SECONDS));
    }

    public void testGetNonExistentPageReturnsNotFound() throws Exception {
        String cursorId = "test-cursor-6";
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("val", "integer", null));

        PlainActionFuture<String> storeFuture = new PlainActionFuture<>();
        service.storeMetadataIncomplete(cursorId, columns, Long.MAX_VALUE, ZoneOffset.UTC, false, null, Map.of(), storeFuture);
        storeFuture.get(5, TimeUnit.SECONDS);

        PlainActionFuture<List<Page>> getFuture = new PlainActionFuture<>();
        service.getPage(cursorId, 0, getFuture);
        expectThrows(Exception.class, () -> getFuture.get(5, TimeUnit.SECONDS));
    }
}
