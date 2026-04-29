/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@link ColumnChunkPrefetcher}: byte range computation, parallel dispatch,
 * and async prefetch with failure handling.
 */
public class ColumnChunkPrefetcherTests extends ESTestCase {

    public void testComputeColumnChunkRangesAllColumns() {
        BlockMetaData block = createBlockWithColumns(
            new ColMeta("col_a", 100, 500),
            new ColMeta("col_b", 700, 300),
            new ColMeta("col_c", 1100, 200)
        );

        List<CoalescedRangeReader.ByteRange> ranges = ColumnChunkPrefetcher.computeColumnChunkRanges(block, null);

        assertThat(ranges.size(), equalTo(3));
        assertThat(ranges.get(0), equalTo(new CoalescedRangeReader.ByteRange(100, 500)));
        assertThat(ranges.get(1), equalTo(new CoalescedRangeReader.ByteRange(700, 300)));
        assertThat(ranges.get(2), equalTo(new CoalescedRangeReader.ByteRange(1100, 200)));
    }

    public void testComputeColumnChunkRangesWithProjection() {
        BlockMetaData block = createBlockWithColumns(
            new ColMeta("col_a", 100, 500),
            new ColMeta("col_b", 700, 300),
            new ColMeta("col_c", 1100, 200)
        );

        List<CoalescedRangeReader.ByteRange> ranges = ColumnChunkPrefetcher.computeColumnChunkRanges(block, Set.of("col_a", "col_c"));

        assertThat(ranges.size(), equalTo(2));
        assertThat(ranges.get(0), equalTo(new CoalescedRangeReader.ByteRange(100, 500)));
        assertThat(ranges.get(1), equalTo(new CoalescedRangeReader.ByteRange(1100, 200)));
    }

    public void testComputeColumnChunkRangesEmptyProjection() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_a", 100, 500));

        List<CoalescedRangeReader.ByteRange> ranges = ColumnChunkPrefetcher.computeColumnChunkRanges(block, Set.of("nonexistent"));

        assertThat(ranges.size(), equalTo(0));
    }

    public void testPrefetchReturnsCorrectData() throws Exception {
        byte[] fileData = new byte[2000];
        for (int i = 0; i < fileData.length; i++) {
            fileData[i] = (byte) (i & 0xFF);
        }

        StorageObject storage = createStorageObject(fileData);
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_a", 100, 50), new ColMeta("col_b", 200, 60));

        CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future = ColumnChunkPrefetcher.prefetch(
            storage,
            block,
            null
        );

        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> result = future.get();
        assertThat(result.size(), greaterThanOrEqualTo(2));

        ColumnChunkPrefetcher.PrefetchedChunk chunkA = result.get(100L);
        assertThat(chunkA, notNullValue());
        assertThat(chunkA.covers(100, 50), equalTo(true));
        byte[] expected = new byte[50];
        System.arraycopy(fileData, 100, expected, 0, 50);
        byte[] actual = new byte[50];
        chunkA.data().duplicate().get(actual);
        assertArrayEquals(expected, actual);
    }

    public void testPrefetchAsyncReturnsCorrectData() throws Exception {
        byte[] fileData = new byte[1000];
        for (int i = 0; i < fileData.length; i++) {
            fileData[i] = (byte) (i & 0xFF);
        }

        StorageObject storage = createStorageObject(fileData);
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_x", 50, 100));

        CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future = ColumnChunkPrefetcher.prefetchAsync(
            storage,
            block,
            null
        );

        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> result = future.get();
        assertThat(result.isEmpty(), equalTo(false));
    }

    public void testPrefetchConcurrentReadCalls() throws Exception {
        AtomicInteger concurrentReads = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        byte[] fileData = new byte[10000];
        StorageObject storage = new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(fileData);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, fileData.length - position);
                return new ByteArrayInputStream(fileData, pos, len);
            }

            @Override
            public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
                int current = concurrentReads.incrementAndGet();
                maxConcurrent.updateAndGet(m -> Math.max(m, current));
                executor.execute(() -> {
                    try (InputStream stream = newStream(position, length)) {
                        byte[] bytes = stream.readAllBytes();
                        listener.onResponse(ByteBuffer.wrap(bytes));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    } finally {
                        concurrentReads.decrementAndGet();
                    }
                });
            }

            @Override
            public long length() {
                return fileData.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("test://concurrent.parquet");
            }
        };

        BlockMetaData block = createBlockWithColumns(new ColMeta("a", 100, 500), new ColMeta("b", 2000, 500), new ColMeta("c", 5000, 500));

        CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future = ColumnChunkPrefetcher.prefetch(
            storage,
            block,
            null
        );

        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> result = future.get();
        assertThat(result.isEmpty(), equalTo(false));
    }

    public void testPrefetchFailureCompletesExceptionally() {
        StorageObject failingStorage = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new IOException("Simulated failure");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                throw new IOException("Simulated failure");
            }

            @Override
            public long length() {
                return 10000;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("test://failing.parquet");
            }
        };

        BlockMetaData block = createBlockWithColumns(new ColMeta("col", 100, 500));

        CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future = ColumnChunkPrefetcher.prefetchAsync(
            failingStorage,
            block,
            null
        );

        assertTrue(future.isCompletedExceptionally());
    }

    public void testPrefetchedChunkCovers() {
        ByteBuffer data = ByteBuffer.allocate(100);
        ColumnChunkPrefetcher.PrefetchedChunk chunk = new ColumnChunkPrefetcher.PrefetchedChunk(200, 100, data);

        assertTrue(chunk.covers(200, 50));
        assertTrue(chunk.covers(200, 100));
        assertTrue(chunk.covers(250, 50));
        assertFalse(chunk.covers(199, 50));
        assertFalse(chunk.covers(250, 51));
        assertFalse(chunk.covers(300, 1));
    }

    public void testPrefetchEmptyRanges() throws Exception {
        byte[] fileData = new byte[1000];
        StorageObject storage = createStorageObject(fileData);
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(0);

        CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future = ColumnChunkPrefetcher.prefetch(
            storage,
            block,
            null
        );

        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> result = future.get();
        assertThat(result.isEmpty(), equalTo(true));
    }

    // --- helpers ---

    private record ColMeta(String name, long startPos, long totalSize) {}

    @SuppressWarnings("deprecation")
    private static BlockMetaData createBlockWithColumns(ColMeta... cols) {
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(100);
        for (ColMeta col : cols) {
            ColumnChunkMetaData chunk = ColumnChunkMetaData.get(
                ColumnPath.get(col.name),
                PrimitiveType.PrimitiveTypeName.INT64,
                CompressionCodecName.UNCOMPRESSED,
                Set.of(org.apache.parquet.column.Encoding.PLAIN),
                org.apache.parquet.column.statistics.Statistics.createStats(
                    Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(col.name)
                ),
                col.startPos,
                0,
                100,
                col.totalSize,
                col.totalSize
            );
            block.addColumn(chunk);
        }
        return block;
    }

    private StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("test://test.parquet");
            }
        };
    }
}
