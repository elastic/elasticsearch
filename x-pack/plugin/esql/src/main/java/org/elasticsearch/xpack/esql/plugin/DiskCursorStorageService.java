/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Stores and retrieves paginated ES|QL cursor state on the local filesystem.
 * <p>
 * Each cursor is stored as a directory containing three files:
 * <ul>
 *   <li>{@code meta.bin} -- cursor metadata (columns, expiration, state, headers)</li>
 *   <li>{@code data.bin} -- append-only concatenated compressed page blobs</li>
 *   <li>{@code pages.idx} -- append-only page index; each entry is {@code [offset:long, length:int]} (12 bytes)</li>
 * </ul>
 */
public class DiskCursorStorageService implements CursorStorageService {

    private static final Logger logger = LogManager.getLogger(DiskCursorStorageService.class);

    static final String CURSORS_DIR = "esql-cursors";
    private static final String META_FILE = "meta.bin";
    private static final String DATA_FILE = "data.bin";
    private static final String PAGES_IDX_FILE = "pages.idx";
    private static final int PAGE_INDEX_ENTRY_SIZE = 12; // long offset (8) + int length (4)

    private final Path basePath;
    private final NamedWriteableRegistry registry;
    private final BlockFactory blockFactory;
    private final Executor executor;
    private final ConcurrentHashMap<String, CursorWriter> cursorWriters = new ConcurrentHashMap<>();

    /**
     * Per-cursor write state that buffers serialized pages and flushes them to disk
     * in sequential page-index order. Pages may arrive out of order from concurrent
     * executor threads; this ensures both {@code data.bin} and {@code pages.idx}
     * receive strictly ordered appends.
     */
    private static class CursorWriter {
        final Object lock = new Object();
        int nextFlushIndex = 0;
        final TreeMap<Integer, byte[]> pendingData = new TreeMap<>();
        final TreeMap<Integer, ActionListener<Void>> pendingListeners = new TreeMap<>();
    }

    public DiskCursorStorageService(Path dataPath, NamedWriteableRegistry registry, BlockFactory blockFactory, Executor executor) {
        this.basePath = dataPath.resolve(CURSORS_DIR);
        this.registry = registry;
        this.blockFactory = blockFactory;
        this.executor = executor;
    }

    /**
     * Returns the base directory where all cursor subdirectories are stored.
     */
    public Path basePath() {
        return basePath;
    }

    // ---- Write path ----

    @Override
    public void storeMetadataIncomplete(
        String cursorId,
        List<ColumnInfoImpl> columns,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        TaskId taskId,
        Map<String, String> securityHeaders,
        ActionListener<String> listener
    ) {
        executor.execute(() -> {
            try {
                Path cursorDir = cursorDir(cursorId);
                Files.createDirectories(cursorDir);
                writeMetaFile(
                    cursorDir,
                    columns,
                    TOTAL_PAGES_IN_PROGRESS,
                    TOTAL_PAGES_IN_PROGRESS,
                    expirationMillis,
                    zoneId,
                    columnar,
                    taskId,
                    securityHeaders
                );
                listener.onResponse(cursorId);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void storePage(String cursorId, int pageIndex, List<Page> pageData, boolean needsDeepCopy, ActionListener<Void> listener) {
        try {
            List<Page> toStore;
            if (needsDeepCopy) {
                toStore = deepCopyPages(pageData);
            } else {
                toStore = pageData;
            }
            byte[] serialized;
            try {
                serialized = serializePages(toStore);
            } finally {
                if (needsDeepCopy) {
                    toStore.forEach(Page::releaseBlocks);
                }
            }
            executor.execute(() -> {
                try {
                    Path cursorDir = cursorDir(cursorId);
                    Path dataFile = cursorDir.resolve(DATA_FILE);
                    Path idxFile = cursorDir.resolve(PAGES_IDX_FILE);

                    CursorWriter writer = cursorWriters.computeIfAbsent(cursorId, k -> new CursorWriter());
                    List<ActionListener<Void>> flushedListeners;
                    synchronized (writer.lock) {
                        writer.pendingData.put(pageIndex, serialized);
                        writer.pendingListeners.put(pageIndex, listener);
                        flushedListeners = flushReady(writer, dataFile, idxFile);
                    }
                    for (ActionListener<Void> flushed : flushedListeners) {
                        flushed.onResponse(null);
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Flushes all consecutive pages starting from {@code writer.nextFlushIndex} to disk
     * and returns the listeners for the flushed pages. Must be called while holding {@code writer.lock}.
     */
    private List<ActionListener<Void>> flushReady(CursorWriter writer, Path dataFile, Path idxFile) throws IOException {
        List<ActionListener<Void>> flushedListeners = new ArrayList<>();
        byte[] data;
        while ((data = writer.pendingData.remove(writer.nextFlushIndex)) != null) {
            long offset;
            try (
                FileChannel dataCh = FileChannel.open(
                    dataFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
                )
            ) {
                offset = dataCh.position();
                dataCh.write(ByteBuffer.wrap(data));
            }

            ByteBuffer idxEntry = ByteBuffer.allocate(PAGE_INDEX_ENTRY_SIZE);
            idxEntry.putLong(offset);
            idxEntry.putInt(data.length);
            idxEntry.flip();
            try (
                FileChannel idxCh = FileChannel.open(
                    idxFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
                )
            ) {
                idxCh.write(idxEntry);
            }
            flushedListeners.add(writer.pendingListeners.remove(writer.nextFlushIndex));
            writer.nextFlushIndex++;
        }
        return flushedListeners;
    }

    @Override
    public void updateMetadataComplete(String cursorId, int totalRows, int totalPages, ActionListener<Void> listener) {
        executor.execute(() -> {
            try {
                Path cursorDir = cursorDir(cursorId);
                CursorMetadata existing = readMetaFile(cursorDir);
                writeMetaFileAtomic(
                    cursorDir,
                    existing.columns(),
                    totalRows,
                    totalPages,
                    existing.expirationTimeMillis(),
                    existing.zoneId(),
                    existing.columnar(),
                    existing.taskId(),
                    existing.headers()
                );
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void updateMetadataFailed(String cursorId, ActionListener<Void> listener) {
        executor.execute(() -> {
            try {
                Path cursorDir = cursorDir(cursorId);
                CursorMetadata existing = readMetaFile(cursorDir);
                writeMetaFileAtomic(
                    cursorDir,
                    existing.columns(),
                    existing.totalRows(),
                    TOTAL_PAGES_FAILED,
                    existing.expirationTimeMillis(),
                    existing.zoneId(),
                    existing.columnar(),
                    existing.taskId(),
                    existing.headers()
                );
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    // ---- Read path ----

    @Override
    public void getMetadata(String cursorId, ActionListener<CursorMetadata> listener) {
        executor.execute(() -> {
            try {
                Path cursorDir = cursorDir(cursorId);
                if (Files.exists(cursorDir.resolve(META_FILE)) == false) {
                    listener.onFailure(new ResourceNotFoundException("cursor not found or expired"));
                    return;
                }
                CursorMetadata metadata = readMetaFile(cursorDir);
                if (metadata.expirationTimeMillis() > 0 && System.currentTimeMillis() >= metadata.expirationTimeMillis()) {
                    listener.onFailure(new ResourceNotFoundException("cursor not found or expired"));
                    return;
                }
                listener.onResponse(metadata);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void getPage(String cursorId, int pageIndex, ActionListener<List<Page>> listener) {
        executor.execute(() -> {
            try {
                Path cursorDir = cursorDir(cursorId);
                Path idxFile = cursorDir.resolve(PAGES_IDX_FILE);
                Path dataFile = cursorDir.resolve(DATA_FILE);

                if (Files.exists(idxFile) == false) {
                    listener.onFailure(new ResourceNotFoundException("cursor page not found"));
                    return;
                }

                long idxOffset = (long) pageIndex * PAGE_INDEX_ENTRY_SIZE;
                ByteBuffer idxEntry = ByteBuffer.allocate(PAGE_INDEX_ENTRY_SIZE);
                try (FileChannel idxCh = FileChannel.open(idxFile, StandardOpenOption.READ)) {
                    if (idxOffset + PAGE_INDEX_ENTRY_SIZE > idxCh.size()) {
                        listener.onFailure(new ResourceNotFoundException("cursor page not found"));
                        return;
                    }
                    idxCh.read(idxEntry, idxOffset);
                }
                idxEntry.flip();
                long dataOffset = idxEntry.getLong();
                int dataLength = idxEntry.getInt();

                ByteBuffer dataBuf = ByteBuffer.allocate(dataLength);
                try (FileChannel dataCh = FileChannel.open(dataFile, StandardOpenOption.READ)) {
                    dataCh.read(dataBuf, dataOffset);
                }
                dataBuf.flip();

                List<Page> pages = deserializePages(dataBuf);
                listener.onResponse(pages);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void updateKeepAlive(String cursorId, TimeValue keepAlive, ActionListener<Void> listener) {
        executor.execute(() -> {
            try {
                Path cursorDir = cursorDir(cursorId);
                CursorMetadata existing = readMetaFile(cursorDir);
                long newExpiration = System.currentTimeMillis() + keepAlive.getMillis();
                writeMetaFileAtomic(
                    cursorDir,
                    existing.columns(),
                    existing.totalRows(),
                    existing.totalPages(),
                    newExpiration,
                    existing.zoneId(),
                    existing.columnar(),
                    existing.taskId(),
                    existing.headers()
                );
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    // ---- Delete path ----

    @Override
    public void delete(String cursorId, ActionListener<Boolean> listener) {
        executor.execute(() -> {
            try {
                Path cursorDir = cursorDir(cursorId);
                if (Files.exists(cursorDir) == false) {
                    listener.onResponse(false);
                    return;
                }
                IOUtils.rm(cursorDir);
                cursorWriters.remove(cursorId);
                listener.onResponse(true);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    // ---- Metadata serialization ----

    private void writeMetaFile(
        Path cursorDir,
        List<ColumnInfoImpl> columns,
        int totalRows,
        int totalPages,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        @Nullable TaskId taskId,
        Map<String, String> securityHeaders
    ) throws IOException {
        byte[] data = serializeMetadata(columns, totalRows, totalPages, expirationMillis, zoneId, columnar, taskId, securityHeaders);
        Files.write(cursorDir.resolve(META_FILE), data);
    }

    private void writeMetaFileAtomic(
        Path cursorDir,
        List<ColumnInfoImpl> columns,
        int totalRows,
        int totalPages,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        @Nullable TaskId taskId,
        Map<String, String> securityHeaders
    ) throws IOException {
        byte[] data = serializeMetadata(columns, totalRows, totalPages, expirationMillis, zoneId, columnar, taskId, securityHeaders);
        Path tmp = cursorDir.resolve(META_FILE + ".tmp");
        Files.write(tmp, data);
        Files.move(tmp, cursorDir.resolve(META_FILE), java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    private byte[] serializeMetadata(
        List<ColumnInfoImpl> columns,
        int totalRows,
        int totalPages,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        @Nullable TaskId taskId,
        Map<String, String> securityHeaders
    ) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeInt(totalPages);
            out.writeInt(totalRows);
            out.writeLong(expirationMillis);
            out.writeString(zoneId.getId());
            out.writeBoolean(columnar);
            out.writeBoolean(taskId != null);
            if (taskId != null) {
                out.writeString(taskId.toString());
            }
            out.writeMap(securityHeaders, StreamOutput::writeString);
            try (var compressedOut = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(Streams.noCloseStream(out))) {
                compressedOut.writeCollection(columns);
            }
            return BytesReference.toBytes(out.bytes());
        }
    }

    CursorMetadata readMetaFile(Path cursorDir) throws IOException {
        byte[] data = Files.readAllBytes(cursorDir.resolve(META_FILE));
        try (StreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(data))) {
            int totalPages = in.readInt();
            int totalRows = in.readInt();
            long expirationMillis = in.readLong();
            String zoneIdStr = in.readString();
            boolean columnar = in.readBoolean();
            TaskId taskId = null;
            if (in.readBoolean()) {
                taskId = new TaskId(in.readString());
            }
            Map<String, String> headers = in.readMap(StreamInput::readString);

            InputStream remaining = new ByteArrayInputStream(data, data.length - in.available(), in.available());
            StreamInput decompressed = CompressorFactory.COMPRESSOR.threadLocalStreamInput(remaining);
            try (var namedIn = new NamedWriteableAwareStreamInput(decompressed, registry)) {
                List<ColumnInfoImpl> columns = namedIn.readCollectionAsList(ColumnInfoImpl::new);
                ZoneId zoneId = zoneIdStr != null ? ZoneId.of(zoneIdStr) : ZoneOffset.UTC;
                return new CursorMetadata(columns, totalRows, totalPages, expirationMillis, zoneId, columnar, taskId, headers);
            }
        }
    }

    // ---- Page serialization ----

    private byte[] serializePages(List<Page> pages) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            try (var compressedOut = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(Streams.noCloseStream(out))) {
                compressedOut.writeCollection(pages);
            }
            return BytesReference.toBytes(out.bytes());
        }
    }

    private List<Page> deserializePages(ByteBuffer data) throws IOException {
        InputStream raw = new ByteArrayInputStream(data.array(), data.arrayOffset() + data.position(), data.remaining());
        StreamInput decompressed = CompressorFactory.COMPRESSOR.threadLocalStreamInput(raw);
        try (BlockStreamInput bsi = new BlockStreamInput(new NamedWriteableAwareStreamInput(decompressed, registry), blockFactory)) {
            return bsi.readCollectionAsList(Page::new);
        }
    }

    private List<Page> deepCopyPages(List<Page> pageData) throws IOException {
        List<Page> result = new ArrayList<>(pageData.size());
        try {
            for (Page page : pageData) {
                Block[] copiedBlocks = new Block[page.getBlockCount()];
                try {
                    for (int b = 0; b < page.getBlockCount(); b++) {
                        copiedBlocks[b] = page.getBlock(b).deepCopy(blockFactory);
                    }
                    result.add(
                        page.batchMetadata() != null
                            ? new Page(page.batchMetadata(), copiedBlocks)
                            : new Page(page.getPositionCount(), copiedBlocks)
                    );
                } catch (Exception e) {
                    Releasables.closeExpectNoException(copiedBlocks);
                    throw e;
                }
            }
            return result;
        } catch (Exception e) {
            result.forEach(Page::releaseBlocks);
            throw e;
        }
    }

    private Path cursorDir(String cursorId) {
        return basePath.resolve(cursorId);
    }
}
