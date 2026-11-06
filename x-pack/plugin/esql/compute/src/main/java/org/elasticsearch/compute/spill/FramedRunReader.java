/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.compute.spill.spi.SpillObject;
import org.elasticsearch.compute.spill.spi.SpillReader;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/**
 * Reads one framed run back, record by record and partition by partition. Opening validates the header and the footer,
 * and loads the directory; the index entries of a partition are loaded in blocks behind a bounded cache, and the body
 * is read in windows of whole chunks, each chunk's CRC verified before any byte of it is handed out. A record is
 * copied out of its chunks into a buffer charged to the breaker before the copy is made, with its length checked
 * against the maxima first, so a corrupt length fails before it allocates.
 */
public final class FramedRunReader implements Releasable {

    private static final int INDEX_BLOCK_ENTRIES = 4096;
    private static final int INDEX_CACHE_BLOCKS = 4;

    private final String name;
    private final SpillObject object;
    private final CircuitBreaker breaker;
    private final FramedRunMaxima maxima;
    private final int windowBytes;
    private final FramedRunLayout.Header header;
    private final FramedRunLayout.Footer footer;
    private final long[] partitionFirst;
    private final long[] partitionCount;
    private final long directoryCharge;
    private final List<IndexBlock> indexCache = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private FramedRunReader(
        String name,
        SpillObject object,
        CircuitBreaker breaker,
        FramedRunMaxima maxima,
        int windowBytes,
        FramedRunLayout.Header header,
        FramedRunLayout.Footer footer,
        long[] partitionFirst,
        long[] partitionCount,
        long directoryCharge
    ) {
        this.name = name;
        this.object = object;
        this.breaker = breaker;
        this.maxima = maxima;
        this.windowBytes = windowBytes;
        this.header = header;
        this.footer = footer;
        this.partitionFirst = partitionFirst;
        this.partitionCount = partitionCount;
        this.directoryCharge = directoryCharge;
    }

    /**
     * Opens a run: header, footer and directory are read and validated before the listener gets the reader.
     *
     * @param breaker where the reader's windows, index blocks and record copies are charged; the query's root view
     * @param windowBytes how much of the body one read brings in; at least one chunk with its trailer
     */
    public static void open(
        String name,
        SpillObject object,
        CircuitBreaker breaker,
        FramedRunMaxima maxima,
        int windowBytes,
        ActionListener<FramedRunReader> listener
    ) {
        long size = object.sizeInBytes();
        if (size < FramedRunLayout.HEADER_BYTES + FramedRunLayout.FOOTER_BYTES) {
            listener.onFailure(
                new FramedRunCorruptionException(name, "object of [" + size + "] bytes is too short for a header and a footer")
            );
            return;
        }
        readFully(object, 0, FramedRunLayout.HEADER_BYTES, breaker, listener.delegateFailureAndWrap((l1, headerBytes) -> {
            FramedRunLayout.Header header;
            try (headerBytes) {
                header = FramedRunLayout.Header.decode(BytesReference.toBytes(headerBytes), 0, name);
                if (header.chunkSize() > maxima.chunkBytes()) {
                    throw new FramedRunCorruptionException(
                        name,
                        "chunk size [" + header.chunkSize() + "] exceeds the maximum [" + maxima.chunkBytes() + "]"
                    );
                }
            }
            readFully(
                object,
                size - FramedRunLayout.FOOTER_BYTES,
                FramedRunLayout.FOOTER_BYTES,
                breaker,
                l1.delegateFailureAndWrap((l2, footerBytes) -> {
                    FramedRunLayout.Footer footer;
                    try (footerBytes) {
                        footer = FramedRunLayout.Footer.decode(BytesReference.toBytes(footerBytes), 0, name);
                    }
                    long expectedIndexOffset = FramedRunLayout.HEADER_BYTES + FramedRunLayout.physicalBodyLength(
                        footer.logicalBytes(),
                        header.chunkSize()
                    );
                    long indexBytes = (long) footer.partitions() * FramedRunLayout.DIRECTORY_ENTRY_BYTES + footer.records()
                        * FramedRunLayout.INDEX_ENTRY_BYTES + FramedRunLayout.INDEX_CRC_BYTES;
                    if (footer.indexOffset() != expectedIndexOffset
                        || footer.records() > maxima.indexEntries()
                        || footer.partitions() > maxima.partitions()
                        || footer.indexOffset() + indexBytes != size - FramedRunLayout.FOOTER_BYTES) {
                        throw new FramedRunCorruptionException(name, "footer does not describe an object of [" + size + "] bytes");
                    }
                    long directoryBytes = (long) footer.partitions() * FramedRunLayout.DIRECTORY_ENTRY_BYTES;
                    if (TransportVersion.fromId(header.transportVersionId()).isKnown() == false
                        || header.transportVersionId() > TransportVersion.current().id()) {
                        throw new FramedRunCorruptionException(
                            name,
                            "written with transport version [" + header.transportVersionId() + "], which this node cannot read"
                        );
                    }
                    verifyIndex(
                        name,
                        object,
                        breaker,
                        footer,
                        indexBytes,
                        windowBytes,
                        l2.delegateFailureAndWrap(
                            (lv, verified) -> readDirectory(name, object, breaker, maxima, windowBytes, header, footer, directoryBytes, lv)
                        )
                    );
                })
            );
        }));
    }

    /**
     * Reads and checks the directory, then hands out the reader; the directory stays charged for the reader's life.
     */
    private static void readDirectory(
        String name,
        SpillObject object,
        CircuitBreaker breaker,
        FramedRunMaxima maxima,
        int windowBytes,
        FramedRunLayout.Header header,
        FramedRunLayout.Footer footer,
        long directoryBytes,
        ActionListener<FramedRunReader> listener
    ) {
        readFully(object, footer.indexOffset(), directoryBytes, breaker, listener.delegateFailureAndWrap((l3, directory) -> {
            // the decoded directory is charged before its arrays exist; the raw bytes are released right after decoding
            long directoryCharge = directoryBytes;
            long[] first;
            long[] count;
            boolean charged = false;
            boolean success = false;
            try (directory) {
                breaker.addEstimateBytesAndMaybeBreak(directoryCharge, "spill run directory");
                charged = true;
                first = new long[footer.partitions()];
                count = new long[footer.partitions()];
                byte[] bytes = BytesReference.toBytes(directory);
                long running = 0;
                for (int p = 0; p < footer.partitions(); p++) {
                    first[p] = ByteUtils.readLongLE(bytes, p * FramedRunLayout.DIRECTORY_ENTRY_BYTES);
                    count[p] = ByteUtils.readLongLE(bytes, p * FramedRunLayout.DIRECTORY_ENTRY_BYTES + 8);
                    if (first[p] != running || count[p] < 0) {
                        throw new FramedRunCorruptionException(name, "directory of partition [" + p + "] out of order");
                    }
                    running += count[p];
                }
                if (running != footer.records()) {
                    throw new FramedRunCorruptionException(
                        name,
                        "directory counts [" + running + "] records, footer [" + footer.records() + "]"
                    );
                }
                success = true;
            } finally {
                if (charged && success == false) {
                    breaker.addWithoutBreaking(-directoryCharge);
                }
            }
            int window = Math.max(windowBytes, header.chunkSize() + FramedRunLayout.CHUNK_TRAILER_BYTES);
            l3.onResponse(new FramedRunReader(name, object, breaker, maxima, window, header, footer, first, count, directoryCharge));
        }));
    }

    /**
     * One pass over the index in windows, retaining none of it, to check its CRC before any entry is trusted.
     */
    private static void verifyIndex(
        String name,
        SpillObject object,
        CircuitBreaker breaker,
        FramedRunLayout.Footer footer,
        long indexBytes,
        int windowBytes,
        ActionListener<Void> listener
    ) {
        long covered = indexBytes - FramedRunLayout.INDEX_CRC_BYTES;
        CRC32 crc = new CRC32();
        verifyIndexFrom(name, object, breaker, footer.indexOffset(), covered, Math.max(windowBytes, 1024 * 1024), crc, listener);
    }

    /**
     * Walks the index window by window. A backend that completes inline would otherwise nest one call per window, so
     * the loop continues in place when the read completed before the call returned, and only recurses when it did not.
     */
    private static void verifyIndexFrom(
        String name,
        SpillObject object,
        CircuitBreaker breaker,
        long at,
        long remaining,
        int window,
        CRC32 crc,
        ActionListener<Void> listener
    ) {
        while (remaining > 0) {
            final long position = at;
            final long length = Math.min(window, remaining);
            final long left = remaining - length;
            // one hand-off between the caller and the callback: whoever loses it carries on, so the loop continues
            // when the read completed before the caller checked, and the callback recurses otherwise
            final AtomicBoolean handedOff = new AtomicBoolean();
            readFully(object, position, length, breaker, listener.delegateFailureAndWrap((l, part) -> {
                try (part) {
                    byte[] bytes = BytesReference.toBytes(part);
                    crc.update(bytes, 0, bytes.length);
                }
                if (handedOff.compareAndSet(false, true)) {
                    return;
                }
                verifyIndexFrom(name, object, breaker, position + length, left, window, crc, l);
            }));
            if (handedOff.compareAndSet(false, true)) {
                return;
            }
            at += length;
            remaining = left;
        }
        readFully(object, at, FramedRunLayout.INDEX_CRC_BYTES, breaker, listener.delegateFailureAndWrap((l, stored) -> {
            try (stored) {
                int expected = ByteUtils.readIntLE(BytesReference.toBytes(stored), 0);
                if (expected != (int) crc.getValue()) {
                    throw new FramedRunCorruptionException(name, "index checksum mismatch");
                }
            }
            l.onResponse(null);
        }));
    }

    public FramedRunLayout.Header header() {
        return header;
    }

    public FramedRunLayout.Footer footer() {
        return footer;
    }

    public TransportVersion transportVersion() {
        return TransportVersion.fromId(header.transportVersionId());
    }

    public int partitions() {
        return footer.partitions();
    }

    public long records(int partition) {
        return partition >= 0 && partition < partitionCount.length ? partitionCount[partition] : 0;
    }

    /**
     * Index blocks held by the cache, for tests of its bound.
     */
    int cachedIndexBlocks() {
        synchronized (indexCache) {
            return indexCache.size();
        }
    }

    /**
     * The records of one partition in the order they were written. Several cursors may be open at once.
     */
    public RecordCursor cursor(int partition) {
        if (partition < 0 || partition >= footer.partitions()) {
            return new RecordCursor(0, 0);
        }
        return new RecordCursor(partitionFirst[partition], partitionFirst[partition] + partitionCount[partition]);
    }

    /**
     * One record: its partition, its logical offset in the run and its payload, which the caller closes.
     */
    public record Record(int partition, long offset, ReleasableBytesReference payload) implements Releasable {
        @Override
        public void close() {
            payload.close();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            synchronized (indexCache) {
                Releasables.close(indexCache);
                indexCache.clear();
            }
            breaker.addWithoutBreaking(-directoryCharge);
        }
    }

    // ---- the index, in blocks behind a bounded cache

    private final class IndexBlock implements Releasable {
        final long firstEntry;
        final int[] partitions;
        final long[] offsets;
        final int[] lengths;
        final long charge;

        IndexBlock(long firstEntry, byte[] bytes, int entries, long charge) throws FramedRunCorruptionException {
            this.firstEntry = firstEntry;
            this.partitions = new int[entries];
            this.offsets = new long[entries];
            this.lengths = new int[entries];
            this.charge = charge;
            for (int i = 0; i < entries; i++) {
                int at = i * FramedRunLayout.INDEX_ENTRY_BYTES;
                partitions[i] = ByteUtils.readIntLE(bytes, at);
                offsets[i] = ByteUtils.readLongLE(bytes, at + 4);
                lengths[i] = ByteUtils.readIntLE(bytes, at + 12);
                if (partitions[i] < 0 || offsets[i] < 0 || lengths[i] < 0 || offsets[i] + lengths[i] > footer.logicalBytes()) {
                    throw new FramedRunCorruptionException(name, "index entry [" + (firstEntry + i) + "] out of range");
                }
                if (lengths[i] > maxima.recordBytes()) {
                    throw new FramedRunCorruptionException(
                        name,
                        "index entry [" + (firstEntry + i) + "] of [" + lengths[i] + "] bytes exceeds the maximum"
                    );
                }
            }
        }

        boolean covers(long entry) {
            return entry >= firstEntry && entry < firstEntry + offsets.length;
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-charge);
        }
    }

    private void indexBlock(long entry, ActionListener<IndexBlock> listener) {
        synchronized (indexCache) {
            for (IndexBlock block : indexCache) {
                if (block.covers(entry)) {
                    listener.onResponse(block);
                    return;
                }
            }
        }
        long blockFirst = entry - entry % INDEX_BLOCK_ENTRIES;
        int entries = (int) Math.min(INDEX_BLOCK_ENTRIES, footer.records() - blockFirst);
        long at = footer.indexOffset() + (long) footer.partitions() * FramedRunLayout.DIRECTORY_ENTRY_BYTES + blockFirst
            * FramedRunLayout.INDEX_ENTRY_BYTES;
        long bytes = (long) entries * FramedRunLayout.INDEX_ENTRY_BYTES;
        readFully(object, at, bytes, breaker, listener.delegateFailureAndWrap((l, raw) -> {
            IndexBlock block;
            long charge = (long) entries * (Integer.BYTES + Long.BYTES + Integer.BYTES);
            try (raw) {
                breaker.addEstimateBytesAndMaybeBreak(charge, "spill run index block");
                try {
                    block = new IndexBlock(blockFirst, BytesReference.toBytes(raw), entries, charge);
                } catch (Exception e) {
                    breaker.addWithoutBreaking(-charge);
                    throw e;
                }
            }
            IndexBlock evicted = null;
            IndexBlock result = block;
            synchronized (indexCache) {
                if (closed.get()) {
                    evicted = block;
                } else {
                    // two cursors may have missed the same block; the first to insert wins and the other copy is dropped
                    for (IndexBlock cached : indexCache) {
                        if (cached.covers(entry)) {
                            result = cached;
                            evicted = block;
                            break;
                        }
                    }
                    if (evicted == null) {
                        indexCache.add(block);
                        if (indexCache.size() > INDEX_CACHE_BLOCKS) {
                            evicted = indexCache.remove(0);
                        }
                    }
                }
            }
            Releasables.close(evicted);
            l.onResponse(result);
        }));
    }

    // ---- the body, in windows of verified chunks

    /**
     * Whole chunks read in one go, trailers verified; {@link #copy} extracts a payload range across them.
     */
    private final class Window implements Releasable {
        final long firstChunk;
        final int chunks;
        final byte[] bytes;
        final long charge;

        Window(long firstChunk, int chunks, byte[] bytes, long charge) {
            this.firstChunk = firstChunk;
            this.chunks = chunks;
            this.bytes = bytes;
            this.charge = charge;
        }

        long logicalStart() {
            return firstChunk * header.chunkSize();
        }

        long logicalEnd() {
            return Math.min(footer.logicalBytes(), (firstChunk + chunks) * (long) header.chunkSize());
        }

        /**
         * Copies logical [from, to) of this window into {@code into} at {@code at}; the range must lie inside the window.
         */
        void copy(long from, long to, byte[] into, int at) {
            int chunkSize = header.chunkSize();
            long position = from;
            int target = at;
            while (position < to) {
                long chunk = position / chunkSize;
                int inChunk = (int) (position % chunkSize);
                int n = (int) Math.min(to - position, chunkSize - inChunk);
                int physical = (int) ((chunk - firstChunk) * (chunkSize + FramedRunLayout.CHUNK_TRAILER_BYTES)) + inChunk;
                System.arraycopy(bytes, physical, into, target, n);
                position += n;
                target += n;
            }
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-charge);
        }
    }

    private void window(long firstChunk, ActionListener<Window> listener) {
        int chunkSize = header.chunkSize();
        long chunkCount = (footer.logicalBytes() + chunkSize - 1) / chunkSize;
        int chunksPerWindow = Math.max(1, windowBytes / (chunkSize + FramedRunLayout.CHUNK_TRAILER_BYTES));
        int chunks = (int) Math.min(chunksPerWindow, chunkCount - firstChunk);
        long start = FramedRunLayout.chunkStart(firstChunk, chunkSize);
        long end = Math.min(FramedRunLayout.chunkStart(firstChunk + chunks, chunkSize), footer.indexOffset());
        readFully(object, start, end - start, breaker, listener.delegateFailureAndWrap((l, raw) -> {
            Window window;
            try (raw) {
                byte[] bytes = BytesReference.toBytes(raw);
                long charge = bytes.length;
                breaker.addEstimateBytesAndMaybeBreak(charge, "spill run window");
                try {
                    verify(bytes, firstChunk, chunks, chunkCount);
                } catch (Exception e) {
                    breaker.addWithoutBreaking(-charge);
                    throw e;
                }
                window = new Window(firstChunk, chunks, bytes, charge);
            }
            l.onResponse(window);
        }));
    }

    private void verify(byte[] bytes, long firstChunk, int chunks, long chunkCount) throws FramedRunCorruptionException {
        int chunkSize = header.chunkSize();
        for (int i = 0; i < chunks; i++) {
            long chunk = firstChunk + i;
            int expected = chunk == chunkCount - 1 ? (int) (footer.logicalBytes() - chunk * chunkSize) : chunkSize;
            int at = i * (chunkSize + FramedRunLayout.CHUNK_TRAILER_BYTES);
            if (at + expected + FramedRunLayout.CHUNK_TRAILER_BYTES > bytes.length) {
                throw new FramedRunCorruptionException(name, "chunk [" + chunk + "] is cut short");
            }
            int length = ByteUtils.readIntLE(bytes, at + expected);
            int crc = ByteUtils.readIntLE(bytes, at + expected + Integer.BYTES);
            if (length != expected) {
                throw new FramedRunCorruptionException(name, "chunk [" + chunk + "] length [" + length + "] is not [" + expected + "]");
            }
            if (crc != FramedRunLayout.crc32(bytes, at, expected)) {
                throw new FramedRunCorruptionException(name, "chunk [" + chunk + "] checksum mismatch");
            }
        }
    }

    /**
     * Reads the records of one partition in order; one read outstanding at a time. Keeps the current window, so
     * neighbouring records cost no extra read. A cursor used after its reader closed still charges the reader's breaker
     * for what it loads; closing the cursor returns its window.
     */
    public final class RecordCursor implements Releasable {
        private long entry;
        private final long end;
        @Nullable
        private Window window;
        private final AtomicBoolean cursorClosed = new AtomicBoolean();

        private RecordCursor(long entry, long end) {
            this.entry = entry;
            this.end = end;
        }

        public boolean hasNext() {
            return entry < end;
        }

        public long remaining() {
            return end - entry;
        }

        /**
         * Reads {@code length} bytes at the logical {@code offset}, through this cursor's window and without the index: for
         * a run whose records' positions the caller holds, as a value run's rows do. The buffer is charged to the breaker
         * before it is allocated; reads in ascending offset order share windows. The listener may complete inline.
         */
        public void readAt(long offset, int length, ActionListener<ReleasableBytesReference> listener) {
            if (offset < 0 || length < 0 || offset + length > footer.logicalBytes()) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "read of [" + length + "] bytes at [" + offset + "] is outside the run's [" + footer.logicalBytes() + "] bytes"
                    )
                );
                return;
            }
            if (length > maxima.recordBytes()) {
                listener.onFailure(new IllegalArgumentException("read of [" + length + "] bytes exceeds the record maximum"));
                return;
            }
            byte[] payload;
            try {
                breaker.addEstimateBytesAndMaybeBreak(length, "spill run read");
                payload = new byte[length];
            } catch (CircuitBreakingException e) {
                listener.onFailure(e);
                return;
            }
            ReleasableBytesReference reference = new ReleasableBytesReference(
                new BytesArray(payload),
                () -> breaker.addWithoutBreaking(-length)
            );
            fill(payload, offset, offset + length, 0, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    listener.onResponse(reference);
                }

                @Override
                public void onFailure(Exception e) {
                    reference.close();
                    listener.onFailure(e);
                }
            });
        }

        /**
         * The next record, or null at the end. The listener may complete inline.
         */
        public void next(ActionListener<Record> listener) {
            if (entry >= end) {
                listener.onResponse(null);
                return;
            }
            final long current = entry++;
            indexBlock(current, listener.delegateFailureAndWrap((l, block) -> {
                int i = (int) (current - block.firstEntry);
                int partition = block.partitions[i];
                long offset = block.offsets[i];
                int length = block.lengths[i];
                byte[] payload;
                try {
                    breaker.addEstimateBytesAndMaybeBreak(length, "spill run record");
                    payload = new byte[length];
                } catch (CircuitBreakingException e) {
                    l.onFailure(e);
                    return;
                }
                ReleasableBytesReference reference = new ReleasableBytesReference(
                    new BytesArray(payload),
                    () -> breaker.addWithoutBreaking(-length)
                );
                Record record = new Record(partition, offset, reference);
                // not a wrapping listener: a throw from the caller's onResponse must not close a record already handed out
                fill(payload, offset, offset + length, 0, new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        l.onResponse(record);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        reference.close();
                        l.onFailure(e);
                    }
                });
            }));
        }

        /**
         * Copies logical [from, to) into the payload, window by window.
         */
        private void fill(byte[] payload, long from, long to, int at, ActionListener<Void> listener) {
            if (from >= to) {
                listener.onResponse(null);
                return;
            }
            Window current = window;
            if (current != null && from >= current.logicalStart() && from < current.logicalEnd()) {
                long chunkTo = Math.min(to, current.logicalEnd());
                current.copy(from, chunkTo, payload, at);
                fill(payload, chunkTo, to, at + (int) (chunkTo - from), listener);
                return;
            }
            long chunk = FramedRunLayout.chunkOf(from, header.chunkSize());
            window(chunk, listener.delegateFailureAndWrap((l, loaded) -> {
                // the swap and the close take the cursor's monitor, so a close cannot slip between the check and the swap
                Window previous;
                synchronized (this) {
                    if (cursorClosed.get()) {
                        loaded.close();
                        l.onFailure(new IllegalStateException("cursor closed"));
                        return;
                    }
                    previous = window;
                    window = loaded;
                }
                Releasables.close(previous);
                fill(payload, from, to, at, l);
            }));
        }

        @Override
        public void close() {
            Window held;
            synchronized (this) {
                if (cursorClosed.compareAndSet(false, true) == false) {
                    return;
                }
                held = window;
                window = null;
            }
            Releasables.close(held);
        }
    }

    // ---- reading a range of the object into a charged buffer

    /**
     * Reads exactly {@code length} bytes at {@code offset} into one charged buffer, through as many reads as the backend
     * needs; the caller closes it.
     */
    static void readFully(
        SpillObject object,
        long offset,
        long length,
        CircuitBreaker breaker,
        ActionListener<ReleasableBytesReference> listener
    ) {
        if (length > Integer.MAX_VALUE - 16) {
            listener.onFailure(new IllegalArgumentException("read of [" + length + "] bytes is too large"));
            return;
        }
        byte[] buffer;
        try {
            breaker.addEstimateBytesAndMaybeBreak(length, "spill run read");
            buffer = new byte[(int) length];
        } catch (CircuitBreakingException e) {
            listener.onFailure(e);
            return;
        }
        ReleasableBytesReference result = new ReleasableBytesReference(new BytesArray(buffer), () -> breaker.addWithoutBreaking(-length));
        object.openRange(offset, length, ActionListener.wrap(reader -> readInto(reader, buffer, 0, ActionListener.wrap(ignored -> {
            reader.close(ActionListener.running(() -> listener.onResponse(result)));
        }, e -> {
            result.close();
            reader.close(ActionListener.running(() -> listener.onFailure(e)));
        })), e -> {
            result.close();
            listener.onFailure(e);
        }));
    }

    /**
     * Reads until the buffer is full, looping in place while the backend completes inline and recursing only when it
     * does not, so a backend of short reads cannot nest one frame per read.
     */
    private static void readInto(SpillReader reader, byte[] buffer, int at, ActionListener<Void> listener) {
        int position = at;
        while (position < buffer.length) {
            final int start = position;
            final AtomicBoolean handedOff = new AtomicBoolean();
            final AtomicInteger copied = new AtomicInteger();
            reader.read(buffer.length - start, listener.delegateFailureAndWrap((l, read) -> {
                if (read.isEof()) {
                    l.onFailure(new IOException("object ended after [" + start + "] of [" + buffer.length + "] bytes"));
                    return;
                }
                int n;
                try (ReleasableBytesReference data = read.data()) {
                    n = data.length();
                    if (n > buffer.length - start) {
                        l.onFailure(
                            new IOException("read returned [" + n + "] bytes, more than the [" + (buffer.length - start) + "] asked for")
                        );
                        return;
                    }
                    BytesRefIterator slices = data.iterator();
                    BytesRef slice;
                    int target = start;
                    while ((slice = slices.next()) != null) {
                        System.arraycopy(slice.bytes, slice.offset, buffer, target, slice.length);
                        target += slice.length;
                    }
                }
                copied.set(n);
                if (handedOff.compareAndSet(false, true)) {
                    return;
                }
                readInto(reader, buffer, start + n, l);
            }));
            if (handedOff.compareAndSet(false, true)) {
                return;
            }
            position += copied.get();
        }
        listener.onResponse(null);
    }
}
