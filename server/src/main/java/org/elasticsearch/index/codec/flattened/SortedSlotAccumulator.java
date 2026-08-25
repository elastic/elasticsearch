/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Accumulates {@code (keyOrd, docId, preEncodedSlotBytes)} triples written in
 * document-visit order and exposes them sorted by {@code (lexRank, docId)}.
 *
 * <h2>Protocol</h2>
 * <ol>
 *   <li>Call {@link #add} once per slot encountered during the document scan.</li>
 *   <li>After all documents have been scanned, compute the {@code lexRankOf} mapping
 *       (hash ordinal → lex rank, from {@link org.apache.lucene.util.BytesRefHash#sort})
 *       and call {@link #sortedCursor} to obtain a sorted cursor.</li>
 *   <li>Drain the cursor, then close it. Closing releases resources and deletes temp files.</li>
 * </ol>
 *
 * <h2>Sort strategy</h2>
 * Records are collected into a growing in-memory byte array during the collection phase.
 * When {@link #sortedCursor} is called (with {@code lexRankOf} now known):
 * <ul>
 *   <li>If the accumulated bytes are at most {@code maxBufferBytes}: sort an index array in
 *       memory and return an in-memory cursor. No temp files are created.</li>
 *   <li>Otherwise: partition the buffer into {@code maxBufferBytes}-sized chunks, sort each
 *       chunk by {@code (lexRankOf[keyOrd], docId)} and write it as a sorted-run temp file,
 *       then return a k-way merge cursor over all run files.</li>
 * </ul>
 *
 * <h2>Record format</h2>
 * Each record consists of three 4-byte <em>little-endian</em> integers
 * ({@code keyOrd}, {@code docId}, {@code payloadLen}) followed by {@code payloadLen} payload
 * bytes. Little-endian matches Lucene's {@link org.apache.lucene.store.DataOutput#writeInt} /
 * {@link org.apache.lucene.store.DataInput#readInt}, which is what the external-sort run
 * files are written and read through. The payload holds pre-encoded column-block slot bytes:
 * {@code [vint prefix][value bytes]} per slot, where prefix 0 = null and prefix N+1 = N
 * value bytes.
 */
final class SortedSlotAccumulator implements Closeable {

    /** Bytes of the fixed record header: {@code keyOrd + docId + payloadLen} (3 × 4). */
    static final int RECORD_HEADER_BYTES = 12;

    private final Directory directory;
    private final IOContext context;
    private final int maxBufferBytes;

    /** Growing in-memory buffer holding all unsorted records. */
    private byte[] buf;
    /** Number of bytes written into {@link #buf}. */
    private int bufLen;
    /** Number of records accumulated. */
    private int numRecords;

    SortedSlotAccumulator(Directory directory, IOContext context, int maxBufferBytes) {
        this.directory = directory;
        this.context = context;
        this.maxBufferBytes = maxBufferBytes;
        this.buf = new byte[Math.min(4096, maxBufferBytes)];
    }

    /**
     * Records one pre-encoded slot. {@code slotPayload[payloadOff..payloadOff+payloadLen)} must
     * contain the slot in columnar block-payload framing: {@code [vint prefix][value bytes]},
     * where prefix 0 = null and prefix {@code N+1} = N value bytes.
     */
    void add(int keyOrd, int docId, byte[] slotPayload, int payloadOff, int payloadLen) {
        final int recLen = RECORD_HEADER_BYTES + payloadLen;
        if (bufLen + recLen > buf.length) {
            buf = ArrayUtil.grow(buf, bufLen + recLen);
        }
        writeInt(buf, bufLen, keyOrd);
        writeInt(buf, bufLen + 4, docId);
        writeInt(buf, bufLen + 8, payloadLen);
        System.arraycopy(slotPayload, payloadOff, buf, bufLen + RECORD_HEADER_BYTES, payloadLen);
        bufLen += recLen;
        numRecords++;
    }

    /**
     * Returns a cursor over all records sorted by {@code (lexRankOf[keyOrd], docId)}.
     *
     * @param lexRankOf mapping from hash ordinal to lex rank, as produced by
     *                  {@link org.apache.lucene.util.BytesRefHash#sort}:
     *                  {@code sortedOrds[lexRank] = hashOrd} → {@code lexRankOf[hashOrd] = lexRank}
     */
    SortedCursor sortedCursor(int[] lexRankOf) throws IOException {
        if (numRecords == 0) {
            return SortedCursor.EMPTY;
        }
        if (bufLen <= maxBufferBytes) {
            return sortInMemory(lexRankOf);
        }
        return externalSort(lexRankOf);
    }

    // -----------------------------------------------------------------------
    // In-memory sort (data fits within maxBufferBytes)
    // -----------------------------------------------------------------------

    private SortedCursor sortInMemory(int[] lexRankOf) {
        final int n = numRecords;
        final int[] off = new int[n];
        final long[] key = new long[n];
        int pos = 0;
        for (int i = 0; i < n; i++) {
            off[i] = pos;
            key[i] = sortKey(lexRankOf[readInt(buf, pos)], readInt(buf, pos + 4));
            pos += RECORD_HEADER_BYTES + readInt(buf, pos + 8);
        }
        sortParallel(key, off, n);
        // Transfer buf ownership to the cursor (its lifecycle now owns buf).
        final byte[] data = buf;
        buf = null;
        return new InMemoryCursor(data, off, n, lexRankOf);
    }

    // -----------------------------------------------------------------------
    // External sort (data exceeds maxBufferBytes)
    // -----------------------------------------------------------------------

    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    private SortedCursor externalSort(int[] lexRankOf) throws IOException {
        final List<String> runFiles = new ArrayList<>();
        int pos = 0;
        final byte[] chunk = new byte[maxBufferBytes];

        try {
            while (pos < bufLen) {
                int chunkLen = 0;
                int numRecs = 0;

                while (pos < bufLen) {
                    final int payloadLen = readInt(buf, pos + 8);
                    final int recLen = RECORD_HEADER_BYTES + payloadLen;
                    if (chunkLen + recLen > chunk.length) {
                        if (numRecs > 0) break;
                        // Single oversized record: write it as its own sorted run and move on.
                        final byte[] oversized = new byte[recLen];
                        System.arraycopy(buf, pos, oversized, 0, recLen);
                        runFiles.add(writeSortedRun(oversized, recLen, 1, lexRankOf));
                        pos += recLen;
                        continue;
                    }
                    System.arraycopy(buf, pos, chunk, chunkLen, recLen);
                    chunkLen += recLen;
                    numRecs++;
                    pos += recLen;
                }

                if (numRecs > 0) {
                    runFiles.add(writeSortedRun(chunk, chunkLen, numRecs, lexRankOf));
                }
            }
        } catch (IOException | RuntimeException e) {
            // Clean up any run files already written before re-throwing.
            org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(directory, runFiles.toArray(new String[0]));
            throw e;
        }

        // buf no longer needed.
        buf = null;

        if (runFiles.size() == 1) {
            return new RunFileCursor(directory.openInput(runFiles.get(0), context), directory, runFiles.get(0), lexRankOf);
        }
        return new MergeCursor(directory, context, runFiles, lexRankOf);
    }

    private String writeSortedRun(byte[] chunk, int chunkLen, int numRecs, int[] lexRankOf) throws IOException {
        final int[] off = new int[numRecs];
        final long[] key = new long[numRecs];
        int pos = 0;
        for (int i = 0; i < numRecs; i++) {
            off[i] = pos;
            key[i] = sortKey(lexRankOf[readInt(chunk, pos)], readInt(chunk, pos + 4));
            pos += RECORD_HEADER_BYTES + readInt(chunk, pos + 8);
        }
        sortParallel(key, off, numRecs);

        final IndexOutput runOut = directory.createTempOutput("fss", "tmp", context);
        final String name = runOut.getName();
        try {
            for (int i = 0; i < numRecs; i++) {
                final int o = off[i];
                final int pl = readInt(chunk, o + 8);
                runOut.writeBytes(chunk, o, RECORD_HEADER_BYTES + pl);
            }
        } finally {
            runOut.close();
        }
        return name;
    }

    // -----------------------------------------------------------------------
    // Sort helpers
    // -----------------------------------------------------------------------

    private static long sortKey(int lexRank, int docId) {
        return ((long) lexRank << 32) | (docId & 0xFFFFFFFFL);
    }

    /**
     * Sorts {@code key[0..n)} ascending in-place, applying the same permutation to
     * {@code off[0..n)}.
     *
     * <p>The sort is effectively stable: when two records share the same primary sort key
     * {@code (lexRank, docId)} (i.e., multiple slot values for the same sub-field in the
     * same document), their relative order is resolved by {@code off[i]}, which is their
     * byte offset within the buffer or chunk and increases monotonically with insertion order.
     * This preserves the original document-visit order of multiple values for the same key,
     * which the columnar reader exposes as the array order for that field.
     */
    private static void sortParallel(final long[] key, final int[] off, final int n) {
        new IntroSorter() {
            private long pivotKey;
            private int pivotOff;

            @Override
            protected int compare(int i, int j) {
                final int c = Long.compare(key[i], key[j]);
                // Stable tiebreak: smaller byte offset = earlier insertion = earlier array slot.
                return c != 0 ? c : Integer.compare(off[i], off[j]);
            }

            @Override
            protected void swap(int i, int j) {
                final long tmpKey = key[i];
                key[i] = key[j];
                key[j] = tmpKey;
                final int tmpOff = off[i];
                off[i] = off[j];
                off[j] = tmpOff;
            }

            @Override
            protected void setPivot(int i) {
                pivotKey = key[i];
                pivotOff = off[i];
            }

            @Override
            protected int comparePivot(int j) {
                final int c = Long.compare(pivotKey, key[j]);
                return c != 0 ? c : Integer.compare(pivotOff, off[j]);
            }
        }.sort(0, n);
    }

    // -----------------------------------------------------------------------
    // Cursors
    // -----------------------------------------------------------------------

    /**
     * Sorted record cursor returned by {@link #sortedCursor}. Each {@link #next} call advances
     * to the next record; field accessors return values for the current record. Must be closed
     * after use to release temporary files.
     */
    abstract static class SortedCursor implements Closeable {

        static final SortedCursor EMPTY = new SortedCursor() {
            @Override
            public boolean next() {
                return false;
            }

            @Override
            public int lexRank() {
                return -1;
            }

            @Override
            public int docId() {
                return -1;
            }

            @Override
            public byte[] payloadBytes() {
                return new byte[0];
            }

            @Override
            public int payloadOffset() {
                return 0;
            }

            @Override
            public int payloadLength() {
                return 0;
            }

            @Override
            public void close() {}
        };

        abstract boolean next() throws IOException;

        abstract int lexRank();

        abstract int docId();

        abstract byte[] payloadBytes();

        abstract int payloadOffset();

        abstract int payloadLength();
    }

    /** In-memory cursor over a sorted index array. */
    private static final class InMemoryCursor extends SortedCursor {
        private final byte[] buf;
        private final int[] off;
        private final int count;
        private final int[] lexRankOf;
        private int idx = -1;
        private int curLexRank;
        private int curDocId;

        InMemoryCursor(byte[] buf, int[] off, int count, int[] lexRankOf) {
            this.buf = buf;
            this.off = off;
            this.count = count;
            this.lexRankOf = lexRankOf;
        }

        @Override
        public boolean next() {
            if (++idx >= count) return false;
            final int o = off[idx];
            curLexRank = lexRankOf[readInt(buf, o)];
            curDocId = readInt(buf, o + 4);
            return true;
        }

        @Override
        public int lexRank() {
            return curLexRank;
        }

        @Override
        public int docId() {
            return curDocId;
        }

        @Override
        public byte[] payloadBytes() {
            return buf;
        }

        @Override
        public int payloadOffset() {
            return off[idx] + RECORD_HEADER_BYTES;
        }

        @Override
        public int payloadLength() {
            return readInt(buf, off[idx] + 8);
        }

        @Override
        public void close() {}
    }

    /** Sequential cursor over a single sorted run file. */
    private static final class RunFileCursor extends SortedCursor {
        private final IndexInput in;
        private final Directory dir;
        private final String fileName;
        private final int[] lexRankOf;
        private final long fileLen;
        int curLexRank;
        int curDocId;
        int curPayloadLen;
        byte[] payload = new byte[64];

        RunFileCursor(IndexInput in, Directory dir, String fileName, int[] lexRankOf) {
            this.in = in;
            this.dir = dir;
            this.fileName = fileName;
            this.lexRankOf = lexRankOf;
            this.fileLen = in.length();
        }

        @Override
        public boolean next() throws IOException {
            if (in.getFilePointer() >= fileLen) return false;
            curLexRank = lexRankOf[in.readInt()];
            curDocId = in.readInt();
            curPayloadLen = in.readInt();
            if (curPayloadLen > payload.length) {
                payload = new byte[ArrayUtil.oversize(curPayloadLen, 1)];
            }
            in.readBytes(payload, 0, curPayloadLen);
            return true;
        }

        @Override
        public int lexRank() {
            return curLexRank;
        }

        @Override
        public int docId() {
            return curDocId;
        }

        @Override
        public byte[] payloadBytes() {
            return payload;
        }

        @Override
        public int payloadOffset() {
            return 0;
        }

        @Override
        public int payloadLength() {
            return curPayloadLen;
        }

        @Override
        @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
        public void close() throws IOException {
            IOUtils.close(in);
            org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(dir, fileName);
        }
    }

    /** K-way merge cursor over multiple sorted run files, backed by a min-heap. */
    private static final class MergeCursor extends SortedCursor {
        private final List<RunFileCursor> cursors;
        private final PriorityQueue<RunFileCursor> heap;
        private RunFileCursor current;

        MergeCursor(Directory dir, IOContext context, List<String> runFiles, int[] lexRankOf) throws IOException {
            cursors = new ArrayList<>(runFiles.size());
            heap = new PriorityQueue<>((a, b) -> {
                final int cmp = Integer.compare(a.curLexRank, b.curLexRank);
                return cmp != 0 ? cmp : Integer.compare(a.curDocId, b.curDocId);
            });
            for (final String f : runFiles) {
                final RunFileCursor c = new RunFileCursor(dir.openInput(f, context), dir, f, lexRankOf);
                cursors.add(c);
                if (c.next()) heap.offer(c);
            }
        }

        @Override
        public boolean next() throws IOException {
            if (current != null) {
                if (current.next()) heap.offer(current);
                current = null;
            }
            if (heap.isEmpty()) return false;
            current = heap.poll();
            return true;
        }

        @Override
        public int lexRank() {
            return current.curLexRank;
        }

        @Override
        public int docId() {
            return current.curDocId;
        }

        @Override
        public byte[] payloadBytes() {
            return current.payload;
        }

        @Override
        public int payloadOffset() {
            return 0;
        }

        @Override
        public int payloadLength() {
            return current.curPayloadLen;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(cursors);
        }
    }

    // -----------------------------------------------------------------------
    // I/O helpers — little-endian to match Lucene's DataInput#readInt /
    // DataOutput#writeInt, since run files are read back through IndexInput.
    // -----------------------------------------------------------------------

    static int readInt(byte[] buf, int off) {
        return (int) BitUtil.VH_LE_INT.get(buf, off);
    }

    private static void writeInt(byte[] buf, int off, int v) {
        BitUtil.VH_LE_INT.set(buf, off, v);
    }

    @Override
    public void close() {
        buf = null;
    }
}
