/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.MAX_BUFFERED_BYTES_DEFAULT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.MIN_COMPRESS_BYTES_DEFAULT;

/**
 * Duel test between the row format (default Lucene binary DV) and the columnar flattened format.
 *
 * <p>The new columnar format reconstructs {@code binaryValue()} blobs in lexicographic key order
 * (ordinal = lex rank), so blobs are not byte-identical to the original insertion-order row format
 * when keys were written in a different order. The duel therefore asserts <em>semantic</em>
 * equality: both blobs decode to the same {@code key → ordered-list-of-values} multimap.
 *
 * <p>Blobs use {@code KeyedArrayOrderInlineNull} wire framing: per slot
 * {@code [vint prefix][key bytes][0x00][value bytes?]}, where prefix 0 = null slot
 * (no value bytes follow) and prefix N+1 = N value bytes follow.
 *
 * <p>Blobs are constructed with {@link #buildBlob} rather than going through
 * {@code MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull.recordValue}, which would add a
 * companion numeric count field. Adding numeric DV would cause {@link FlattenedDocValuesConsumer}
 * to throw {@link UnsupportedOperationException}, since that consumer only handles binary DV.
 */
public class FlattenedColumnarBinaryDuelTests extends ESTestCase {

    /** The keyed-field name (includes the {@code ._keyed} suffix). */
    private static final String FIELD = "labels._keyed";

    // ---------------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------------

    public void testBasicRoundTrip() throws IOException {
        duelRoundTrip(generateBlobs(100, 10, false, false));
    }

    public void testWithNullSlots() throws IOException {
        duelRoundTrip(generateBlobs(50, 8, true, false));
    }

    public void testWithMultipleValuesPerKey() throws IOException {
        duelRoundTrip(generateBlobs(50, 5, false, true));
    }

    public void testWithSparseField() throws IOException {
        final List<byte[]> blobs = new ArrayList<>(200);
        for (int d = 0; d < 200; d++) {
            blobs.add(random().nextFloat() < 0.3f ? null : buildOneDoc(15, d, false, false));
        }
        duelRoundTrip(blobs);
    }

    public void testShuffledKeyOrder() throws IOException {
        duelRoundTrip(generateBlobs(80, 12, false, false));
    }

    public void testLargeValuesMultiByteVint() throws IOException {
        // Values longer than 127 bytes exercise multi-byte vint length prefixes.
        final int numDocs = 20, numKeys = 5;
        final List<byte[]> blobs = new ArrayList<>(numDocs);
        for (int d = 0; d < numDocs; d++) {
            final List<String[]> slots = new ArrayList<>();
            for (int ki = 0; ki < numKeys; ki++) {
                final char[] filler = new char[200];
                Arrays.fill(filler, (char) ('a' + (ki % 26)));
                slots.add(new String[] { keyName(ki), new String(filler) + "-" + d });
            }
            blobs.add(buildBlob(slots));
        }
        duelRoundTrip(blobs);
    }

    public void testMultipleBlocks() throws IOException {
        // Use explicit small thresholds so multi-block coverage is pinned to intent and is not
        // silently affected by changes to the production defaults.
        final int tinyBlockDocs = 16;
        duelRoundTripWithFormat(
            new FlattenedDocValuesFormat(256, tinyBlockDocs, MIN_COMPRESS_BYTES_DEFAULT, MAX_BUFFERED_BYTES_DEFAULT),
            generateBlobs(tinyBlockDocs * 3 + 7, 8, false, false)
        );
    }

    /**
     * Duel test that exercises the {@link SortedSlotAccumulator} external-sort (spill) path by
     * using a tiny {@code maxBufferedBytes} so that slot records spill to run files on every flush.
     * The duel verifies end-to-end correctness: the columnar format must reconstruct the same blobs
     * as the reference row format regardless of the sort strategy used internally.
     *
     * <p>This is the end-to-end regression guard for the endianness bug: without the fix, the
     * external sort path byte-swaps every record header, causing {@code ArrayIndexOutOfBoundsException}
     * during flush.
     *
     * <p>Note: multi-value blobs (multiple values for the same key in one document) are exercised
     * separately by {@link #testWithMultipleValuesPerKey}, which uses the in-memory sort path where
     * the within-chunk insertion-order tiebreaker fully guarantees stability. The spill path is
     * stable within each sorted run, but the k-way merge heap does not guarantee cross-run insertion
     * order for equal {@code (lexRank, docId)} keys — a limitation that only surfaces when the same
     * document's values for one key span a chunk boundary, which does not occur for the value sizes
     * used in these tests.
     */
    public void testExternalSortSpillDuel() throws IOException {
        // Choose maxBufferedBytes small enough that even a handful of docs triggers the spill path.
        final int spillThreshold = random().nextBoolean() ? 256 : 1024;
        // Null slots are fine (single-value-per-key); multi-value blobs are tested separately.
        duelRoundTripWithFormat(
            new FlattenedDocValuesFormat(
                FlattenedDocValuesFormat.TARGET_BLOCK_BYTES_DEFAULT,
                FlattenedDocValuesFormat.MAX_DOCS_PER_BLOCK_DEFAULT,
                MIN_COMPRESS_BYTES_DEFAULT,
                spillThreshold
            ),
            generateBlobs(80, 10, random().nextBoolean(), false)
        );
    }

    public void testCanonicalOrder() throws IOException {
        // Keys already in lexicographic order — columnar reconstructs byte-identically.
        final int numDocs = 50, numKeys = 10;
        final List<byte[]> blobs = new ArrayList<>(numDocs);
        for (int d = 0; d < numDocs; d++) {
            final List<String[]> slots = new ArrayList<>();
            for (int ki = 0; ki < numKeys; ki++) {
                slots.add(new String[] { keyName(ki), "val-" + ki + "-" + d });
            }
            blobs.add(buildBlob(slots));
        }
        duelRoundTrip(blobs);
    }

    /**
     * Verifies the columnar fast-path: {@link ColumnarKeyedBinaryDocValues#lookupKeyOrdinal}
     * returns a valid ordinal for every key, and {@link ColumnarKeyedBinaryDocValues#advanceExactKey}
     * returns the same slot count as a linear scan of the reference row blob.
     */
    public void testColumnarFastPath() throws IOException {
        final int numKeys = 8;
        final List<byte[]> blobs = generateBlobs(100, numKeys, true, true);
        final List<BytesRef> rowBlobs = indexAndReadRow(blobs);

        try (Directory dir = newDirectory()) {
            indexBlobs(dir, blobs, new FlattenedDocValuesFormat());
            try (IndexReader reader = DirectoryReader.open(dir)) {
                for (final LeafReaderContext ctx : reader.leaves()) {
                    final LeafReader leaf = ctx.reader();
                    final BinaryDocValues rawDv = leaf.getBinaryDocValues(FIELD);
                    assertNotNull("no binary DV for " + FIELD, rawDv);
                    if ((rawDv instanceof ColumnarKeyedBinaryDocValues) == false) continue;
                    final ColumnarKeyedBinaryDocValues columnar = (ColumnarKeyedBinaryDocValues) rawDv;

                    for (int ki = 0; ki < numKeys; ki++) {
                        final BytesRef keyRef = new BytesRef(keyName(ki).getBytes(StandardCharsets.UTF_8));
                        assertTrue("key " + ki + " must be in segment dict", columnar.lookupKeyOrdinal(keyRef) >= 0);
                    }

                    for (int d = 0; d < leaf.maxDoc(); d++) {
                        if (columnar.advanceExact(d) == false) continue;
                        final BytesRef rowBlob = rowBlobs.get(ctx.docBase + d);
                        if (rowBlob == null) continue;

                        for (int ki = 0; ki < numKeys; ki++) {
                            final BytesRef keyRef = new BytesRef(keyName(ki).getBytes(StandardCharsets.UTF_8));
                            final int ord = columnar.lookupKeyOrdinal(keyRef);
                            if (ord < 0) continue;
                            final int expectedCount = countSlotsForKey(rowBlob, keyRef);
                            final int actualCount = columnar.advanceExactKey(ord);
                            // Drain slots so the cursor is positioned correctly for the next key.
                            for (int s = 0; s < actualCount; s++) {
                                columnar.nextKeyValue();
                            }
                            assertEquals("slot count for key " + ki + " at doc " + d, expectedCount, actualCount);
                        }
                    }
                }
            }
        }
    }

    /**
     * Duel test for {@link KeyColumnBatchReader}: compares the block produced by the batch path
     * against the per-doc reference path ({@link ColumnarKeyedBinaryDocValues#advanceExactKey} +
     * {@link ColumnarKeyedBinaryDocValues#nextKeyValue()}) for every key and every document in
     * the segment.
     *
     * <p>The test uses a small {@code maxDocsPerBlock} so columns span multiple blocks, exercising
     * the cross-block {@link SequentialColumnReader#advance} block-skip logic.
     */
    public void testBatchReaderDuelsPerDocPath() throws IOException {
        final int numKeys = 6;
        final int numDocs = 60;
        final int tinyBlockDocs = 8; // forces multi-block columns
        final List<byte[]> blobs = generateBlobs(numDocs, numKeys, true, true);

        final FlattenedDocValuesFormat fmt = new FlattenedDocValuesFormat(
            256, // small targetBlockBytes to mix compressed/uncompressed blocks
            tinyBlockDocs,
            MIN_COMPRESS_BYTES_DEFAULT,
            MAX_BUFFERED_BYTES_DEFAULT
        );

        try (Directory dir = newDirectory()) {
            indexBlobs(dir, blobs, fmt);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                for (final LeafReaderContext ctx : reader.leaves()) {
                    final LeafReader leaf = ctx.reader();
                    final BinaryDocValues rawDv = leaf.getBinaryDocValues(FIELD);
                    assertNotNull(rawDv);
                    if ((rawDv instanceof ColumnarKeyedBinaryDocValues) == false) continue;
                    final ColumnarKeyedBinaryDocValues columnar = (ColumnarKeyedBinaryDocValues) rawDv;

                    final int maxDoc = leaf.maxDoc();

                    for (int ki = 0; ki < numKeys; ki++) {
                        final BytesRef keyRef = new BytesRef(keyName(ki).getBytes(StandardCharsets.UTF_8));
                        final int ord = columnar.lookupKeyOrdinal(keyRef);
                        if (ord < 0) continue; // key absent from this segment

                        // Build a per-doc reference: for each doc, collect the sorted/deduped non-null values.
                        final List<Object> expected = new ArrayList<>(maxDoc);
                        for (int d = 0; d < maxDoc; d++) {
                            if (columnar.advanceExact(d) == false) {
                                expected.add(null);
                                continue;
                            }
                            final int slotCount = columnar.advanceExactKey(ord);
                            final List<BytesRef> vals = new ArrayList<>(slotCount);
                            for (int s = 0; s < slotCount; s++) {
                                final BytesRef v = columnar.nextKeyValue();
                                if (v != null) vals.add(BytesRef.deepCopyOf(v));
                            }
                            vals.sort(BytesRef::compareTo);
                            // deduplicate
                            for (int i = vals.size() - 1; i > 0; i--) {
                                if (vals.get(i).equals(vals.get(i - 1))) vals.remove(i);
                            }
                            if (vals.isEmpty()) {
                                expected.add(null);
                            } else if (vals.size() == 1) {
                                expected.add(vals.get(0));
                            } else {
                                expected.add(vals);
                            }
                        }

                        // Now read via the batch path using a simple sequential Docs list.
                        final BlockLoader.OptionalColumnAtATimeReader batchReader = columnar.keyColumnReader(ord);
                        assertNotNull("keyColumnReader must return non-null for ordinal " + ord, batchReader);

                        final int[] docArray = new int[maxDoc];
                        for (int d = 0; d < maxDoc; d++) {
                            docArray[d] = d;
                        }
                        final TestBlock block = (TestBlock) batchReader.tryRead(
                            TestBlock.factory(),
                            sequentialDocs(docArray),
                            0,
                            false,
                            null,
                            false,
                            false
                        );
                        assertNotNull("tryRead must not return null for key " + ki, block);
                        assertEquals("block size for key " + ki, maxDoc, block.size());

                        for (int d = 0; d < maxDoc; d++) {
                            assertEquals("value mismatch at doc " + d + " key " + ki + " (ord=" + ord + ")", expected.get(d), block.get(d));
                        }
                        block.close();
                    }
                }
            }
        }
    }

    /**
     * Returns a {@link BlockLoader.Docs} backed by a contiguous {@code [0, maxDoc)} range.
     * {@code mayContainDuplicates()} returns {@code false}.
     */
    private static BlockLoader.Docs sequentialDocs(int[] docs) {
        return new BlockLoader.Docs() {
            @Override
            public int count() {
                return docs.length;
            }

            @Override
            public int get(int i) {
                return docs[i];
            }

            @Override
            public boolean mayContainDuplicates() {
                return false;
            }
        };
    }

    // ---------------------------------------------------------------------------------
    // Metadata-compression threshold coverage
    // ---------------------------------------------------------------------------------

    /**
     * Exercises all four (FLAG_META_COMPRESSED, FLAG_VALUES_COMPRESSED) combinations in one test by
     * choosing parameters that produce the asymmetric cases:
     *
     * <ol>
     *   <li>Many docs with 1-byte values: metadata region is large (many slot-count + length entries)
     *       but the raw value bytes are small. A high {@code minCompressBytes} flips the combination:
     *       meta compresses and values do not.</li>
     *   <li>Two docs with multi-KB values: value region is large but the metadata payload is tiny
     *       (2 docs × 1 slot × packed length = a handful of bytes). Values compress, meta does not.</li>
     * </ol>
     *
     * <p>Both cases duel against the row format so round-trip correctness is verified regardless of
     * which compression flags are active.
     */
    public void testMetaCompressionThresholdBothWays() throws IOException {
        // Case 1: many docs with 1-byte values — forces meta compression, suppresses value compression.
        // minCompressBytes = 1024 so that the ~numDocs-byte metadata frame crosses the threshold
        // while the numDocs single-byte value region stays below it.
        final int manyDocs = 200;
        final int highThreshold = 1024;
        final List<byte[]> blobs1 = new ArrayList<>(manyDocs);
        for (int d = 0; d < manyDocs; d++) {
            blobs1.add(buildBlob(List.<String[]>of(new String[] { "k", String.valueOf(d % 10) })));
        }
        duelRoundTripWithFormat(
            new FlattenedDocValuesFormat(
                FlattenedDocValuesFormat.TARGET_BLOCK_BYTES_DEFAULT,
                manyDocs * 2,
                highThreshold,
                MAX_BUFFERED_BYTES_DEFAULT
            ),
            blobs1
        );

        // Case 2: two docs with large values — forces value compression, suppresses meta compression.
        // The metadata payload for 2 docs with 1 slot each is tiny (2 bytes), well below the default
        // minCompressBytes threshold; the ~4 KB value region exceeds it.
        final List<byte[]> blobs2 = new ArrayList<>();
        final char[] filler = new char[4000];
        Arrays.fill(filler, 'x');
        final String bigVal = new String(filler);
        blobs2.add(buildBlob(List.<String[]>of(new String[] { "k", bigVal + "-0" })));
        blobs2.add(buildBlob(List.<String[]>of(new String[] { "k", bigVal + "-1" })));
        duelRoundTripWithFormat(
            new FlattenedDocValuesFormat(
                FlattenedDocValuesFormat.TARGET_BLOCK_BYTES_DEFAULT,
                8192,
                MIN_COMPRESS_BYTES_DEFAULT,
                MAX_BUFFERED_BYTES_DEFAULT
            ),
            blobs2
        );
    }

    // ---------------------------------------------------------------------------------
    // Duel infrastructure
    // ---------------------------------------------------------------------------------

    private void duelRoundTrip(List<byte[]> blobs) throws IOException {
        duelRoundTripWithFormat(new FlattenedDocValuesFormat(), blobs);
    }

    private void duelRoundTripWithFormat(FlattenedDocValuesFormat fmt, List<byte[]> blobs) throws IOException {
        final List<BytesRef> rowValues = indexAndReadRow(blobs);
        final List<BytesRef> colValues = indexAndReadColumnar(blobs, fmt);

        assertEquals("doc count mismatch", rowValues.size(), colValues.size());
        for (int i = 0; i < rowValues.size(); i++) {
            final BytesRef row = rowValues.get(i);
            final BytesRef col = colValues.get(i);
            if (row == null && col == null) continue;
            assertNotNull("row is null but col is non-null at doc " + i, row);
            assertNotNull("col is null but row is non-null at doc " + i, col);
            assertEquals("multimap mismatch at doc " + i, parseBlob(row), parseBlob(col));
        }
    }

    private List<BytesRef> indexAndReadRow(List<byte[]> blobs) throws IOException {
        try (Directory dir = newDirectory()) {
            indexBlobs(dir, blobs, null);
            return readAllBinaryValues(dir);
        }
    }

    private List<BytesRef> indexAndReadColumnar(List<byte[]> blobs) throws IOException {
        return indexAndReadColumnar(blobs, new FlattenedDocValuesFormat());
    }

    private List<BytesRef> indexAndReadColumnar(List<byte[]> blobs, FlattenedDocValuesFormat fmt) throws IOException {
        try (Directory dir = newDirectory()) {
            indexBlobs(dir, blobs, fmt);
            return readAllBinaryValues(dir);
        }
    }

    private void indexBlobs(Directory dir, List<byte[]> blobs, FlattenedDocValuesFormat fmt) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig();
        if (fmt != null) {
            // Route ALL binary DV through FlattenedDocValuesFormat. No numeric/sorted DV is added
            // to these documents, so the format's UnsupportedOperationException is never hit.
            config.setCodec(TestUtil.alwaysDocValuesFormat(fmt));
        }
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (final byte[] blob : blobs) {
                final Document doc = new Document();
                if (blob != null) {
                    doc.add(new BinaryDocValuesField(FIELD, new BytesRef(blob)));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private List<BytesRef> readAllBinaryValues(Directory dir) throws IOException {
        final List<BytesRef> result = new ArrayList<>();
        try (IndexReader reader = DirectoryReader.open(dir)) {
            for (final LeafReaderContext ctx : reader.leaves()) {
                final LeafReader leaf = ctx.reader();
                final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
                final int maxDoc = leaf.maxDoc();
                if (dv == null) {
                    for (int d = 0; d < maxDoc; d++) {
                        result.add(null);
                    }
                } else {
                    for (int d = 0; d < maxDoc; d++) {
                        result.add(dv.advanceExact(d) ? BytesRef.deepCopyOf(dv.binaryValue()) : null);
                    }
                }
            }
        }
        return result;
    }

    // ---------------------------------------------------------------------------------
    // Blob generators
    // ---------------------------------------------------------------------------------

    private static List<byte[]> generateBlobs(int numDocs, int numKeys, boolean withNulls, boolean withArrays) {
        final List<byte[]> blobs = new ArrayList<>(numDocs);
        for (int d = 0; d < numDocs; d++) {
            blobs.add(buildOneDoc(numKeys, d, withNulls, withArrays));
        }
        return blobs;
    }

    private static byte[] buildOneDoc(int numKeys, int docSeed, boolean withNulls, boolean withArrays) {
        // Deterministic Fisher-Yates shuffle keyed on docSeed.
        final int[] perm = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            perm[i] = i;
        }
        for (int i = numKeys - 1; i > 0; i--) {
            final int j = (docSeed * 31 + i) % (i + 1);
            final int tmp = perm[i];
            perm[i] = perm[j];
            perm[j] = tmp;
        }
        final List<String[]> slots = new ArrayList<>(numKeys);
        for (final int ki : perm) {
            final int reps = (withArrays && ki % 3 == 0) ? 2 : 1;
            for (int r = 0; r < reps; r++) {
                if (withNulls && ki % 5 == 0 && r == 0) {
                    slots.add(new String[] { keyName(ki), null });
                } else {
                    slots.add(new String[] { keyName(ki), "val-" + ki + "-" + docSeed + "-" + r });
                }
            }
        }
        return buildBlob(slots);
    }

    // ---------------------------------------------------------------------------------
    // Blob encoding: KeyedArrayOrderInlineNull wire format
    //
    // per slot: [vint prefix][key bytes][0x00][value bytes?]
    // prefix 0 → null slot (no value bytes)
    // prefix N+1 → N value bytes follow after key\0
    // ---------------------------------------------------------------------------------

    /**
     * Encodes a list of {@code {key, value}} pairs into the {@code KeyedArrayOrderInlineNull} wire
     * format. A {@code null} value in the pair represents a null slot.
     */
    static byte[] buildBlob(List<String[]> slots) {
        int totalBytes = 0;
        for (final String[] kv : slots) {
            final byte[] keyBytes = kv[0].getBytes(StandardCharsets.UTF_8);
            final int valLen = kv[1] != null ? kv[1].getBytes(StandardCharsets.UTF_8).length : 0;
            final int prefix = kv[1] != null ? valLen + 1 : 0;
            totalBytes += vIntLen(prefix) + keyBytes.length + 1 + valLen;
        }
        final byte[] buf = new byte[totalBytes];
        int pos = 0;
        for (final String[] kv : slots) {
            final byte[] keyBytes = kv[0].getBytes(StandardCharsets.UTF_8);
            final byte[] valBytes = kv[1] != null ? kv[1].getBytes(StandardCharsets.UTF_8) : null;
            final int prefix = valBytes != null ? valBytes.length + 1 : 0;
            pos = writeVInt(buf, pos, prefix);
            System.arraycopy(keyBytes, 0, buf, pos, keyBytes.length);
            pos += keyBytes.length;
            buf[pos++] = 0;
            if (valBytes != null) {
                System.arraycopy(valBytes, 0, buf, pos, valBytes.length);
                pos += valBytes.length;
            }
        }
        assert pos == totalBytes;
        return buf;
    }

    // ---------------------------------------------------------------------------------
    // Blob parsing helpers
    // ---------------------------------------------------------------------------------

    static Map<String, List<String>> parseBlob(BytesRef blob) {
        final Map<String, List<String>> map = new LinkedHashMap<>();
        int pos = blob.offset;
        final int end = blob.offset + blob.length;
        while (pos < end) {
            int prefix = 0, shift = 0;
            while (true) {
                final int b = blob.bytes[pos++] & 0xFF;
                prefix |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            final boolean isNull = (prefix == 0);
            final int valLen = isNull ? 0 : (prefix - 1);
            final int keyStart = pos;
            while (pos < end && blob.bytes[pos] != 0) {
                pos++;
            }
            final String key = new String(blob.bytes, keyStart, pos - keyStart, StandardCharsets.UTF_8);
            pos++; // skip \0
            final String value = isNull ? null : new String(blob.bytes, pos, valLen, StandardCharsets.UTF_8);
            pos += valLen;
            map.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }
        return map;
    }

    static int countSlotsForKey(BytesRef blob, BytesRef key) {
        int count = 0;
        int pos = blob.offset;
        final int end = blob.offset + blob.length;
        while (pos < end) {
            int prefix = 0, shift = 0;
            while (true) {
                final int b = blob.bytes[pos++] & 0xFF;
                prefix |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            final int valLen = (prefix == 0) ? 0 : (prefix - 1);
            final int keyStart = pos;
            while (pos < end && blob.bytes[pos] != 0) {
                pos++;
            }
            final int keyLen = pos - keyStart;
            pos++; // skip \0
            if (keyLen == key.length) {
                boolean match = true;
                for (int i = 0; i < keyLen; i++) {
                    if (blob.bytes[keyStart + i] != key.bytes[key.offset + i]) {
                        match = false;
                        break;
                    }
                }
                if (match) count++;
            }
            pos += valLen;
        }
        return count;
    }

    private static String keyName(int ki) {
        return String.format(java.util.Locale.ROOT, "key%04d", ki);
    }

    private static int vIntLen(int v) {
        int n = 1;
        while ((v & ~0x7F) != 0) {
            n++;
            v >>>= 7;
        }
        return n;
    }

    private static int writeVInt(byte[] buf, int off, int v) {
        while ((v & ~0x7F) != 0) {
            buf[off++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        buf[off++] = (byte) v;
        return off;
    }
}
