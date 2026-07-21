/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.LongBytesRefAdaptiveBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * End-to-end tests verifying that {@link PageColumnReader} emits an
 * {@link OrdinalBytesRefBlock} for dictionary-encoded BINARY columns.
 *
 * <p>The ordinal block is the input shape that activates the
 * {@code BytesRefBlockHash#addOrdinalsVector}/{@code addOrdinalsBlock} fast path in the
 * compute engine: hash aggregation runs over the {@code k} dictionary entries instead of
 * the {@code N} row values, which is the win we are after for high-cardinality string
 * GROUP BY on Parquet.
 */
public class ParquetOrdinalBytesRefTests extends ESTestCase {

    private static final int BATCH_SIZE = 1000;

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() throws Exception {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testDictionaryEncodedColumnEmitsOrdinalBlock() throws IOException {
        int numRows = 5_000;
        int dictSize = 64;
        // Many repeats per dictionary entry — the writer keeps the column dictionary-encoded.
        byte[] data = writeStringFile(numRows, false, row -> "v_" + (row % dictSize));

        verifyDictionaryEncoded(data, numRows, false, row -> "v_" + (row % dictSize));
    }

    public void testNullableDictionaryEncodedColumnEmitsOrdinalBlock() throws IOException {
        int numRows = 5_000;
        int dictSize = 32;
        byte[] data = writeStringFile(numRows, true, row -> row % 7 == 0 ? null : "color_" + (row % dictSize));

        verifyDictionaryEncoded(data, numRows, true, row -> row % 7 == 0 ? null : "color_" + (row % dictSize));
    }

    /**
     * End-to-end check that the ordinal block produced by the Parquet reader is consumable by
     * {@link BlockHash}: the hash returns the same group ids and key set whether we feed it the
     * {@link OrdinalBytesRefBlock} from the reader or a freshly-built equivalent
     * {@code BytesRefBlock} populated with the same values. The dictionary size is a small
     * fraction of the row count, so on a correct ordinal path the hash table ends up holding
     * exactly the dictionary entries (k keys for N rows, k &lt;&lt; N).
     *
     * <p>This also exercises the asynchronous-consumer lifetime invariant: we deliberately
     * release every parquet-sourced page and close the underlying iterator <strong>before</strong>
     * extracting keys from the {@link BlockHash}. If the dictionary {@code BytesRefVector}
     * still aliased Parquet-side memory (rather than its own deep copy in
     * {@code BytesRefArray}-backed {@code BigArrays} storage), the keys we read out at the end
     * would be corrupted.
     */
    public void testOrdinalBlockFlowsThroughBlockHashAfterSourceReleased() throws IOException {
        int numRows = 4_000;
        int distinctValues = 50;
        IntFunction<String> valueFor = row -> "term_" + (row % distinctValues);
        byte[] data = writeStringFile(numRows, false, valueFor);

        int[] groupIdsFromOrdinal = new int[numRows];
        BytesRef[] keysFromOrdinal;
        int numKeysOrdinal;
        boolean sawOrdinalBlock = false;
        try (BlockHash hash = newBytesRefHash()) {
            try (CloseableIterator<Page> iter = openIterator(data)) {
                int row = 0;
                while (iter.hasNext()) {
                    Page page = iter.next();
                    try {
                        BytesRefBlock col = page.getBlock(0);
                        if (col.asOrdinals() != null) {
                            sawOrdinalBlock = true;
                        }
                        int rowAtPageStart = row;
                        hash.add(page, new CapturingAddInput(groupIdsFromOrdinal, rowAtPageStart));
                        row += page.getPositionCount();
                    } finally {
                        page.releaseBlocks();
                    }
                }
                assertEquals(numRows, row);
            }
            // Source iterator and pages are now closed. Read keys out of the hash.
            assertTrue("ordinal block should have been observed at least once", sawOrdinalBlock);
            numKeysOrdinal = hash.numKeys();
            keysFromOrdinal = readKeys(hash);
        }

        // Reference path: same data, plain BytesRefBlock.
        int[] groupIdsFromPlain = new int[numRows];
        BytesRef[] keysFromPlain;
        int numKeysPlain;
        try (BlockHash hash = newBytesRefHash()) {
            try (Page page = new Page(buildPlainBytesRefBlock(numRows, valueFor))) {
                hash.add(page, new CapturingAddInput(groupIdsFromPlain, 0));
            }
            numKeysPlain = hash.numKeys();
            keysFromPlain = readKeys(hash);
        }

        // The hash sees one key per logically-distinct value. We deliberately do not pin the
        // count to the writer's dictionary cardinality (which can include unused entries in
        // some encodings) — the structural assertion is that ordinal and plain runs agree.
        assertEquals("logical distinct value count", distinctValues, numKeysPlain);
        assertEquals("plain and ordinal hashes must agree on key count", numKeysPlain, numKeysOrdinal);

        // Group ids may not be byte-equal across runs (insertion order can differ), so compare
        // by mapping each group id to the key it represents.
        for (int i = 0; i < numRows; i++) {
            BytesRef expected = new BytesRef(valueFor.apply(i));
            assertEquals("row " + i + " (ordinal path)", expected, keysFromOrdinal[groupIdsFromOrdinal[i]]);
            assertEquals("row " + i + " (plain path)", expected, keysFromPlain[groupIdsFromPlain[i]]);
        }
    }

    /**
     * End-to-end check that a Parquet file with a dictionary-encoded BYTES_REF column plus a LONG column
     * flows into the {@code (LONG, BYTES_REF)} {@link BlockHash}'s vector-only fast path. This is the
     * input shape produced by the ClickBench q17/q18 query
     * {@code STATS COUNT(*) BY UserID:long, SearchPhrase:keyword}: a dense {@link LongBlock} (a vector
     * for non-nullable columns) paired with an {@link OrdinalBytesRefBlock}. The adaptive hash should
     * keep its vector-only delegate the whole way through — no migration to {@link
     * org.elasticsearch.compute.aggregation.blockhash.PackedValuesBlockHash} — so the dictionary is
     * hashed once instead of once per row.
     */
    public void testLongBytesRefStatsKeepsVectorOnlyFastPath() throws IOException {
        int numRows = 4_000;
        int dictSize = 50;
        int numUsers = 20;
        // Make user and phrase independent so we exercise a non-trivial group count
        // (numUsers * dictSize = 1000 distinct (long, bytesref) pairs).
        IntFunction<String> phraseFor = row -> "phrase_" + (row % dictSize);
        IntFunction<Long> userFor = row -> (long) ((row / dictSize) % numUsers);
        byte[] data = writeLongAndStringFile(numRows, phraseFor, userFor);

        boolean sawOrdinalBlock = false;
        boolean sawLongVector = false;
        Map<List<Object>, Integer> expectedGroupOrds = new HashMap<>();
        try (
            LongBytesRefAdaptiveBlockHash hash = (LongBytesRefAdaptiveBlockHash) BlockHash.build(
                List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
                blockFactory,
                BATCH_SIZE,
                /* allowBrokenOptimizations */ false
            )
        ) {
            try (CloseableIterator<Page> iter = openIterator(data, List.of("user_id", "search_phrase"))) {
                int row = 0;
                while (iter.hasNext()) {
                    Page page = iter.next();
                    try {
                        LongBlock longs = page.getBlock(0);
                        BytesRefBlock bytes = page.getBlock(1);
                        if (longs.asVector() != null) {
                            sawLongVector = true;
                        }
                        if (bytes instanceof OrdinalBytesRefBlock obrb) {
                            assertNotNull("ordinal vector at row=" + row, obrb.asVector());
                            sawOrdinalBlock = true;
                        } else {
                            BytesRefVector v = bytes.asVector();
                            // OrdinalBytesRefVector is a BytesRefVector; both shapes are vectors here.
                            assertTrue("bytes column must be a vector at row=" + row, v != null);
                            if (v instanceof OrdinalBytesRefVector) {
                                sawOrdinalBlock = true;
                            }
                        }
                        hash.add(page, new GroupCapturingAddInput(expectedGroupOrds, page));
                        row += page.getPositionCount();
                    } finally {
                        page.releaseBlocks();
                    }
                }
                assertEquals(numRows, row);
            }
            // The crux of this test: the adaptive hash must stay on its vector-only fast path. If it
            // had migrated, this would flip to true.
            assertFalse("expected vector-only fast path; got " + hash, hash.migratedToPackedHash());

            // Read final group keys for cross-checking.
            try (IntVector nonEmpty = hash.nonEmpty()) {
                assertEquals("captured one entry per distinct key", expectedGroupOrds.size(), hash.numKeys());
                assertEquals(numUsers * dictSize, hash.numKeys());
                Block[] keys = hash.getKeys(nonEmpty);
                try {
                    LongBlock keyLong = (LongBlock) keys[0];
                    BytesRefBlock keyBytes = (BytesRefBlock) keys[1];
                    BytesRef scratch = new BytesRef();
                    Map<Integer, List<Object>> ordToKey = new HashMap<>();
                    for (int p = 0; p < nonEmpty.getPositionCount(); p++) {
                        int gid = nonEmpty.getInt(p);
                        Long lv = keyLong.isNull(p) ? null : keyLong.getLong(p);
                        String bv = keyBytes.isNull(p) ? null : keyBytes.getBytesRef(p, scratch).utf8ToString();
                        ordToKey.put(gid, List.of(lv == null ? "_NULL_" : lv, bv == null ? "_NULL_" : bv));
                    }
                    for (Map.Entry<List<Object>, Integer> e : expectedGroupOrds.entrySet()) {
                        List<Object> expectedKey = e.getKey();
                        List<Object> actualKey = ordToKey.get(e.getValue());
                        assertNotNull("group id " + e.getValue() + " missing from keys", actualKey);
                        assertEquals(expectedKey, actualKey);
                    }
                } finally {
                    Releasables.closeExpectNoException(keys);
                }
            }
        }
        assertTrue("expected ordinal-encoded bytes block from parquet at least once", sawOrdinalBlock);
        assertTrue("expected long vector from parquet at least once", sawLongVector);
    }

    /**
     * Captures the {@code (long, bytesref)} key for each input row alongside the group ord assigned by the
     * hash so the test can cross-check {@link BlockHash#getKeys} after all pages have been consumed.
     */
    private static final class GroupCapturingAddInput implements GroupingAggregatorFunction.AddInput {
        private final Map<List<Object>, Integer> target;
        private final LongBlock longBlock;
        private final BytesRefBlock bytesBlock;
        private final BytesRef scratch = new BytesRef();

        GroupCapturingAddInput(Map<List<Object>, Integer> target, Page page) {
            this.target = target;
            this.longBlock = page.getBlock(0);
            this.bytesBlock = page.getBlock(1);
        }

        private List<Object> keyAt(int position) {
            Object lv = longBlock.isNull(position) ? "_NULL_" : longBlock.getLong(longBlock.getFirstValueIndex(position));
            Object bv = bytesBlock.isNull(position)
                ? "_NULL_"
                : bytesBlock.getBytesRef(bytesBlock.getFirstValueIndex(position), scratch).utf8ToString();
            return List.of(lv, bv);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                target.put(keyAt(positionOffset + p), groupIds.getInt(p));
            }
        }

        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                if (groupIds.isNull(p) || groupIds.getValueCount(p) != 1) {
                    throw new AssertionError("unexpected null/multivalue group at " + p + " (no nulls/MV in this test)");
                }
                target.put(keyAt(positionOffset + p), groupIds.getInt(groupIds.getFirstValueIndex(p)));
            }
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                if (groupIds.isNull(p) || groupIds.getValueCount(p) != 1) {
                    throw new AssertionError("unexpected null/multivalue group at " + p + " (no nulls/MV in this test)");
                }
                target.put(keyAt(positionOffset + p), groupIds.getInt(groupIds.getFirstValueIndex(p)));
            }
        }

        @Override
        public void close() {
            throw new AssertionError("BlockHash#add should not close the AddInput");
        }
    }

    public void testUniqueStringsRemainCorrectAcrossPages() throws IOException {
        // High-cardinality input: every value distinct. Whether the writer keeps the column
        // dictionary-encoded or falls back to PLAIN depends on parquet-mr's per-column
        // heuristics, which we don't try to pin down here. The contract verified is that
        // the reader returns the correct values regardless of the chosen encoding (i.e.
        // both the ordinal and materialized paths agree on the values they emit).
        int numRows = 2_000;
        byte[] data = writeStringFile(numRows, false, row -> "unique_" + row);

        List<Page> pages = readAll(data);
        try {
            int row = 0;
            for (Page page : pages) {
                BytesRefBlock block = page.getBlock(0);
                assertNotNull("page should produce a block", block);
                BytesRef scratch = new BytesRef();
                for (int p = 0; p < page.getPositionCount(); p++) {
                    assertFalse("required column", block.isNull(p));
                    assertEquals(new BytesRef("unique_" + row), block.getBytesRef(p, scratch));
                    row++;
                }
            }
            assertEquals(numRows, row);
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    // --- helpers ---

    private void verifyDictionaryEncoded(byte[] data, int numRows, boolean optional, IntFunction<String> valueFor) throws IOException {
        List<Page> pages = readAll(data);
        try {
            int seenOrdinalPages = 0;
            int totalRows = 0;
            BytesRef scratch = new BytesRef();
            for (Page page : pages) {
                BytesRefBlock block = page.getBlock(0);
                assertNotNull(block);
                OrdinalBytesRefBlock ordinal = block.asOrdinals();
                if (ordinal != null) {
                    seenOrdinalPages++;
                    assertTrue(
                        "dictionary should be small relative to dataset cardinality",
                        ordinal.getDictionaryVector().getPositionCount() < numRows
                    );
                }
                for (int p = 0; p < page.getPositionCount(); p++) {
                    String expected = valueFor.apply(totalRows);
                    if (expected == null) {
                        assertTrue("expected null at row " + totalRows, block.isNull(p));
                    } else {
                        assertFalse("unexpected null at row " + totalRows, block.isNull(p));
                        assertEquals("row " + totalRows, new BytesRef(expected), block.getBytesRef(p, scratch));
                    }
                    totalRows++;
                }
            }
            assertEquals(numRows, totalRows);
            assertTrue("expected at least one ordinal-encoded page; got " + seenOrdinalPages + " of " + pages.size(), seenOrdinalPages > 0);
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    private List<Page> readAll(byte[] data) throws IOException {
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = openIterator(data)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }
        return pages;
    }

    private CloseableIterator<Page> openIterator(byte[] data) throws IOException {
        return openIterator(data, List.of("name"));
    }

    private CloseableIterator<Page> openIterator(byte[] data, List<String> columns) throws IOException {
        StorageObject so = storageObject(data);
        return new ParquetFormatReader(blockFactory).read(so, FormatReadContext.of(columns, BATCH_SIZE));
    }

    private BlockHash newBytesRefHash() {
        // allowBrokenOptimizations is irrelevant for a single BYTES_REF group (BlockHash.build
        // routes that case directly to BytesRefBlockHash regardless of the flag), but keep it
        // false so readers don't infer the test relies on optimizations with known caveats.
        return BlockHash.build(List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF)), blockFactory, randomIntBetween(1, 1000), false);
    }

    private BytesRefBlock buildPlainBytesRefBlock(int numRows, IntFunction<String> valueFor) {
        try (var builder = blockFactory.newBytesRefBlockBuilder(numRows)) {
            for (int i = 0; i < numRows; i++) {
                builder.appendBytesRef(new BytesRef(valueFor.apply(i)));
            }
            return builder.build();
        }
    }

    /**
     * Reads the distinct keys held by a {@link BlockHash} into a {@link BytesRef} array indexed
     * by group id (with index 0 reserved for the null group). Used to map captured group ids
     * back to their original values when comparing two hash runs whose group-id assignment may
     * differ in insertion order.
     */
    private BytesRef[] readKeys(BlockHash hash) {
        try (IntVector selected = hash.nonEmpty()) {
            Block[] keyBlocks = hash.getKeys(selected);
            try {
                BytesRefBlock keys = (BytesRefBlock) keyBlocks[0];
                BytesRef[] out = new BytesRef[hash.numKeys() + 1];
                BytesRef scratch = new BytesRef();
                for (int p = 0; p < keys.getPositionCount(); p++) {
                    int groupId = selected.getInt(p);
                    if (keys.isNull(p)) {
                        out[groupId] = null;
                    } else {
                        out[groupId] = BytesRef.deepCopyOf(keys.getBytesRef(p, scratch));
                    }
                }
                return out;
            } finally {
                Releasables.closeExpectNoException(keyBlocks);
            }
        }
    }

    /**
     * Captures the group ids emitted by {@link BlockHash#add(Page, GroupingAggregatorFunction.AddInput)}
     * into a single shared array indexed by the row's position in the input dataset.
     */
    private static final class CapturingAddInput implements GroupingAggregatorFunction.AddInput {
        private final int[] target;
        private final int positionBase;

        CapturingAddInput(int[] target, int positionBase) {
            this.target = target;
            this.positionBase = positionBase;
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            for (int i = 0; i < groupIds.getPositionCount(); i++) {
                target[positionBase + positionOffset + i] = groupIds.getInt(i);
            }
        }

        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
            // No multivalues / nulls expected for this test (required column, no MV).
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                if (groupIds.isNull(p) || groupIds.getValueCount(p) != 1) {
                    throw new AssertionError("unexpected null/multivalue group at " + p);
                }
                target[positionBase + positionOffset + p] = groupIds.getInt(groupIds.getFirstValueIndex(p));
            }
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                if (groupIds.isNull(p) || groupIds.getValueCount(p) != 1) {
                    throw new AssertionError("unexpected null/multivalue group at " + p);
                }
                target[positionBase + positionOffset + p] = groupIds.getInt(groupIds.getFirstValueIndex(p));
            }
        }

        @Override
        public void close() {
            // BlockHash#add must not close its AddInput callback (per its Javadoc).
            throw new AssertionError("BlockHash should not close the AddInput callback");
        }
    }

    /**
     * Writes a 2-column Parquet file with a non-null {@code user_id} long and a non-null dictionary-encoded
     * {@code search_phrase} string per row. Models the schema shape ClickBench produces for q17/q18.
     */
    private byte[] writeLongAndStringFile(int numRows, IntFunction<String> phraseFor, IntFunction<Long> userFor) throws IOException {
        Types.MessageTypeBuilder schemaBuilder = Types.buildMessage();
        schemaBuilder.addField(Types.required(INT64).named("user_id"));
        schemaBuilder.addField(Types.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("search_phrase"));
        MessageType schema = schemaBuilder.named("long_bytesref_test");
        return writeFile(schema, numRows, (g, i) -> {
            g.append("user_id", userFor.apply(i));
            g.append("search_phrase", phraseFor.apply(i));
        });
    }

    private byte[] writeStringFile(int numRows, boolean optional, IntFunction<String> valueFor) throws IOException {
        Types.MessageTypeBuilder schemaBuilder = Types.buildMessage();
        var col = optional ? Types.optional(BINARY) : Types.required(BINARY);
        col.as(LogicalTypeAnnotation.stringType());
        schemaBuilder.addField(col.named("name"));
        MessageType schema = schemaBuilder.named("ordinal_test");

        return writeFile(schema, numRows, (g, i) -> {
            String v = valueFor.apply(i);
            if (v != null) {
                g.append("name", v);
            }
        });
    }

    private byte[] writeFile(MessageType schema, int numRows, BiConsumer<Group, Integer> populate) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(64 * 1024)
                .withRowGroupSize(64 * 1024L)
                .withPageSize(4 * 1024)
                .build()
        ) {
            for (int i = 0; i < numRows; i++) {
                Group g = factory.newGroup();
                populate.accept(g, i);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    private static StorageObject storageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.EPOCH;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://ordinal_test.parquet");
            }
        };
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        out.close();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://ordinal_test.parquet";
            }
        };
    }
}
