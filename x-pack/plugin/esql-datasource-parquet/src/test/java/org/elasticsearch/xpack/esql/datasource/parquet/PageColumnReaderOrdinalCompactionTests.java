/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Unit tests for the ordinal-dictionary compaction in {@link PageColumnReader} and
 * {@link OrdinalCompaction#compact}, driven by hand-built Parquet pages served from an in-memory
 * {@link PageReader}, so the page layout (dictionary vs PLAIN pages, nulls, out-of-range indices)
 * is exact rather than up to the writer's heuristics.
 *
 * <p>The invariant under test: a batch at or below the share threshold carries a fully referenced
 * dictionary in ascending chunk-dictionary order. A batch above the threshold, with a small
 * dictionary, or inside the short back-off window shares the chunk dictionary. Values are unchanged
 * in every case.
 */
public class PageColumnReaderOrdinalCompactionTests extends ESTestCase {

    private static final String COLUMN = "name";

    private TestBreaker breaker;
    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        breaker = new TestBreaker();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
    }

    /**
     * A batch that touches part of the dictionary gets a dictionary of exactly the referenced entries,
     * in ascending chunk-dictionary order, with renumbered ordinals.
     */
    public void testPartialBatchGetsCompactDictionary() {
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(dictionaryPage(entries(8)), dictionaryDataPage(false, 6, 1, 6, 3, 1, 3, 6, 1, 1, 3));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(10, blockFactory)) {
                OrdinalBytesRefBlock ordinals = block.asOrdinals();
                assertNotNull("expected an ordinal block", ordinals);
                assertDictionary(ordinals, "e00001", "e00003", "e00006");
                IntBlock ords = ordinals.getOrdinalsBlock();
                int[] expected = { 2, 0, 2, 1, 0, 1, 2, 0, 0, 1 };
                for (int p = 0; p < expected.length; p++) {
                    assertEquals("ordinal at " + p, expected[p], ords.getInt(ords.getFirstValueIndex(p)));
                }
                assertValues(block, "e00006", "e00001", "e00006", "e00003", "e00001", "e00003", "e00006", "e00001", "e00001", "e00003");
            }
        }
        assertEquals(0, breaker.getUsed());
    }

    /** A batch that references every entry keeps the whole chunk dictionary. */
    public void testFullyReferencedBatchKeepsChunkDictionary() {
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(dictionaryPage(entries(4)), dictionaryDataPage(false, 3, 2, 1, 0, 0, 1, 2, 3));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(8, blockFactory)) {
                OrdinalBytesRefBlock ordinals = block.asOrdinals();
                assertNotNull(ordinals);
                assertDictionary(ordinals, "e00000", "e00001", "e00002", "e00003");
                assertValues(block, "e00003", "e00002", "e00001", "e00000", "e00000", "e00001", "e00002", "e00003");
            }
        }
    }

    /**
     * Null slots carry ordinal 0 in the decoded array; they must not mark entry 0 as referenced. A
     * batch whose only non-null value is another entry compacts to a one-entry dictionary (not a
     * constant block, since it has nulls).
     */
    public void testNullsDoNotKeepEntryZero() {
        ColumnDescriptor desc = column(true);
        PageReader pages = pageReader(dictionaryPage(entries(4)), dictionaryDataPage(true, null, 3, null, null, 3));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(5, blockFactory)) {
                OrdinalBytesRefBlock ordinals = block.asOrdinals();
                assertNotNull("a nullable single-entry batch stays an ordinal block", ordinals);
                assertDictionary(ordinals, "e00003");
                assertValues(block, null, "e00003", null, null, "e00003");
            }
        }
    }

    /** Non-null, all-equal ordinals still become a constant block, with or without compaction. */
    public void testConstantBatch() {
        for (boolean compaction : new boolean[] { true, false }) {
            ColumnDescriptor desc = column(false);
            PageReader pages = pageReader(dictionaryPage(entries(8)), dictionaryDataPage(false, 5, 5, 5, 5));
            try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
                reader.setOrdinalCompaction(compaction);
                try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(4, blockFactory)) {
                    assertNull("compaction=" + compaction, block.asOrdinals());
                    assertNotNull("compaction=" + compaction, block.asVector());
                    assertTrue("compaction=" + compaction, block.asVector().isConstant());
                    assertValues(block, "e00005", "e00005", "e00005", "e00005");
                }
                assertEquals("constant detection must not allocate mark scratch", 0, breaker.getUsed());
            }
        }
    }

    /** Exactly half compacts; one entry above half shares. Each case starts with a fresh reader. */
    public void testShareThresholdBoundary() {
        int dictSize = 100;
        for (int referenced : new int[] { dictSize / 2, dictSize / 2 + 1 }) {
            ColumnDescriptor desc = column(false);
            PageReader pages = pageReader(
                dictionaryPage(entries(dictSize)),
                dictionaryDataPage(false, repeatedIndices(100, referenced, 0))
            );
            try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
                try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                    OrdinalBytesRefBlock ordinals = block.asOrdinals();
                    assertNotNull(ordinals);
                    assertEquals(referenced * 2 <= dictSize ? referenced : dictSize, ordinals.getDictionaryVector().getPositionCount());
                    String[] expected = new String[100];
                    for (int p = 0; p < expected.length; p++) {
                        expected[p] = entry(p % referenced);
                    }
                    assertValues(block, expected);
                    if (referenced * 2 <= dictSize) {
                        assertFullyReferenced(ordinals);
                    }
                }
            }
        }
    }

    /**
     * Pins the deliberately tuned constants and comparisons with literal values. Changing a constant
     * requires changing this test on purpose, rather than silently changing the reader's contract.
     */
    public void testThresholdLiteralPin() {
        assertEquals(8, PageColumnReader.SKIP_DICTIONARY_RATIO);
        assertEquals(4, PageColumnReader.SHARED_BACKOFF_BATCHES);

        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(dictionaryPage(entries(100)), dictionaryDataPage(false, repeatedIndices(800, 50, 0)));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(800, blockFactory)) {
                assertEquals(
                    "dictSize * 8 == values skips the mark pass",
                    100,
                    block.asOrdinals().getDictionaryVector().getPositionCount()
                );
            }
            assertEquals("the exact skip boundary reserves no scratch", 0, breaker.getUsed());
        }
    }

    /** The skip rule counts non-null values, and still detects a non-null constant batch. */
    public void testSmallDictionarySkipRuleUsesNonNullValues() {
        int dictSize = 8;
        ColumnDescriptor required = column(false);
        PageReader requiredPages = pageReader(
            dictionaryPage(entries(dictSize)),
            dictionaryDataPage(false, repeatedIndices(dictSize * PageColumnReader.SKIP_DICTIONARY_RATIO, 2, 0)),
            dictionaryDataPage(false, repeatedIndices(dictSize * PageColumnReader.SKIP_DICTIONARY_RATIO, 1, 5)),
            dictionaryDataPage(false, repeatedIndices(8, 2, 3))
        );
        try (PageColumnReader reader = new PageColumnReader(requiredPages, required, info(required), null)) {
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(64, blockFactory)) {
                assertEquals(dictSize, block.asOrdinals().getDictionaryVector().getPositionCount());
            }
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(64, blockFactory)) {
                assertNull(block.asOrdinals());
                assertTrue(block.asVector().isConstant());
            }
            assertEquals("skip-only reads reserve no scratch", 0, breaker.getUsed());
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(8, blockFactory)) {
                assertEquals("skip-rule batches do not arm the back-off", 2, block.asOrdinals().getDictionaryVector().getPositionCount());
            }
            assertThat(breaker.getUsed(), greaterThanOrEqualTo(PageColumnReader.compactionScratchBytes(dictSize)));
        }

        ColumnDescriptor optional = column(true);
        Integer[] indices = repeatedIndices(64, 2, 3);
        indices[0] = null;
        PageReader optionalPages = pageReader(dictionaryPage(entries(dictSize)), dictionaryDataPage(true, indices));
        try (PageColumnReader reader = new PageColumnReader(optionalPages, optional, info(optional), null)) {
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(64, blockFactory)) {
                OrdinalBytesRefBlock ordinals = block.asOrdinals();
                assertNotNull(ordinals);
                assertEquals("63 non-null values are below the skip boundary", 2, ordinals.getDictionaryVector().getPositionCount());
                assertFullyReferenced(ordinals);
            }
            assertThat(breaker.getUsed(), greaterThanOrEqualTo(PageColumnReader.compactionScratchBytes(dictSize)));
        }
        assertEquals(0, breaker.getUsed());
    }

    /** Four batches after a mostly-referenced one share; the following probe compacts. */
    public void testSharedDictionaryBackoff() {
        int dictSize = 100;
        List<DataPage> pages = new ArrayList<>();
        pages.add(dictionaryDataPage(false, repeatedIndices(100, 51, 0)));
        for (int batch = 0; batch < PageColumnReader.SHARED_BACKOFF_BATCHES + 1; batch++) {
            pages.add(dictionaryDataPage(false, repeatedIndices(100, 10, 70)));
        }
        ColumnDescriptor desc = column(false);
        try (
            PageColumnReader reader = new PageColumnReader(
                pageReader(dictionaryPage(entries(dictSize)), pages.toArray(DataPage[]::new)),
                desc,
                info(desc),
                null
            )
        ) {
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                assertEquals(dictSize, block.asOrdinals().getDictionaryVector().getPositionCount());
            }
            for (int batch = 0; batch < PageColumnReader.SHARED_BACKOFF_BATCHES; batch++) {
                try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                    assertEquals("back-off batch " + batch, dictSize, block.asOrdinals().getDictionaryVector().getPositionCount());
                }
            }
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                assertEquals(10, block.asOrdinals().getDictionaryVector().getPositionCount());
                assertFullyReferenced(block.asOrdinals());
            }
        }
    }

    /** A probe that shares starts a new window; a constant outcome does not. */
    public void testBackoffRearmsButConstantDoesNotArm() {
        int dictSize = 100;
        List<DataPage> pages = new ArrayList<>();
        pages.add(dictionaryDataPage(false, repeatedIndices(100, 51, 0)));
        for (int batch = 0; batch < PageColumnReader.SHARED_BACKOFF_BATCHES; batch++) {
            pages.add(dictionaryDataPage(false, repeatedIndices(100, 10, 70)));
        }
        pages.add(dictionaryDataPage(false, repeatedIndices(100, 51, 0)));
        pages.add(dictionaryDataPage(false, repeatedIndices(100, 10, 70)));
        ColumnDescriptor desc = column(false);
        try (
            PageColumnReader reader = new PageColumnReader(
                pageReader(dictionaryPage(entries(dictSize)), pages.toArray(DataPage[]::new)),
                desc,
                info(desc),
                null
            )
        ) {
            for (int batch = 0; batch < pages.size(); batch++) {
                try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                    assertEquals("batch " + batch, dictSize, block.asOrdinals().getDictionaryVector().getPositionCount());
                }
            }
        }

        PageReader constantPages = pageReader(
            dictionaryPage(entries(dictSize)),
            dictionaryDataPage(false, repeatedIndices(100, 1, 5)),
            dictionaryDataPage(false, repeatedIndices(100, 10, 70))
        );
        try (PageColumnReader reader = new PageColumnReader(constantPages, desc, info(desc), null)) {
            try (BytesRefBlock constant = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                assertNull(constant.asOrdinals());
            }
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                assertEquals(10, block.asOrdinals().getDictionaryVector().getPositionCount());
            }
        }
    }

    /** Reads that deliberately use the uncompacted path neither consume nor start the back-off. */
    public void testUncompactedReadsDoNotTickBackoff() {
        int dictSize = 100;
        List<DataPage> pages = new ArrayList<>();
        pages.add(dictionaryDataPage(false, repeatedIndices(100, 51, 0)));
        pages.add(dictionaryDataPage(false, repeatedIndices(100, 10, 70)));
        pages.add(dictionaryDataPage(false, repeatedIndices(100, 10, 70)));
        for (int batch = 0; batch < PageColumnReader.SHARED_BACKOFF_BATCHES + 1; batch++) {
            pages.add(dictionaryDataPage(false, repeatedIndices(100, 10, 70)));
        }
        ColumnDescriptor desc = column(false);
        try (
            PageColumnReader reader = new PageColumnReader(
                pageReader(dictionaryPage(entries(dictSize)), pages.toArray(DataPage[]::new)),
                desc,
                info(desc),
                null
            )
        ) {
            reader.readBatch(100, blockFactory).close();
            reader.readBatchFiltered(100, blockFactory, new int[] { 0, 2 }, 2).close();
            reader.readBatchSparse(100, blockFactory, new int[] { 0, 2 }, 2).close();
            for (int batch = 0; batch < PageColumnReader.SHARED_BACKOFF_BATCHES; batch++) {
                try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                    assertEquals("back-off batch " + batch, dictSize, block.asOrdinals().getDictionaryVector().getPositionCount());
                }
            }
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                assertEquals(10, block.asOrdinals().getDictionaryVector().getPositionCount());
            }
        }

        PageReader noArmPages = pageReader(
            dictionaryPage(entries(dictSize)),
            dictionaryDataPage(false, repeatedIndices(100, 10, 70)),
            dictionaryDataPage(false, repeatedIndices(100, 10, 70))
        );
        try (PageColumnReader reader = new PageColumnReader(noArmPages, desc, info(desc), null)) {
            reader.readBatchFiltered(100, blockFactory, new int[] { 0, 2 }, 2).close();
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                assertEquals(
                    "an uncompacted read does not arm the back-off",
                    10,
                    block.asOrdinals().getDictionaryVector().getPositionCount()
                );
            }
        }
    }

    /** Empty and all-null batches do not consume the back-off window. */
    public void testEmptyAndAllNullBatchesDoNotTickBackoff() {
        int dictSize = 100;
        List<DataPage> pages = new ArrayList<>();
        pages.add(dictionaryDataPage(true, repeatedIndices(100, 51, 0)));
        pages.add(dictionaryDataPage(true, new Integer[100]));
        for (int batch = 0; batch < PageColumnReader.SHARED_BACKOFF_BATCHES + 1; batch++) {
            pages.add(dictionaryDataPage(true, repeatedIndices(100, 10, 70)));
        }
        ColumnDescriptor desc = column(true);
        try (
            PageColumnReader reader = new PageColumnReader(
                pageReader(dictionaryPage(entries(dictSize)), pages.toArray(DataPage[]::new)),
                desc,
                info(desc),
                null
            )
        ) {
            reader.readBatch(100, blockFactory).close();
            try (Block empty = reader.readBatch(0, blockFactory)) {
                assertEquals(0, empty.getPositionCount());
            }
            try (Block allNull = reader.readBatch(100, blockFactory)) {
                assertTrue(allNull.areAllValuesNull());
            }
            for (int batch = 0; batch < PageColumnReader.SHARED_BACKOFF_BATCHES; batch++) {
                try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                    assertEquals("back-off batch " + batch, dictSize, block.asOrdinals().getDictionaryVector().getPositionCount());
                }
            }
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                assertEquals(10, block.asOrdinals().getDictionaryVector().getPositionCount());
            }
        }

        PageReader noArmPages = pageReader(
            dictionaryPage(entries(dictSize)),
            dictionaryDataPage(true, new Integer[100]),
            dictionaryDataPage(true, repeatedIndices(100, 10, 70))
        );
        try (PageColumnReader reader = new PageColumnReader(noArmPages, desc, info(desc), null)) {
            reader.readBatch(0, blockFactory).close();
            reader.readBatch(100, blockFactory).close();
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(100, blockFactory)) {
                assertEquals(
                    "empty and all-null reads do not arm the back-off",
                    10,
                    block.asOrdinals().getDictionaryVector().getPositionCount()
                );
            }
        }
    }

    /** With compaction off, a partial batch keeps the whole chunk dictionary and its numbering. */
    public void testCompactionOffKeepsChunkDictionaryAndReservesNothing() {
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(dictionaryPage(entries(100)), dictionaryDataPage(false, 70, 2, 70, 2));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            reader.setOrdinalCompaction(false);
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(4, blockFactory)) {
                OrdinalBytesRefBlock ordinals = block.asOrdinals();
                assertNotNull(ordinals);
                assertEquals(100, ordinals.getDictionaryVector().getPositionCount());
                IntBlock ords = ordinals.getOrdinalsBlock();
                assertEquals(70, ords.getInt(0));
                assertEquals(2, ords.getInt(1));
                assertValues(block, "e00070", "e00002", "e00070", "e00002");
            }
            assertEquals("a reader with compaction off must not reserve the mark scratch", 0, breaker.getUsed());
        }
        assertEquals(0, breaker.getUsed());
    }

    /**
     * Random dictionaries, page splits, nulls and batch sizes: every batch must read back the written
     * values. With compaction on, an ordinal block carries either a compact, fully referenced
     * dictionary in ascending order or the whole chunk dictionary; with it off it always carries the
     * whole chunk dictionary.
     */
    public void testRandomBatchesMatchPlainValues() {
        boolean optional = randomBoolean();
        boolean compaction = randomBoolean();
        int dictSize = between(2, 300);
        int rows = between(1, 600);
        // A random subset of entries is used, clustered or scattered, so batches see partial dictionaries.
        int[] used = randomSubset(dictSize);
        boolean clustered = randomBoolean();
        Integer[] indices = new Integer[rows];
        for (int r = 0; r < rows; r++) {
            if (optional && rarely()) {
                indices[r] = null;
            } else {
                indices[r] = clustered ? used[(int) ((long) r * used.length / rows)] : used[between(0, used.length - 1)];
            }
        }
        List<DataPage> dataPages = new ArrayList<>();
        int start = 0;
        while (start < rows) {
            int end = Math.min(rows, start + between(1, 128));
            dataPages.add(dictionaryDataPage(optional, Arrays.copyOfRange(indices, start, end)));
            start = end;
        }
        ColumnDescriptor desc = column(optional);
        PageReader pages = pageReader(dictionaryPage(entries(dictSize)), dataPages.toArray(DataPage[]::new));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            reader.setOrdinalCompaction(compaction);
            int row = 0;
            while (row < rows) {
                int batch = Math.min(rows - row, between(1, 200));
                try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(batch, blockFactory)) {
                    assertEquals(batch, block.getPositionCount());
                    String[] expected = new String[batch];
                    for (int i = 0; i < batch; i++) {
                        Integer ord = indices[row + i];
                        expected[i] = ord == null ? null : entry(ord);
                    }
                    assertValues(block, expected);
                    OrdinalBytesRefBlock ordinals = block.asOrdinals();
                    if (ordinals != null) {
                        if (compaction) {
                            if (ordinals.getDictionaryVector().getPositionCount() < dictSize) {
                                assertFullyReferenced(ordinals);
                                assertAscending(ordinals.getDictionaryVector());
                            } else {
                                assertEquals(dictSize, ordinals.getDictionaryVector().getPositionCount());
                            }
                        } else {
                            assertEquals(dictSize, ordinals.getDictionaryVector().getPositionCount());
                        }
                    }
                }
                row += batch;
            }
        }
        assertEquals(0, breaker.getUsed());
    }

    /**
     * A batch at the compaction threshold builds its own dictionary; batches above it share the
     * chunk's cached dictionary copy. Block identity can't show the sharing (each batch has its own
     * wrapper), so it is measured on a breaker that sees the {@code BigArrays} bytes of the dictionary,
     * with entries large enough to dominate everything else a batch allocates.
     */
    public void testThresholdBatchesCompactAndMostlyReferencedBatchesShareDictionary() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(64)).withCircuitBreaking();
        CircuitBreaker trackingBreaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory trackingFactory = BlockFactory.builder(bigArrays).breaker(trackingBreaker).build();
        int entryBytes = 4096;
        String[] dict = new String[8];
        for (int i = 0; i < dict.length; i++) {
            dict[i] = Character.toString('a' + i).repeat(entryBytes);
        }
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(
            dictionaryPage(dict),
            dictionaryDataPage(false, 2, 5, 2, 5, 2, 5, 2, 5),
            dictionaryDataPage(false, 0, 1, 2, 3, 4, 0, 1, 2),
            dictionaryDataPage(false, 4, 3, 2, 1, 0, 4, 3, 2)
        );
        List<Block> blocks = new ArrayList<>();
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            try {
                long before = trackingBreaker.getUsed();
                blocks.add(reader.readBatch(8, trackingFactory));
                long afterFirst = trackingBreaker.getUsed();
                blocks.add(reader.readBatch(8, trackingFactory));
                long afterSecond = trackingBreaker.getUsed();
                blocks.add(reader.readBatch(8, trackingFactory));
                long afterThird = trackingBreaker.getUsed();

                assertDictionarySize(blocks.get(0), 2);
                assertFullyReferenced(((BytesRefBlock) blocks.get(0)).asOrdinals());
                assertDictionarySize(blocks.get(1), 8);
                assertDictionarySize(blocks.get(2), 8);
                assertThat("the compact batch copies its two entries", afterFirst - before, greaterThanOrEqualTo(2L * entryBytes));
                assertThat("the compact batch copies only its entries", afterFirst - before, lessThan(8L * entryBytes));
                assertThat(
                    "the first shared batch copies the whole dictionary",
                    afterSecond - afterFirst,
                    greaterThanOrEqualTo(8L * entryBytes)
                );
                assertThat("the next shared batch reuses it", afterThird - afterSecond, lessThan((long) entryBytes));
            } finally {
                Releasables.close(blocks);
            }
        }
        assertEquals(0, trackingBreaker.getUsed());
    }

    /** A few entries holding most of the payload share the chunk dictionary instead of copying it per batch. */
    public void testWideReferencedEntriesKeepSharedDictionary() {
        String[] dict = entries(8);
        dict[0] = "x".repeat(4096);
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(dictionaryPage(dict), dictionaryDataPage(false, 0, 1, 0, 1, 0, 1));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(6, blockFactory)) {
                assertDictionarySize(block, dict.length);
                assertValues(block, dict[0], dict[1], dict[0], dict[1], dict[0], dict[1]);
            }
        }
        assertEquals(0, breaker.getUsed());
    }

    /** Consecutive batches referencing the same subset share one compact dictionary vector. */
    public void testRepeatedCompactSubsetReusesDictionary() {
        int dictSize = 8;
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(
            dictionaryPage(entries(dictSize)),
            dictionaryDataPage(false, repeatedIndices(8, 4, 0)),
            dictionaryDataPage(false, repeatedIndices(8, 4, 0))
        );
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            try (
                BytesRefBlock first = (BytesRefBlock) reader.readBatch(8, blockFactory);
                BytesRefBlock second = (BytesRefBlock) reader.readBatch(8, blockFactory)
            ) {
                assertDictionarySize(first, 4);
                assertDictionarySize(second, 4);
                assertSame(first.asOrdinals().getDictionaryVector(), second.asOrdinals().getDictionaryVector());
            }
        }
        assertEquals(0, breaker.getUsed());
    }

    /** Replacing the reuse cache releases its old dictionary before allocating the new one. */
    public void testReplacingCachedCompactDictionaryDoesNotTemporarilyRetainBoth() {
        byte[][] dict = new byte[8][];
        for (int i = 0; i < dict.length; i++) {
            dict[i] = String.valueOf((char) ('a' + i)).repeat(64 * 1024).getBytes(StandardCharsets.UTF_8);
        }
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(dictionaryPage(dict), dictionaryDataPage(false, 0, 1, 0, 1), dictionaryDataPage(false, 2, 3, 2, 3));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            long firstDictionaryBytes;
            try (BytesRefBlock first = (BytesRefBlock) reader.readBatch(4, blockFactory)) {
                assertDictionarySize(first, 2);
                firstDictionaryBytes = first.asOrdinals().getDictionaryVector().ramBytesUsed();
            }
            breaker.resetPeak();
            try (BytesRefBlock second = (BytesRefBlock) reader.readBatch(4, blockFactory)) {
                assertDictionarySize(second, 2);
            }
            long retainedAfterReplacement = breaker.getUsed();
            assertThat(
                "replacement peak must not include the old dictionary",
                breaker.peakUsed() - retainedAfterReplacement,
                lessThan(firstDictionaryBytes)
            );
        }
        assertEquals(0, breaker.getUsed());
    }

    /**
     * With every emitted block closed and the reader still open, the breaker holds exactly the
     * compaction scratch and the last compact dictionary retained for reuse; closing the reader
     * releases both, and closing twice changes nothing.
     */
    public void testMarkScratchIsChargedUntilClose() {
        int dictSize = between(65, 1000);
        int words = (dictSize + 63) / 64;
        long expected = RamUsageEstimator.sizeOf(new long[4 * words]) + RamUsageEstimator.sizeOf(new long[words]) + RamUsageEstimator
            .sizeOf(new int[words]) + RamUsageEstimator.shallowSizeOf(new BytesRef[dictSize]);
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(
            dictionaryPage(entries(dictSize)),
            dictionaryDataPage(false, 1, 2, 1, 2),
            dictionaryDataPage(false, dictSize - 1, 0, dictSize - 1, 0)
        );
        PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null);
        try {
            reader.readBatch(4, blockFactory).close();
            reader.readBatch(4, blockFactory).close();
            assertThat(breaker.getUsed(), greaterThanOrEqualTo(expected));
        } finally {
            reader.close();
        }
        assertEquals(0, breaker.getUsed());
        reader.close();
        assertEquals(0, breaker.getUsed());
    }

    /**
     * A trip while reserving the mark scratch propagates as is (a 429, not wrapped into a 400 or 500),
     * leaves nothing charged, and the reader still closes cleanly.
     */
    public void testMarkScratchBreakerTripPropagates() {
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(dictionaryPage(entries(8)), dictionaryDataPage(false, 1, 2, 1, 2));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            breaker.tripOn(OrdinalCompaction.SCRATCH_LABEL);
            CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> reader.readBatch(4, blockFactory));
            assertSame(e, ParquetReadFailures.wrap(e, "Failed to read Parquet column"));
            assertEquals(0, breaker.getUsed());
        }
        assertEquals(0, breaker.getUsed());
    }

    /** Compact dictionary payload arrays are reserved before allocation and refunded when that reservation trips. */
    public void testCompactDictionaryReservationTripPropagates() {
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(dictionaryPage(entries(8)), dictionaryDataPage(false, 1, 2, 1, 2));
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            breaker.tripOn(PageColumnReader.COMPACT_DICTIONARY_LABEL);
            expectThrows(CircuitBreakingException.class, () -> reader.readBatch(4, blockFactory));
            assertEquals(PageColumnReader.compactionScratchBytes(8), breaker.getUsed());
        }
        assertEquals(0, breaker.getUsed());
    }

    /** Repaired UTF-8 bytes retained in the per-reader sanitized-entry cache stay charged until close. */
    public void testRepairedDictionaryEntriesAreChargedUntilClose() {
        byte[][] dict = {
            "zero".getBytes(StandardCharsets.UTF_8),
            { (byte) 0xFF },
            "ok".getBytes(StandardCharsets.UTF_8),
            "three".getBytes(StandardCharsets.UTF_8),
            "four".getBytes(StandardCharsets.UTF_8),
            "five".getBytes(StandardCharsets.UTF_8),
            "six".getBytes(StandardCharsets.UTF_8),
            "seven".getBytes(StandardCharsets.UTF_8) };
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(dictionaryPage(dict), dictionaryDataPage(false, 1, 2, 1, 2));
        long repairedBytes = RamUsageEstimator.shallowSizeOf(new BytesRef()) + RamUsageEstimator.sizeOf(new byte[3]);
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            long dictionaryBytes;
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(4, blockFactory)) {
                assertValues(block, "\uFFFD", "ok", "\uFFFD", "ok");
                dictionaryBytes = block.asOrdinals().getDictionaryVector().ramBytesUsed();
            }
            assertEquals(PageColumnReader.compactionScratchBytes(dict.length) + repairedBytes + dictionaryBytes, breaker.getUsed());
        }
        assertEquals(0, breaker.getUsed());
    }

    /**
     * A failure after the new ordinals block was built (here the compact dictionary allocation
     * trips) must close that block and not leave marks behind for the next batch. Stale marks would
     * add the first batch's entries to the second batch's dictionary.
     */
    public void testFailureAfterMarkPassLeavesNoStaleMarks() {
        ColumnDescriptor desc = column(false);
        PageReader pages = pageReader(
            dictionaryPage(entries(8)),
            dictionaryDataPage(false, 0, 1, 0, 1, 0, 1),
            dictionaryDataPage(false, 5, 6, 5, 6, 5, 6)
        );
        try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
            breaker.tripOnBlockAllocation(2);
            expectThrows(CircuitBreakingException.class, () -> reader.readBatch(6, blockFactory));
            assertEquals(PageColumnReader.compactionScratchBytes(8), breaker.getUsed());
            try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(6, blockFactory)) {
                OrdinalBytesRefBlock ordinals = block.asOrdinals();
                assertNotNull(ordinals);
                assertDictionary(ordinals, "e00005", "e00006");
                assertValues(block, "e00005", "e00006", "e00005", "e00006", "e00005", "e00006");
            }
        }
        assertEquals(0, breaker.getUsed());
    }

    /** An out-of-range dictionary index is bad input (400-class), whatever the compaction toggle. */
    public void testOutOfRangeDictionaryIndexIsBadInput() {
        for (boolean compaction : new boolean[] { true, false }) {
            ColumnDescriptor desc = column(false);
            PageReader pages = pageReader(dictionaryPage(entries(4)), dictionaryDataPage(false, 1, 9, 2));
            try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
                reader.setOrdinalCompaction(compaction);
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> reader.readBatch(3, blockFactory));
                assertThat(e.getMessage(), containsString("Dictionary index [9] out of range for dictionary of size [4]"));
                assertThat(e.getMessage(), containsString("[" + COLUMN + "]"));
            }
            assertEquals(0, breaker.getUsed());
        }
    }

    /**
     * Control for the corrupt variants below: a batch spanning a dictionary-encoded page and a PLAIN
     * page resolves the dictionary prefix and reads the rest as plain values, which is the
     * {@code finishMaterializedFallback} path.
     */
    public void testDictionaryThenPlainPageFallback() {
        for (boolean optional : new boolean[] { false, true }) {
            ColumnDescriptor desc = column(optional);
            PageReader pages = pageReader(
                dictionaryPage(entries(4)),
                dictionaryDataPage(optional, 0, 3, 1),
                plainDataPage(optional, "p0", "p1")
            );
            try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
                try (BytesRefBlock block = (BytesRefBlock) reader.readBatch(5, blockFactory)) {
                    assertNull("the fallback emits a plain block", block.asOrdinals());
                    assertValues(block, "e00000", "e00003", "e00001", "p0", "p1");
                }
            }
        }
    }

    /**
     * An out-of-range index in the dictionary-encoded prefix of a batch that falls back to PLAIN must
     * fail as bad input too. The fallback resolves the prefix through the dictionary itself, so a check
     * placed only in the compaction pass would miss it and surface an
     * {@link ArrayIndexOutOfBoundsException}.
     */
    public void testOutOfRangeIndexBeforePlainFallbackIsBadInput() {
        // required column; nullable with the corrupt index after a null (decoded into a packed array and
        // scattered); nullable without nulls in the page (decoded straight into the ordinals)
        Integer[][] prefixes = { { 0, 9, 1 }, { null, 9, 1 }, { 2, 9, 1 } };
        boolean[] optionals = { false, true, true };
        for (int v = 0; v < prefixes.length; v++) {
            ColumnDescriptor desc = column(optionals[v]);
            PageReader pages = pageReader(
                dictionaryPage(entries(4)),
                dictionaryDataPage(optionals[v], prefixes[v]),
                plainDataPage(optionals[v], "p0", "p1")
            );
            try (PageColumnReader reader = new PageColumnReader(pages, desc, info(desc), null)) {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> reader.readBatch(5, blockFactory));
                assertThat("variant " + v, e.getMessage(), containsString("Dictionary index [9] out of range for dictionary of size [4]"));
            }
            assertEquals(0, breaker.getUsed());
        }
    }

    // --- OrdinalCompaction.compact ---

    public void testCompactPartialBlock() {
        BytesRefBlock source = ordinalBlock(new String[] { "a", "b", "c", "d" }, 3, 0, 3, null, 0);
        Block result = OrdinalCompaction.compact(source, blockFactory);
        try {
            assertNotSame(source, result);
            assertTrue("source is closed on success", source.isReleased());
            OrdinalBytesRefBlock ordinals = ((BytesRefBlock) result).asOrdinals();
            assertNotNull(ordinals);
            assertDictionary(ordinals, "a", "d");
            assertValues((BytesRefBlock) result, "d", "a", "d", null, "a");
        } finally {
            result.close();
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testCompactUsesShareAndSkipThresholds() {
        try (BytesRefBlock aboveHalf = ordinalBlock(entries(10), 0, 1, 2, 3, 4, 5)) {
            assertSame(aboveHalf, OrdinalCompaction.compact(aboveHalf, blockFactory));
            assertFalse(aboveHalf.isReleased());
        }

        Integer[] manyValues = repeatedIndices(16, 1, 1);
        try (BytesRefBlock smallDictionary = ordinalBlock(entries(2), manyValues)) {
            assertSame(smallDictionary, OrdinalCompaction.compact(smallDictionary, blockFactory));
            assertFalse(smallDictionary.isReleased());
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testCompactKeepsDictionaryWhenReferencedEntriesHoldMostPayloadBytes() {
        String[] dictionary = entries(8);
        dictionary[0] = "x".repeat(4096);
        try (BytesRefBlock source = ordinalBlock(dictionary, 0, 1, 0, 1)) {
            assertSame(source, OrdinalCompaction.compact(source, blockFactory));
            assertFalse(source.isReleased());
        }
        assertEquals(0, breaker.getUsed());
    }

    /** The helper's skip rule uses non-null values rather than positions. */
    public void testCompactSkipRuleCountsNonNullValues() {
        Integer[] ordinals = repeatedIndices(64, 2, 3);
        ordinals[0] = null;
        BytesRefBlock source = ordinalBlock(entries(8), ordinals);
        Block result = OrdinalCompaction.compact(source, blockFactory);
        try {
            assertNotSame(source, result);
            assertTrue(source.isReleased());
            OrdinalBytesRefBlock compacted = ((BytesRefBlock) result).asOrdinals();
            assertNotNull(compacted);
            assertEquals(2, compacted.getDictionaryVector().getPositionCount());
            assertFullyReferenced(compacted);
        } finally {
            result.close();
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testCompactLeavesFullyReferencedAndNonOrdinalBlocks() {
        try (BytesRefBlock full = ordinalBlock(new String[] { "a", "b" }, 1, 0, null, 1)) {
            assertSame(full, OrdinalCompaction.compact(full, blockFactory));
        }
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.appendBytesRef(new BytesRef("a")).appendNull();
            try (BytesRefBlock plain = builder.build()) {
                assertSame(plain, OrdinalCompaction.compact(plain, blockFactory));
            }
        }
        try (Block ints = blockFactory.newConstantIntBlockWith(1, 3)) {
            assertSame(ints, OrdinalCompaction.compact(ints, blockFactory));
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testCompactAllNullBlock() {
        BytesRefBlock source = ordinalBlock(new String[] { "a", "b" }, null, null, null);
        Block result = OrdinalCompaction.compact(source, blockFactory);
        try {
            assertTrue(source.isReleased());
            assertTrue(result.areAllValuesNull());
            assertEquals(3, result.getPositionCount());
        } finally {
            result.close();
        }
        assertEquals(0, breaker.getUsed());
    }

    /** On a breaker trip the caller still owns the source, untouched, and the scratch is released. */
    public void testCompactBreakerTripKeepsSource() {
        for (boolean tripOnScratch : new boolean[] { true, false }) {
            try (BytesRefBlock source = ordinalBlock(new String[] { "a", "b", "c", "d" }, 3, 0, 3, null, 0)) {
                long sourceBytes = breaker.getUsed();
                if (tripOnScratch) {
                    breaker.tripOn(OrdinalCompaction.SCRATCH_LABEL);
                } else {
                    breaker.tripOnNextBlockAllocation();
                }
                expectThrows(CircuitBreakingException.class, () -> OrdinalCompaction.compact(source, blockFactory));
                assertFalse(source.isReleased());
                assertEquals("tripOnScratch=" + tripOnScratch, sourceBytes, breaker.getUsed());
                assertValues(source, "d", "a", "d", null, "a");
            }
            assertEquals(0, breaker.getUsed());
        }
    }

    /** A dictionary-builder failure closes the newly built ordinals and leaves the source owned by the caller. */
    public void testCompactDictionaryFailureKeepsSource() {
        try (BytesRefBlock source = ordinalBlock(new String[] { "a", "b", "c", "d" }, 3, 0, 3, null, 0)) {
            long sourceBytes = breaker.getUsed();
            breaker.tripOnBlockAllocation(3);
            expectThrows(CircuitBreakingException.class, () -> OrdinalCompaction.compact(source, blockFactory));
            assertFalse(source.isReleased());
            assertEquals(sourceBytes, breaker.getUsed());
            assertValues(source, "d", "a", "d", null, "a");
        }
        assertEquals(0, breaker.getUsed());
    }

    // --- assertions ---

    /** Every dictionary entry is referenced by at least one non-null position. */
    static void assertFullyReferenced(OrdinalBytesRefBlock block) {
        IntBlock ordinals = block.getOrdinalsBlock();
        boolean[] referenced = new boolean[block.getDictionaryVector().getPositionCount()];
        for (int p = 0; p < ordinals.getPositionCount(); p++) {
            int first = ordinals.getFirstValueIndex(p);
            for (int v = 0; v < ordinals.getValueCount(p); v++) {
                referenced[ordinals.getInt(first + v)] = true;
            }
        }
        for (int i = 0; i < referenced.length; i++) {
            assertTrue("dictionary entry [" + i + "] of [" + referenced.length + "] is not referenced", referenced[i]);
        }
    }

    private static void assertAscending(BytesRefVector dictionary) {
        BytesRef previous = null;
        for (int i = 0; i < dictionary.getPositionCount(); i++) {
            BytesRef current = BytesRef.deepCopyOf(dictionary.getBytesRef(i, new BytesRef()));
            if (previous != null) {
                assertThat("dictionary order at " + i, previous.compareTo(current), lessThan(0));
            }
            previous = current;
        }
    }

    private static void assertDictionary(OrdinalBytesRefBlock block, String... expected) {
        assertFullyReferenced(block);
        BytesRefVector dictionary = block.getDictionaryVector();
        BytesRef scratch = new BytesRef();
        String[] actual = new String[dictionary.getPositionCount()];
        for (int i = 0; i < actual.length; i++) {
            actual[i] = dictionary.getBytesRef(i, scratch).utf8ToString();
        }
        assertArrayEquals(expected, actual);
    }

    private static void assertDictionarySize(Block block, int expected) {
        OrdinalBytesRefBlock ordinals = ((BytesRefBlock) block).asOrdinals();
        assertNotNull(ordinals);
        assertEquals(expected, ordinals.getDictionaryVector().getPositionCount());
    }

    private static void assertValues(BytesRefBlock block, String... expected) {
        assertEquals(expected.length, block.getPositionCount());
        BytesRef scratch = new BytesRef();
        for (int p = 0; p < expected.length; p++) {
            if (expected[p] == null) {
                assertTrue("expected null at " + p, block.isNull(p));
            } else {
                assertFalse("unexpected null at " + p, block.isNull(p));
                assertEquals("value at " + p, expected[p], block.getBytesRef(block.getFirstValueIndex(p), scratch).utf8ToString());
            }
        }
    }

    // --- fixtures ---

    private BytesRefBlock ordinalBlock(String[] dictionary, Integer... ordinals) {
        IntBlock ords = null;
        BytesRefVector dict = null;
        try {
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(ordinals.length)) {
                for (Integer ord : ordinals) {
                    if (ord == null) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(ord);
                    }
                }
                ords = builder.build();
            }
            try (BytesRefVector.Builder builder = blockFactory.newBytesRefVectorBuilder(dictionary.length)) {
                for (String entry : dictionary) {
                    builder.appendBytesRef(new BytesRef(entry));
                }
                dict = builder.build();
            }
            return new OrdinalBytesRefBlock(ords, dict);
        } catch (RuntimeException e) {
            Releasables.closeExpectNoException(ords, dict);
            throw e;
        }
    }

    private static String entry(int ord) {
        return String.format(Locale.ROOT, "e%05d", ord);
    }

    private static String[] entries(int size) {
        String[] out = new String[size];
        for (int i = 0; i < size; i++) {
            out[i] = entry(i);
        }
        return out;
    }

    /** Exactly {@code referenced} ordinals, starting at {@code first}, repeated over {@code positions}. */
    private static Integer[] repeatedIndices(int positions, int referenced, int first) {
        Integer[] out = new Integer[positions];
        for (int p = 0; p < positions; p++) {
            out[p] = first + p % referenced;
        }
        return out;
    }

    /** Ascending, non-empty random subset of {@code [0, size)}. */
    private static int[] randomSubset(int size) {
        int count = between(1, size);
        List<Integer> all = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            all.add(i);
        }
        return randomSubsetOf(count, all).stream().mapToInt(Integer::intValue).sorted().toArray();
    }

    private static ColumnDescriptor column(boolean optional) {
        PrimitiveType type = (optional ? Types.optional(BINARY) : Types.required(BINARY)).as(LogicalTypeAnnotation.stringType())
            .named(COLUMN);
        return new ColumnDescriptor(new String[] { COLUMN }, type, 0, optional ? 1 : 0);
    }

    private static ColumnInfo info(ColumnDescriptor desc) {
        return new ColumnInfo(
            desc,
            BINARY,
            DataType.KEYWORD,
            desc.getMaxDefinitionLevel(),
            desc.getMaxRepetitionLevel(),
            desc.getPrimitiveType().getLogicalTypeAnnotation()
        );
    }

    private static PageReader pageReader(DictionaryPage dictionary, DataPage... pages) {
        ArrayDeque<DataPage> queue = new ArrayDeque<>(Arrays.asList(pages));
        long total = Arrays.stream(pages).mapToLong(DataPage::getValueCount).sum();
        return new PageReader() {
            @Override
            public DictionaryPage readDictionaryPage() {
                return dictionary;
            }

            @Override
            public long getTotalValueCount() {
                return total;
            }

            @Override
            public DataPage readPage() {
                return queue.poll();
            }
        };
    }

    /** A PLAIN-encoded dictionary page of BINARY entries. */
    private static DictionaryPage dictionaryPage(String... entries) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writePlainBinaries(out, entries);
        return new DictionaryPage(BytesInput.from(out.toByteArray()), entries.length, Encoding.PLAIN_DICTIONARY);
    }

    /** A PLAIN-encoded dictionary page whose entries may contain malformed UTF-8. */
    private static DictionaryPage dictionaryPage(byte[]... entries) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writePlainBinaries(out, entries);
        return new DictionaryPage(BytesInput.from(out.toByteArray()), entries.length, Encoding.PLAIN_DICTIONARY);
    }

    /** A V1 dictionary-encoded data page; a {@code null} index is a null slot (optional columns only). */
    private static DataPageV1 dictionaryDataPage(boolean optional, Integer... indices) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (optional) {
                writeDefinitionLevels(out, indices);
            }
            int[] nonNull = Arrays.stream(indices).filter(i -> i != null).mapToInt(Integer::intValue).toArray();
            int bitWidth = Math.max(1, BytesUtils.getWidthFromMaxInt(Arrays.stream(nonNull).max().orElse(0)));
            out.write(bitWidth);
            RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(
                bitWidth,
                64,
                1024,
                HeapByteBufferAllocator.getInstance()
            );
            for (int index : nonNull) {
                encoder.writeInt(index);
            }
            encoder.toBytes().writeAllTo(out);
            return dataPage(out.toByteArray(), indices.length, Encoding.RLE_DICTIONARY);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** A V1 PLAIN-encoded data page of BINARY values; a {@code null} value is a null slot. */
    private static DataPageV1 plainDataPage(boolean optional, String... values) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (optional) {
                writeDefinitionLevels(out, values);
            }
            writePlainBinaries(out, Arrays.stream(values).filter(v -> v != null).toArray(String[]::new));
            return dataPage(out.toByteArray(), values.length, Encoding.PLAIN);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static DataPageV1 dataPage(byte[] bytes, int valueCount, Encoding valuesEncoding) {
        PrimitiveType type = Types.optional(BINARY).named(COLUMN);
        return new DataPageV1(
            BytesInput.from(bytes),
            valueCount,
            bytes.length,
            Statistics.getBuilderForReading(type).build(),
            Encoding.RLE,
            Encoding.RLE,
            valuesEncoding
        );
    }

    /** V1 definition levels: a 4-byte little-endian length, then the RLE / bit-packed hybrid levels. */
    private static void writeDefinitionLevels(ByteArrayOutputStream out, Object[] values) throws IOException {
        RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(1, 64, 1024, HeapByteBufferAllocator.getInstance());
        for (Object value : values) {
            encoder.writeInt(value == null ? 0 : 1);
        }
        BytesInput levels = encoder.toBytes();
        BytesUtils.writeIntLittleEndian(out, Math.toIntExact(levels.size()));
        levels.writeAllTo(out);
    }

    private static void writePlainBinaries(ByteArrayOutputStream out, String[] values) {
        try {
            for (String value : values) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                BytesUtils.writeIntLittleEndian(out, bytes.length);
                out.write(bytes);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void writePlainBinaries(ByteArrayOutputStream out, byte[][] values) {
        try {
            for (byte[] value : values) {
                BytesUtils.writeIntLittleEndian(out, value.length);
                out.write(value);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Tracks charged bytes and trips on demand: once on the next reservation with a given label, or
     * once on a selected block allocation (any reservation other than the mark scratch).
     */
    private static final class TestBreaker extends NoopCircuitBreaker {
        private final AtomicLong used = new AtomicLong();
        private final AtomicLong peakUsed = new AtomicLong();
        private String tripLabel;
        private int blockAllocationsUntilTrip;

        TestBreaker() {
            super(CircuitBreaker.REQUEST);
        }

        void tripOn(String label) {
            tripLabel = label;
        }

        void tripOnNextBlockAllocation() {
            tripOnBlockAllocation(1);
        }

        void tripOnBlockAllocation(int allocation) {
            assert allocation > 0;
            blockAllocationsUntilTrip = allocation;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            boolean blockAllocation = bytes > 0 && label.equals(OrdinalCompaction.SCRATCH_LABEL) == false;
            boolean tripBlockAllocation = blockAllocation && blockAllocationsUntilTrip > 0 && --blockAllocationsUntilTrip == 0;
            boolean trip = bytes > 0 && (label.equals(tripLabel) || tripBlockAllocation);
            if (trip) {
                tripLabel = null;
                blockAllocationsUntilTrip = 0;
                throw new CircuitBreakingException("test trip on [" + label + "]", bytes, getLimit(), Durability.TRANSIENT);
            }
            updateUsed(bytes);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            updateUsed(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }

        void resetPeak() {
            peakUsed.set(used.get());
        }

        long peakUsed() {
            return peakUsed.get();
        }

        private void updateUsed(long bytes) {
            long current = used.addAndGet(bytes);
            peakUsed.accumulateAndGet(current, Math::max);
        }
    }
}
