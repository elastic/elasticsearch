/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.ToXContent;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * The rows of a page as NDJSON lines, the part that {@code POST /_query/stream} and {@code GET /_query/stream/{id}} share. The pages
 * come from a circuit breaking {@link BlockFactory}, and every test checks that the breaker is back to zero.
 */
public class NdjsonRowWriterTests extends ESTestCase {

    private static final List<ColumnInfoImpl> ONE_LONG = List.of(new ColumnInfoImpl("n", "long", null));

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking()
        ).build();
    }

    @After
    public void checkBreaker() {
        assertThat("every page must be released", blockFactory.breaker().getUsed(), equalTo(0L));
    }

    /** A page with one long column whose values are {@code 0..rows-1}. */
    private Page longs(int rows) {
        long[] values = new long[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = i;
        }
        return new Page(blockFactory.newLongArrayVector(values, rows).asBlock());
    }

    /** One call of {@link NdjsonRowWriter#writeRows}, returning the lines it wrote. */
    private static List<String> writeChunk(NdjsonRowWriter writer, int sizeHint, NdjsonRowWriter.RowLimit limit) throws IOException {
        try (RecyclerBytesStreamOutput chunk = new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            int rows = writer.writeRows(chunk, ToXContent.EMPTY_PARAMS, sizeHint, limit);
            String text = chunk.bytes().utf8ToString();
            if (rows == 0) {
                assertThat("no row is written, so no byte is written either", text, equalTo(""));
                return List.of();
            }
            assertTrue("every row ends with a newline", text.endsWith("\n"));
            // Only the newline of the last row is removed, and empty lines are kept (the -1), so a blank line anywhere in the chunk
            // is counted and fails the check below.
            List<String> lines = List.of(text.substring(0, text.length() - 1).split("\n", -1));
            assertThat("the returned count is the number of lines written", rows, equalTo(lines.size()));
            for (String line : lines) {
                // a row is a JSON array, so a line is never empty: a blank line must not be counted as a row
                assertThat("every line is a JSON array", line, allOf(startsWith("["), endsWith("]")));
            }
            return lines;
        }
    }

    /** Writes the whole page in one chunk. */
    private static List<String> writeAll(NdjsonRowWriter writer) throws IOException {
        List<String> lines = writeChunk(writer, Integer.MAX_VALUE, NdjsonRowWriter.RowLimit.NONE);
        assertTrue(writer.allRowsWritten());
        return lines;
    }

    // ---- the lines ----

    public void testEachRowIsAJsonArrayInColumnOrder() throws IOException {
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("n", "long", null), new ColumnInfoImpl("k", "keyword", null));
        try (
            LongBlock.Builder longs = blockFactory.newLongBlockBuilder(3);
            BytesRefBlock.Builder keywords = blockFactory.newBytesRefBlockBuilder(3)
        ) {
            longs.appendLong(1).appendNull().appendLong(3);
            keywords.appendBytesRef(new BytesRef("a")).appendBytesRef(new BytesRef("b")).appendNull();
            Page page = new Page(longs.build(), keywords.build());
            try {
                NdjsonRowWriter writer = new NdjsonRowWriter(page, columns, null, ZoneOffset.UTC);
                assertThat(writeAll(writer), equalTo(List.of("[1,\"a\"]", "[null,\"b\"]", "[3,null]")));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testNullColumnsAreLeftOut() throws IOException {
        List<ColumnInfoImpl> columns = List.of(
            new ColumnInfoImpl("a", "long", null),
            new ColumnInfoImpl("dropped", "long", null),
            new ColumnInfoImpl("c", "long", null)
        );
        Page page = new Page(
            blockFactory.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock(),
            blockFactory.newConstantNullBlock(2),
            blockFactory.newLongArrayVector(new long[] { 10, 20 }, 2).asBlock()
        );
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, columns, new boolean[] { false, true, false }, ZoneOffset.UTC);
            assertThat(writeAll(writer), equalTo(List.of("[1,10]", "[2,20]")));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testDatesAreWrittenInTheGivenZone() throws IOException {
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("d", "date", null));
        long millis = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
        Page page = new Page(blockFactory.newLongArrayVector(new long[] { millis }, 1).asBlock());
        try {
            assertThat(
                writeAll(new NdjsonRowWriter(page, columns, null, ZoneOffset.UTC)),
                equalTo(List.of("[\"2024-01-01T00:00:00.000Z\"]"))
            );
            assertThat(
                writeAll(new NdjsonRowWriter(page, columns, null, ZoneId.of("+02:00"))),
                equalTo(List.of("[\"2024-01-01T02:00:00.000+02:00\"]"))
            );
        } finally {
            page.releaseBlocks();
        }
    }

    /** A multivalued field is a JSON array inside the row, and a single value is written bare. */
    public void testMultivaluedFieldsAreArraysInTheRow() throws IOException {
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("n", "long", null));
        try (LongBlock.Builder longs = blockFactory.newLongBlockBuilder(3)) {
            longs.beginPositionEntry().appendLong(1).appendLong(2).appendLong(3).endPositionEntry();
            longs.appendLong(4);
            longs.appendNull();
            Page page = new Page(longs.build());
            try {
                NdjsonRowWriter writer = new NdjsonRowWriter(page, columns, null, ZoneOffset.UTC);
                assertThat(writeAll(writer), equalTo(List.of("[[1,2,3]]", "[4]", "[null]")));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /** A row is always one line: a newline inside a value is escaped, and other characters are written as they are. */
    public void testValuesWithNewlinesQuotesAndUnicodeStayOnOneLine() throws IOException {
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("k", "keyword", null));
        try (BytesRefBlock.Builder keywords = blockFactory.newBytesRefBlockBuilder(2)) {
            keywords.appendBytesRef(new BytesRef("a\"b\nc\r\nd"));
            keywords.appendBytesRef(new BytesRef("caf\u00e9 \u2603"));
            Page page = new Page(keywords.build());
            try {
                NdjsonRowWriter writer = new NdjsonRowWriter(page, columns, null, ZoneOffset.UTC);
                // writeChunk checks that the number of lines is the number of rows, so a raw newline in a value would fail it
                assertThat(writeAll(writer), equalTo(List.of("[\"a\\\"b\\nc\\r\\nd\"]", "[\"caf\u00e9 \u2603\"]")));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /** When every column is left out each row is an empty array, so the number of lines is still the number of rows. */
    public void testWhenEveryColumnIsLeftOutEachRowIsAnEmptyArray() throws IOException {
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("a", "long", null), new ColumnInfoImpl("b", "long", null));
        Page page = new Page(blockFactory.newConstantNullBlock(3), blockFactory.newConstantNullBlock(3));
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, columns, new boolean[] { true, true }, ZoneOffset.UTC);
            assertThat(writeAll(writer), equalTo(List.of("[]", "[]", "[]")));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testAPageWithoutRowsWritesNothing() throws IOException {
        Page page = longs(0);
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, ONE_LONG, null, ZoneOffset.UTC);
            assertTrue("there is nothing to write", writer.allRowsWritten());
            assertThat(writeAll(writer), equalTo(List.of()));
            assertFalse(writer.stoppedAtLimit());
        } finally {
            page.releaseBlocks();
        }
    }

    // ---- a page written over several chunks ----

    public void testContinuesWhereThePreviousChunkStopped() throws IOException {
        int rows = between(1, 200);
        // a row of this page is at most 6 bytes, so a small hint needs several chunks and a large one needs a single chunk
        int sizeHint = between(1, 2000);
        Page page = longs(rows);
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, ONE_LONG, null, ZoneOffset.UTC);
            List<String> lines = new ArrayList<>();
            while (writer.allRowsWritten() == false) {
                List<String> chunk = writeChunk(writer, sizeHint, NdjsonRowWriter.RowLimit.NONE);
                assertThat("a chunk makes progress whatever the size hint", chunk.size(), greaterThanOrEqualTo(1));
                lines.addAll(chunk);
            }
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                expected.add("[" + i + "]");
            }
            assertThat(lines, equalTo(expected));
            assertThat(
                "nothing is left after the last row",
                writeChunk(writer, sizeHint, NdjsonRowWriter.RowLimit.NONE),
                equalTo(List.of())
            );
        } finally {
            page.releaseBlocks();
        }
    }

    public void testAChunkStopsAtTheFirstRowThatReachesTheSizeHint() throws IOException {
        Page page = longs(10);
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, ONE_LONG, null, ZoneOffset.UTC);
            // "[0]\n" is 4 bytes: a hint of 1 is reached by the first row, a hint of 9 by the third
            assertThat(writeChunk(writer, 1, NdjsonRowWriter.RowLimit.NONE), equalTo(List.of("[0]")));
            assertThat(writeChunk(writer, 9, NdjsonRowWriter.RowLimit.NONE), equalTo(List.of("[1]", "[2]", "[3]")));
            assertFalse(writer.allRowsWritten());
        } finally {
            page.releaseBlocks();
        }
    }

    // ---- a caller with limits ----

    /** A limit reached once a call has written {@code maxRows} rows. The writer reports the rows of the current call only. */
    private static NdjsonRowWriter.RowLimit afterRows(int maxRows) {
        return (rowsInChunk, bytesInChunk) -> rowsInChunk >= maxRows;
    }

    /**
     * A limit on the whole response, applied across chunks the way the listener does: the caller keeps the total of the chunks that are
     * done and the limit adds the rows of the chunk being written to it.
     */
    public void testALimitAppliesAcrossChunks() throws IOException {
        Page page = longs(12);
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, ONE_LONG, null, ZoneOffset.UTC);
            final int maxRows = 11;
            int rowsDone = 0;
            List<String> lines = new ArrayList<>();
            List<Integer> chunkSizes = new ArrayList<>();
            // "[0]\n" is 4 bytes, so a hint of 16 gives chunks of 4 rows
            while (writer.allRowsWritten() == false && writer.stoppedAtLimit() == false) {
                final int before = rowsDone;
                List<String> chunk = writeChunk(writer, 16, (rowsInChunk, bytesInChunk) -> before + rowsInChunk >= maxRows);
                lines.addAll(chunk);
                chunkSizes.add(chunk.size());
                rowsDone += chunk.size();
            }
            assertThat(lines.size(), equalTo(maxRows));
            assertThat(lines.get(maxRows - 1), equalTo("[10]"));
            assertTrue(writer.stoppedAtLimit());
            assertFalse("the last row of the page was not written", writer.allRowsWritten());
            // the first two chunks end at their size hint, the last one is cut short by the limit
            assertThat(chunkSizes, equalTo(List.of(4, 4, 3)));
            assertThat("a reached limit writes nothing", writeChunk(writer, 16, (r, b) -> true), equalTo(List.of()));
        } finally {
            page.releaseBlocks();
        }
    }

    /** The limit is told the rows and bytes of the current call, a newline included in each row, at every check. */
    public void testALimitIsToldTheCountsOfTheCurrentCall() throws IOException {
        Page page = longs(4);
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, ONE_LONG, null, ZoneOffset.UTC);
            List<String> seen = new ArrayList<>();
            NdjsonRowWriter.RowLimit recording = (rowsInChunk, bytesInChunk) -> {
                seen.add(rowsInChunk + "/" + bytesInChunk);
                return rowsInChunk >= 3;
            };
            writeChunk(writer, Integer.MAX_VALUE, recording);
            // "[0]\n" is 4 bytes
            assertThat(seen, equalTo(List.of("0/0", "1/4", "2/8", "3/12")));
            // a new call starts counting again; one row of the page is left, and nothing is checked once the page has ended
            seen.clear();
            writeChunk(writer, Integer.MAX_VALUE, (rowsInChunk, bytesInChunk) -> {
                seen.add(rowsInChunk + "/" + bytesInChunk);
                return false;
            });
            assertThat(seen, equalTo(List.of("0/0")));
            // a full chunk is checked once more, with the row that filled it counted: a hint of 5 is reached by the second row
            seen.clear();
            writeChunk(new NdjsonRowWriter(page, ONE_LONG, null, ZoneOffset.UTC), 5, recording);
            assertThat(seen, equalTo(List.of("0/0", "1/4", "2/8")));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * The chunk is full and the limit is reached by the same row. The limit is reported by that call, so the caller is not asked for
     * one more chunk, which would be empty.
     */
    public void testALimitReachedWhenTheChunkIsFullIsReportedAtOnce() throws IOException {
        Page page = longs(10);
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, ONE_LONG, null, ZoneOffset.UTC);
            // "[0]\n" is 4 bytes: a hint of 5 is reached by the second row, which is also the row that reaches the limit
            assertThat(writeChunk(writer, 5, afterRows(2)), equalTo(List.of("[0]", "[1]")));
            assertTrue(writer.stoppedAtLimit());
            assertFalse(writer.allRowsWritten());
        } finally {
            page.releaseBlocks();
        }
    }

    /** A limit that is reached before the first row of a call is reported, and nothing is written. */
    public void testALimitAlreadyReachedWritesNoRow() throws IOException {
        Page page = longs(5);
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, ONE_LONG, null, ZoneOffset.UTC);
            assertThat(writeChunk(writer, between(1, 100), afterRows(0)), equalTo(List.of()));
            assertTrue(writer.stoppedAtLimit());
            assertFalse(writer.allRowsWritten());
        } finally {
            page.releaseBlocks();
        }
    }

    public void testALimitReachedByTheLastRowOfThePageDoesNotStopThePage() throws IOException {
        Page page = longs(3);
        try {
            NdjsonRowWriter writer = new NdjsonRowWriter(page, ONE_LONG, null, ZoneOffset.UTC);
            assertThat(writeChunk(writer, Integer.MAX_VALUE, afterRows(3)), equalTo(List.of("[0]", "[1]", "[2]")));
            assertTrue(writer.allRowsWritten());
            assertFalse("no row of the page was left out", writer.stoppedAtLimit());
        } finally {
            page.releaseBlocks();
        }
    }
}
