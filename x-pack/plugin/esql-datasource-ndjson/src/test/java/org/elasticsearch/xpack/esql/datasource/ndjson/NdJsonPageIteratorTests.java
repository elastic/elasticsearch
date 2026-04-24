/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ConstantNullBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.rest.RestResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.formatter.TextFormat;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NdJsonPageIteratorTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testIterator() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("classpath://employees.ndjson", IOUtils.resourceToByteArray("/employees.ndjson"));

        List<Integer> sizes = new ArrayList<>();
        try (var iterator = reader.read(object, List.of("still_hired", "emp_no", "birth_date", "non_existing_field"), 42)) {
            while (iterator.hasNext()) {
                var page = iterator.next();
                assertEquals(4, page.getBlockCount());
                checkBlockSizes(page);

                // Make sure blocks are returned in the order requested, with nulls for unknown columns
                assertThat(page.getBlock(0), Matchers.instanceOf(BooleanBlock.class));
                assertThat(page.getBlock(1), Matchers.instanceOf(IntBlock.class));
                assertThat(page.getBlock(2), Matchers.instanceOf(LongBlock.class));
                assertThat(page.getBlock(3), Matchers.instanceOf(ConstantNullBlock.class));

                sizes.add(page.getBlock(0).getPositionCount());
            }
        }

        assertEquals(List.of(42, 42, 16), sizes); // Total 100
    }

    public void testJsonExtensionRecognized() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        assertTrue("NdJsonFormatReader should list .json as a supported extension", reader.fileExtensions().contains(".json"));
    }

    public void testJsonExtensionReadsData() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("file:///data.json", IOUtils.resourceToByteArray("/employees.ndjson"));

        try (var iterator = reader.read(object, List.of("emp_no"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertThat(page.getBlock(0), Matchers.instanceOf(IntBlock.class));
            assertTrue(page.getPositionCount() > 0);
        }
    }

    public void testSkipFirstLineForSplit() throws IOException {
        // Simulate a split that starts mid-line: "partial_first_line\n{\"id\":1}\n{\"id\":2}\n"
        String data = "partial_first_line\n{\"id\":1}\n{\"id\":2}\n";
        var object = new BytesStorageObject("file:///split.ndjson", data.getBytes(StandardCharsets.UTF_8));

        var reader = new NdJsonFormatReader(null, blockFactory);
        try (
            var iterator = reader.read(
                object,
                FormatReadContext.builder()
                    .projectedColumns(List.of("id"))
                    .batchSize(100)
                    .errorPolicy(ErrorPolicy.LENIENT)
                    .firstSplit(false)
                    .lastSplit(true)
                    .build()
            )
        ) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            // Should have skipped "partial_first_line" and read 2 records
            assertEquals(2, page.getPositionCount());
            assertThat(page.getBlock(0), Matchers.instanceOf(IntBlock.class));
            IntBlock idBlock = page.getBlock(0);
            assertEquals(1, idBlock.getInt(0));
            assertEquals(2, idBlock.getInt(1));
        }
    }

    /**
     * Same shape as {@code NdJsonFixtureGenerator} output from {@code employees.csv}: flat keys such as
     * {@code languages.long} must decode when {@code languages} is also a scalar column.
     */
    public void testFlatDottedColumnsFromEmployeesFixtureShape() throws IOException {
        String ndjson = """
            {
                "birth_date":"1953-09-02T00:00:00Z",
                "emp_no":10001,
                "first_name":"Georgi",
                "gender":"M",
                "hire_date":"1986-06-26T00:00:00Z",
                "languages":2,
                "languages.long":2,
                "languages.short":2,
                "languages.byte":2,
                "last_name":"Facello",
                "salary":57305,
                "height":2.03,
                "height.float":2.03,
                "height.scaled_float":2.03,
                "height.half_float":2.03,
                "still_hired":true,
                "avg_worked_seconds":268728049,
                "job_positions":["Senior Python Developer","Accountant"],
                "is_rehired":[false,true],
                "salary_change":[1.19],
                "salary_change.int":[1],
                "salary_change.long":[1],
                "salary_change.keyword":["1.19"]
            }""";
        var object = new BytesStorageObject("memory://employees-qa.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        try (var iterator = reader.read(object, List.of("emp_no", "first_name", "languages.long", "avg_worked_seconds"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertThat(page.getBlock(0), Matchers.instanceOf(IntBlock.class));
            assertThat(page.getBlock(1), Matchers.instanceOf(BytesRefBlock.class));
            assertEquals(10001, ((IntBlock) page.getBlock(0)).getInt(0));
            Block languagesLong = page.getBlock(2);
            if (languagesLong instanceof IntBlock il) {
                assertEquals(2, il.getInt(0));
            } else if (languagesLong instanceof LongBlock ll) {
                assertEquals(2L, ll.getLong(0));
            } else {
                fail("unexpected block for languages.long: " + languagesLong);
            }
            Block avgWorked = page.getBlock(3);
            if (avgWorked instanceof IntBlock iw) {
                assertEquals(268728049, iw.getInt(0));
            } else if (avgWorked instanceof LongBlock lw) {
                assertEquals(268728049L, lw.getLong(0));
            } else {
                fail("unexpected block for avg_worked_seconds: " + avgWorked);
            }
        }
    }

    public void testTrimLastPartialLineDropsIncompleteTail() throws IOException {
        String data = "{\"id\":1}\n{\"id\":2}\n{\"incomplete\":";
        try (
            InputStream trimmed = NdJsonPageIterator.trimLastPartialLine(
                new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)),
                ErrorPolicy.STRICT
            )
        ) {
            assertEquals("{\"id\":1}\n{\"id\":2}\n", new String(trimmed.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    public void testTrimLastPartialLineEmptyWhenNoNewline() throws IOException {
        try (
            InputStream trimmed = NdJsonPageIterator.trimLastPartialLine(
                new ByteArrayInputStream("partial-only".getBytes(StandardCharsets.UTF_8)),
                ErrorPolicy.STRICT
            )
        ) {
            assertEquals(0, trimmed.readAllBytes().length);
        }
    }

    public void testTrimLastPartialLineEmptyStream() throws IOException {
        try (InputStream trimmed = NdJsonPageIterator.trimLastPartialLine(new ByteArrayInputStream(new byte[0]), ErrorPolicy.STRICT)) {
            assertEquals(0, trimmed.readAllBytes().length);
        }
    }

    /** Input already ends on a line feed: nothing after the last delimiter to trim. */
    public void testTrimLastPartialLineInputEndsWithNewline() throws IOException {
        byte[] data = "{\"x\":1}\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream trimmed = NdJsonPageIterator.trimLastPartialLine(new ByteArrayInputStream(data), ErrorPolicy.STRICT)) {
            assertArrayEquals(data, trimmed.readAllBytes());
        }
    }

    /**
     * Exercises carry + emit across multiple small reads (chunk size 4) to match the behavior of
     * trimming when newline boundaries do not align with read buffers.
     */
    public void testTrimLastPartialLineAcrossSmallChunks() throws IOException {
        byte[] payload = "aa\nbb\nPART".getBytes(StandardCharsets.UTF_8);
        try (InputStream trimmed = new TrimLastPartialLineInputStream(new ByteArrayInputStream(payload), 4, ErrorPolicy.STRICT)) {
            assertEquals("aa\nbb\n", new String(trimmed.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /**
     * Regression: after the consumer has advanced {@code readIdx}, growing the emit buffer must use
     * {@code writeIdx + emitLen}, not {@code unread + emitLen}, or a large carry + line can write past
     * the end of the reallocated array.
     */
    public void testTrimLastPartialLineBufferGrowAfterPartialRead() throws IOException {
        int trimChunk = 8192;
        List<byte[]> parts = new ArrayList<>();
        byte[] firstLine = new byte[5001];
        Arrays.fill(firstLine, 0, 5000, (byte) '0');
        firstLine[5000] = '\n';
        parts.add(firstLine);
        for (int i = 0; i < 4; i++) {
            parts.add(bytesOf(trimChunk, 'c'));
        }
        byte[] terminal = new byte[3001];
        Arrays.fill(terminal, 0, 3000, (byte) 'd');
        terminal[3000] = '\n';
        parts.add(terminal);

        try (InputStream trimmed = new TrimLastPartialLineInputStream(new ChainedByteChunksStream(parts), trimChunk, ErrorPolicy.STRICT)) {
            assertEquals(2000, trimmed.readNBytes(2000).length);
            byte[] tail = trimmed.readAllBytes();
            assertEquals(5001 - 2000 + (4L * trimChunk) + 3001, tail.length);
        }
    }

    private static byte[] bytesOf(int len, char fill) {
        byte[] b = new byte[len];
        Arrays.fill(b, (byte) fill);
        return b;
    }

    /** Sequences fixed-size byte arrays as one logical {@link InputStream}. */
    private static final class ChainedByteChunksStream extends InputStream {
        private final List<byte[]> chunks;
        private int chunkIndex;
        private int posInChunk;

        ChainedByteChunksStream(List<byte[]> chunks) {
            this.chunks = chunks;
        }

        @Override
        public int read() {
            while (chunkIndex < chunks.size()) {
                byte[] cur = chunks.get(chunkIndex);
                if (posInChunk < cur.length) {
                    return cur[posInChunk++] & 0xFF;
                }
                chunkIndex++;
                posInChunk = 0;
            }
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (len == 0) {
                return 0;
            }
            int total = 0;
            while (len > 0 && chunkIndex < chunks.size()) {
                byte[] cur = chunks.get(chunkIndex);
                if (posInChunk >= cur.length) {
                    chunkIndex++;
                    posInChunk = 0;
                    continue;
                }
                int n = Math.min(len, cur.length - posInChunk);
                System.arraycopy(cur, posInChunk, b, off, n);
                posInChunk += n;
                off += n;
                len -= n;
                total += n;
            }
            return total == 0 ? -1 : total;
        }
    }

    /**
     * Without a record delimiter, {@link TrimLastPartialLineInputStream} must not grow {@code carry}
     * past {@link TrimLastPartialLineInputStream#MAX_CARRY_BYTES} (defends against pathological lines).
     */
    public void testTrimLastPartialLineCarryExceedsMaxThrows() throws IOException {
        int chunk = 8192;
        long streamLen = TrimLastPartialLineInputStream.MAX_CARRY_BYTES + chunk;
        try (
            InputStream trimmed = new TrimLastPartialLineInputStream(new FiniteBytesWithoutNewline(streamLen), chunk, ErrorPolicy.STRICT)
        ) {
            IOException ex = expectThrows(IOException.class, trimmed::readAllBytes);
            assertThat(ex.getMessage(), Matchers.containsString(TrimLastPartialLineInputStream.MAX_CARRY.toString()));
        }
    }

    /**
     * When {@link ErrorPolicy#isStrict()} is false, an oversized partial line is dropped instead of
     * failing the whole read (same stream shape as {@link #testTrimLastPartialLineCarryExceedsMaxThrows}).
     */
    public void testTrimLastPartialLineCarryOverLimitLenientSkipsBogusLine() throws IOException {
        int chunk = 8192;
        long streamLen = TrimLastPartialLineInputStream.MAX_CARRY_BYTES + chunk;
        try (
            InputStream trimmed = new TrimLastPartialLineInputStream(new FiniteBytesWithoutNewline(streamLen), chunk, ErrorPolicy.LENIENT)
        ) {
            assertEquals(0, trimmed.readAllBytes().length);
        }
    }

    /** Supplies {@code length} bytes of {@code 'a'} without allocating that array (no newlines). */
    private static final class FiniteBytesWithoutNewline extends InputStream {
        private final long length;
        private long pos;

        FiniteBytesWithoutNewline(long length) {
            this.length = length;
        }

        @Override
        public int read() {
            if (pos >= length) {
                return -1;
            }
            pos++;
            return 'a';
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= length) {
                return -1;
            }
            long remaining = length - pos;
            int n = (int) Math.min(len, remaining);
            Arrays.fill(b, off, off + n, (byte) 'a');
            pos += n;
            return n;
        }
    }

    public void testSkipFirstLineNoSkip() throws IOException {
        String data = "{\"id\":1}\n{\"id\":2}\n";
        var object = new BytesStorageObject("file:///split.ndjson", data.getBytes(StandardCharsets.UTF_8));

        var reader = new NdJsonFormatReader(null, blockFactory);
        try (
            var iterator = reader.read(
                object,
                FormatReadContext.builder()
                    .projectedColumns(List.of("id"))
                    .batchSize(100)
                    .errorPolicy(ErrorPolicy.LENIENT)
                    .firstSplit(true)
                    .lastSplit(true)
                    .build()
            )
        ) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertEquals(2, page.getPositionCount());
        }
    }

    public void testSampleData() throws Exception {
        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("classpath://employees.ndjson", IOUtils.resourceToByteArray("/employees.ndjson"));

        var metadata = reader.metadata(object);
        var schema = metadata.schema();

        assertEquals("birth_date", schema.get(0).name());
        assertEquals(DataType.DATETIME, schema.get(0).dataType());

        assertEquals("emp_no", schema.get(1).name());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());

        assertEquals("still_hired", schema.get(9).name());
        assertEquals(DataType.BOOLEAN, schema.get(9).dataType());

        try (var iterator = reader.read(object, List.of(), 1000)) {
            var page = iterator.next();
            checkBlockSizes(page);

            LongBlock birthDate = page.getBlock(blockIdx(metadata, "birth_date"));
            IntBlock empNo = page.getBlock(blockIdx(metadata, "emp_no"));
            BooleanBlock stillHired = page.getBlock(blockIdx(metadata, "still_hired"));
            DoubleBlock height = page.getBlock(blockIdx(metadata, "height"));

            assertEquals("1963-06-01T00:00:00Z", Instant.ofEpochMilli(birthDate.getLong(9)).toString());
            assertEquals(10010, empNo.getInt(9));
            assertFalse(stillHired.getBoolean(9));
            assertEquals(1.70, height.getDouble(9), 0.0001);
        }
    }

    public void testMalformedLineDoesNotCrash() throws IOException {
        // A completely invalid JSON line should not crash the parser; it should be skipped
        String ndjson = """
            {"name":"alice","age":30}
            NOT-JSON-AT-ALL
            {"name":"charlie","age":40}
            """;
        var object = new BytesStorageObject("memory://test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);

        List<Page> pages = new ArrayList<>();
        try (
            var iterator = reader.read(
                object,
                FormatReadContext.builder().projectedColumns(List.of()).batchSize(100).errorPolicy(ErrorPolicy.LENIENT).build()
            )
        ) {
            while (iterator.hasNext()) {
                pages.add(iterator.next());
            }
        }

        // Two valid rows (alice + charlie); the invalid line is skipped
        int totalRows = 0;
        for (var page : pages) {
            totalRows += page.getPositionCount();
            checkBlockSizes(page);
        }
        assertEquals(2, totalRows);
    }

    /**
     * Regression: decodeObject failure on a line must not null-fill a bogus row or leave the stream
     * positioned so following valid NDJSON lines are lost. The middle line uses invalid structure
     * triple-brace garbage so parsing fails before any field value is appended to block builders.
     */
    public void testMalformedObjectMidLineSkippedReaderResumes() throws IOException {
        String ndjson = """
            {"id":1}
            {{{not-an-object
            {"id":3}
            """;
        var object = new BytesStorageObject("memory://test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        try (
            var iterator = reader.read(
                object,
                FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(100).errorPolicy(ErrorPolicy.LENIENT).build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            IntBlock id = page.getBlock(0);
            assertEquals(1, id.getInt(0));
            assertEquals(3, id.getInt(1));
            assertFalse(iterator.hasNext());
        }
    }

    public void testFailFastOnMalformedNdjsonLine() throws IOException {
        String ndjson = """
            {"id":1}
            {{{not-an-object
            {"id":3}
            """;
        var object = new BytesStorageObject("memory://test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        var ctx = FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(1).errorPolicy(ErrorPolicy.STRICT).build();
        try (var iterator = reader.read(object, ctx)) {
            assertTrue(iterator.hasNext());
            Page first = iterator.next();
            assertEquals(1, first.getPositionCount());
            assertEquals(1, ((IntBlock) first.getBlock(0)).getInt(0));
            EsqlIllegalArgumentException ex = expectThrows(EsqlIllegalArgumentException.class, iterator::hasNext);
            assertThat(ex.getMessage(), Matchers.containsString("Malformed NDJSON"));
        }
    }

    /**
     * FAIL_FAST must abort while decoding a later page, not only when the first malformed line
     * falls in the first {@link FormatReadContext#batchSize()} rows. Uses a small batch for speed;
     * see {@link #testFailFastWhenMalformedLineAfterPlannerDefaultExternalPageSize()} for the planner’s
     * default external page size ({@link LocalExecutionPlanner#DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS}).
     */
    public void testFailFastAfterFirstFullPage() throws IOException {
        int batchSize = 3;
        assertThat(batchSize, Matchers.lessThan(LocalExecutionPlanner.DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS));
        String ndjson = """
            {"id":1}
            {"id":2}
            {"id":3}
            {{{not-an-object
            {"id":5}
            """;
        var object = new BytesStorageObject("memory://test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        var ctx = FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(batchSize).errorPolicy(ErrorPolicy.STRICT).build();
        try (var iterator = reader.read(object, ctx)) {
            assertTrue(iterator.hasNext());
            Page first = iterator.next();
            assertEquals(batchSize, first.getPositionCount());
            EsqlIllegalArgumentException ex = expectThrows(EsqlIllegalArgumentException.class, iterator::hasNext);
            assertThat(ex.getMessage(), Matchers.containsString("Malformed NDJSON"));
        }
    }

    /**
     * Same regression as {@link #testFailFastAfterFirstFullPage}, but with a batch size equal to
     * {@link LocalExecutionPlanner#DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS} (the fallback when estimated row size
     * is unknown for external sources in {@link LocalExecutionPlanner}). The first full page succeeds; FAIL_FAST must
     * still surface on the next decode when the malformed line is past that many good rows.
     */
    public void testFailFastWhenMalformedLineAfterPlannerDefaultExternalPageSize() throws IOException {
        int pageRows = LocalExecutionPlanner.DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS;
        StringBuilder ndjson = new StringBuilder(pageRows * 20);
        for (int i = 1; i <= pageRows; i++) {
            ndjson.append("{\"id\":").append(i).append("}\n");
        }
        ndjson.append("{{{not-an-object\n");
        ndjson.append("{\"id\":").append(pageRows + 2).append("}\n");

        var object = new BytesStorageObject("memory://failfast-large.ndjson", ndjson.toString().getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        var ctx = FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(pageRows).errorPolicy(ErrorPolicy.STRICT).build();
        try (var iterator = reader.read(object, ctx)) {
            assertTrue(iterator.hasNext());
            Page first = iterator.next();
            assertEquals(pageRows, first.getPositionCount());
            EsqlIllegalArgumentException ex = expectThrows(EsqlIllegalArgumentException.class, iterator::hasNext);
            assertThat(ex.getMessage(), Matchers.containsString("Malformed NDJSON"));
        }
    }

    public void testRowLimitTrimsLastPage() throws IOException {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 1; i <= 20; i++) {
            ndjson.append("{\"id\":").append(i).append("}\n");
        }
        var object = new BytesStorageObject("memory://rows.ndjson", ndjson.toString().getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        var ctx = FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(8).rowLimit(5).build();
        int totalRows = 0;
        try (var iterator = reader.read(object, ctx)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                checkBlockSizes(page);
            }
        }
        assertEquals(5, totalRows);
    }

    public void testRowLimitNoOpWhenUnlimited() throws IOException {
        String ndjson = "{\"id\":1}\n{\"id\":2}\n";
        var object = new BytesStorageObject("memory://x.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        var ctx = FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(10).rowLimit(FormatReader.NO_LIMIT).build();
        int totalRows = 0;
        try (var iterator = reader.read(object, ctx)) {
            while (iterator.hasNext()) {
                totalRows += iterator.next().getPositionCount();
            }
        }
        assertEquals(2, totalRows);
    }

    /**
     * Regression: when {@code decodeObject} fails after writing at least one projected field, tolerant
     * policies must not commit partial data to page builders (would misalign {@link Page} columns).
     * The stream ends after the bad line so recovery does not need a following record boundary.
     */
    public void testPartialDecodeLineFailsScratchDoesNotMisalignPage() throws IOException {
        String ndjson = """
            {"id":1,"name":"a"}
            {"id":2,"note":"x
            """;
        var object = new BytesStorageObject("memory://trunc.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        int totalRows = 0;
        try (
            var iterator = reader.read(
                object,
                FormatReadContext.builder().projectedColumns(List.of("id", "name")).batchSize(50).errorPolicy(ErrorPolicy.LENIENT).build()
            )
        ) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                checkBlockSizes(page);
                totalRows += page.getPositionCount();
            }
        }
        assertEquals(1, totalRows);
    }

    public void testFailFastPartialDecodeLine() throws IOException {
        String ndjson = """
            {"id":1,"name":"a"}
            {"id":2,"note":"x
            """;
        var object = new BytesStorageObject("memory://trunc.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        var ctx = FormatReadContext.builder().projectedColumns(List.of("id", "name")).batchSize(1).errorPolicy(ErrorPolicy.STRICT).build();
        try (var iterator = reader.read(object, ctx)) {
            assertTrue(iterator.hasNext());
            Page first = iterator.next();
            assertEquals(1, first.getPositionCount());
            assertEquals(1, ((IntBlock) first.getBlock(0)).getInt(0));
            EsqlIllegalArgumentException ex = expectThrows(EsqlIllegalArgumentException.class, iterator::hasNext);
            assertThat(ex.getMessage(), Matchers.containsString("Malformed NDJSON"));
        }
    }

    public void testConsistentBlockPositionCounts() throws IOException {
        // Ensures all blocks in a page have the same position count even with missing data
        String ndjson = """
            {"x":1,"y":"a"}
            {"x":2}
            {"x":3,"y":"c"}
            """;
        var object = new BytesStorageObject("memory://test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);

        try (var iterator = reader.read(object, List.of(), 100)) {
            while (iterator.hasNext()) {
                var page = iterator.next();
                checkBlockSizes(page);
                assertEquals(3, page.getPositionCount());
            }
        }
    }

    public void testTypeDifferentFromSchema() throws IOException {

        String ndjson = """
            {"x": "2024-01-01T00:00:00Z", "y": 1}
            {"x": true, "y": 2}
            """;

        // Infer schema from the first line only
        var settings = Settings.builder().put(NdJsonFormatReader.SCHEMA_SAMPLE_SIZE_SETTING, 1).build();

        var reader = new NdJsonFormatReader(settings, blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("x", "y"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertPage(page, """
                     LONG      |      INT     \s
                ---------------+---------------
                1704067200000  |1             \s
                null           |2             \s
                """);

            assertEquals(ElementType.LONG, page.getBlock(0).elementType()); // DATETIME

            assertEquals(2, page.getBlock(0).getPositionCount());
            assertEquals(2, page.getBlock(1).getPositionCount());
            assertEquals(2, page.getPositionCount());

            assertEquals(Instant.parse("2024-01-01T00:00:00Z").toEpochMilli(), ((LongBlock) page.getBlock(0)).getLong(0));
            assertTrue(page.getBlock(0).isNull(1)); // Boolean ignored
        }
    }

    public void testNestedObject() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"address": {"city": "NYC", "zip": "10001"}}
            {"address": {"city": "London", "zip": "SW1A"}}
            """;

        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("address.city", "address.zip"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertPage(page, """
                   BYTES_REF   |   BYTES_REF  \s
                ---------------+---------------
                NYC            |10001         \s
                London         |SW1A          \s
                """);
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());
        }
    }

    public void testArrayOfObjects() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"events": [{"type": "click", "page": 1}, {"type": "view", "page": 2}], "id": 1}
            {"events": [{"type": "click", "page": 3}, {"type": "view", "page": null}], "id": 2}
            """;

        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var schema = reader.metadata(object).schema();
        assertSchema(schema, "events.type:KEYWORD, events.page:INTEGER?, id:INTEGER");

        try (var iterator = reader.read(object, null, 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();

            assertPage(page, """
                   BYTES_REF   |      INT      |      INT     \s
                ---------------+---------------+---------------
                [click, view]  |[1, 2]         |1             \s
                [click, view]  |3              |2             \s
                """);

        }
    }

    public void testNullsInArray() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"tags": ["a", null, "b"], "id": 1}
            {"tags": ["c", "d"], "id": 2}
            """;

        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("tags", "id"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();

            assertPage(page, """
                   BYTES_REF   |      INT     \s
                ---------------+---------------
                [a, b]         |1             \s
                [c, d]         |2             \s
                """);

            assertEquals(page.getBlock(0).getPositionCount(), page.getBlock(1).getPositionCount());
            assertEquals(2, page.getPositionCount());
        }
    }

    public void testNestedArraysMisalignment() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"matrix": [[1,2],[3,4]], "id": 1}
            {"matrix": [[5,6]], "id": 2}
            """;

        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("matrix", "id"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertEquals(page.getBlock(0).getPositionCount(), page.getBlock(1).getPositionCount());
            assertEquals(2, page.getPositionCount());
        }
    }

    public void testNonNullValueForNullTypedColumn() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"data": null, "id": 0}
            {"data": [1, 2, 3], "id": 1}
            """;

        var settings = Settings.builder().put(NdJsonFormatReader.SCHEMA_SAMPLE_SIZE_SETTING, 1).build();
        var reader = new NdJsonFormatReader(settings, blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        var schema = reader.metadata(object).schema();
        assertSchema(schema, "id:INTEGER"); // data is all null during inference, and therefore ignored

        try (var iterator = reader.read(object, List.of("data", "id"), 200)) {
            var page = iterator.next();
            // 2nd line ignored as inference was only on line 2
            assertPage(page, """
                     NULL      |      INT     \s
                ---------------+---------------
                null           |0             \s
                null           |1             \s
                """);
        }
    }

    public void testDateParsing() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"timestamp": "2025-03-26T18:12:34Z"}
            {"timestamp": "2025-03-26T00:00:00Z"}
            {"timestamp": "2025-03-26"}
            """;

        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        var schema = reader.metadata(object).schema();
        assertSchema(schema, "timestamp:DATETIME");

        try (var iterator = reader.read(object, List.of(), 100)) {
            var page = iterator.next();
            assertPage(page, """
                     LONG     \s
                ---------------
                1743012754000 \s
                1742947200000 \s
                1742947200000 \s
                """);
        }
    }

    public void testBigInteger() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"id": 1, "big": 18446744073709551615}
            {"id": 2, "big": 42}
            """;

        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("id", "big"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertPage(page, """
                      INT      |       DOUBLE       \s
                ---------------+---------------------
                1              |1.8446744073709552E19
                2              |42.0                \s
                """);
        }
    }

    public void testBigDecimal() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        // Extra large numeric values convert to Infinity
        // DOUBLE.MAX_VALUE is 1.7976931348623157e+308
        String ndjson = """
            {"id": 1, "big": 1.23e+400}
            {"id": 2, "big": 42}
            """;

        var reader = new NdJsonFormatReader(null, blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("id", "big"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertPage(page, """
                      INT      |    DOUBLE    \s
                ---------------+---------------
                1              |Infinity      \s
                2              |42.0          \s
                """);
        }
    }

    // --- findNextRecordBoundary tests ---

    public void testFindNextRecordBoundaryNewline() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        byte[] data = "{\"key\":\"value\"}\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length, reader.findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextRecordBoundaryCRLF() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        byte[] data = "{\"key\":\"value\"}\r\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length, reader.findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextRecordBoundaryCROnly() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        byte[] data = "{\"key\":\"value\"}\rmore".getBytes(StandardCharsets.UTF_8);
        int expected = "{\"key\":\"value\"}\r".length();
        assertEquals(expected, reader.findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextRecordBoundaryCRLFAtBufferEdge() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        byte[] padding = new byte[8191];
        Arrays.fill(padding, (byte) 'x');
        byte[] suffix = "\r\nmore\n".getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[padding.length + suffix.length];
        System.arraycopy(padding, 0, data, 0, padding.length);
        System.arraycopy(suffix, 0, data, padding.length, suffix.length);
        long boundary = reader.findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals(8193, boundary);
    }

    public void testFindNextRecordBoundaryEofNoNewline() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        byte[] data = "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextRecordBoundaryEmptyStream() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        assertEquals(-1, reader.findNextRecordBoundary(new ByteArrayInputStream(new byte[0])));
    }

    private int blockIdx(SourceMetadata meta, String name) {
        for (int i = 0; i < meta.schema().size(); i++) {
            if (meta.schema().get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column '" + name + "' not found in metadata");
    }

    private void checkBlockSizes(Page page) {
        int size = page.getPositionCount();
        for (int i = 0; i < page.getBlockCount(); i++) {
            assertEquals("Block[" + i + "] position count", size, page.getBlock(i).getPositionCount());
        }
    }

    private static void assertSchema(List<Attribute> attributes, String expected) {
        var str = attributes.stream()
            .map(a -> a.name() + ":" + a.dataType().toString() + (a.nullable() == Nullability.TRUE ? "?" : ""))
            .collect(Collectors.joining(", "));

        assertEquals(expected, str);
    }

    private static void assertPage(Page page, String expected) {
        var req = new FakeRestRequest();
        var format = TextFormat.PLAIN_TEXT;
        var cols = new ArrayList<ColumnInfoImpl>();
        for (int i = 0; i < page.getBlockCount(); i++) {
            var block = page.getBlock(i);
            cols.add(new ColumnInfoImpl(block.elementType().toString(), dataType(block), null));
        }
        var resp = new EsqlQueryResponse(cols, List.of(page), 0, 0, null, false, false, ZoneOffset.UTC, 0, 0, null);
        var str = RestResponseUtils.getTextBodyContent(format.format(req, resp));

        assertEquals(expected, str);
    }

    public void testWithConfigSchemaSampleSizeOverride() {
        NdJsonFormatReader reader = new NdJsonFormatReader(Settings.EMPTY, blockFactory);
        var configured = reader.withConfig(Map.of("schema_sample_size", "50"));
        assertNotSame(reader, configured);
    }

    public void testWithConfigSchemaSampleSizeZeroIsRejected() {
        NdJsonFormatReader reader = new NdJsonFormatReader(Settings.EMPTY, blockFactory);
        expectThrows(QlIllegalArgumentException.class, () -> reader.withConfig(Map.of("schema_sample_size", "0")));
    }

    public void testWithConfigSchemaSampleSizeNegativeIsRejected() {
        NdJsonFormatReader reader = new NdJsonFormatReader(Settings.EMPTY, blockFactory);
        expectThrows(QlIllegalArgumentException.class, () -> reader.withConfig(Map.of("schema_sample_size", "-1")));
    }

    public void testWithConfigSchemaSampleSizeInvalidIsRejected() {
        NdJsonFormatReader reader = new NdJsonFormatReader(Settings.EMPTY, blockFactory);
        expectThrows(IllegalArgumentException.class, () -> reader.withConfig(Map.of("schema_sample_size", "abc")));
    }

    public void testWithConfigNullOrEmptyReturnsThis() {
        NdJsonFormatReader reader = new NdJsonFormatReader(Settings.EMPTY, blockFactory);
        assertSame(reader, reader.withConfig(null));
        assertSame(reader, reader.withConfig(Map.of()));
    }

    public void testDefaultErrorPolicyIsStrictLikeOtherFormats() {
        assertEquals(ErrorPolicy.STRICT, new NdJsonFormatReader(Settings.EMPTY, blockFactory).defaultErrorPolicy());
    }

    private static DataType dataType(Block block) {
        return switch (block.elementType()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case INT -> DataType.INTEGER;
            case LONG -> DataType.LONG;
            case FLOAT -> DataType.FLOAT;
            case DOUBLE -> DataType.DOUBLE;
            case NULL -> DataType.NULL;
            case BYTES_REF -> DataType.KEYWORD;
            case DOC, COMPOSITE, UNKNOWN, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE ->
                throw new IllegalArgumentException("Unsupported block type: " + block.elementType());
        };
    }
}
