/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalReadCounters;
import org.elasticsearch.xpack.esql.datasources.ParallelParsingCoordinator;
import org.elasticsearch.xpack.esql.datasources.StreamingParallelParsingCoordinator;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.containsString;

/**
 * A declared schema over a headered CSV binds its columns by name against the header, and only the first
 * chunk of a file can see that header. Every later chunk used to fail outright, so any declared headered
 * file large enough to be read in more than one chunk failed every query that was not limit-pushed —
 * which at a 1 MiB chunk size and default parallelism is every real file.
 *
 * <p>This drives the real reader through the real coordinator over a genuinely multi-chunk stream, which is
 * the closest this layer gets to the failing query. The reader-level halves are pinned in
 * {@link CsvFormatReaderTests}; this pins that the two halves actually meet.
 */
public class CsvDeclaredHeaderMultiChunkTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testDeclaredHeaderedCsvReadsAcrossChunkBoundaries() throws Exception {
        // The streaming coordinator chunks at the reader's minimum segment size, so the content has to
        // exceed it to produce a second chunk at all — below that the read never leaves chunk 0 and the
        // test would pass without the fix.
        long chunkSize = new CsvFormatReader(blockFactory).minimumSegmentSize();
        StringBuilder csv = new StringBuilder("emp_no,first_name,salary\n");
        int rows = 0;
        while (csv.length() < chunkSize * 2) {
            csv.append(rows).append(",name").append(rows).append(',').append(rows * 2L).append('\n');
            rows++;
        }
        assertTrue("fixture must span more than one chunk to exercise the defect", csv.length() > chunkSize);

        // Declared narrower than the file and in a different order, so a positional bind would be visibly wrong.
        List<Attribute> declared = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, null, "first_name", DataType.KEYWORD)
        );

        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", true))
            .withSchema(declared);

        InputStream stream = new ByteArrayInputStream(csv.toString().getBytes(StandardCharsets.UTF_8));
        ExecutorService executor = Executors.newFixedThreadPool(4);
        long seenRows = 0;
        long salarySum = 0;
        try (
            CloseableIterator<Page> pages = StreamingParallelParsingCoordinator.parallelRead(
                (SegmentableFormatReader) reader,
                stream,
                null,
                List.of("salary", "first_name"),
                1000,
                4, // parallelism must exceed 1 or the serial path reads the whole file as one chunk
                executor,
                ErrorPolicy.STRICT,
                declared,
                0L,
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                null,
                -1L,
                StripeColumnScope.PROJECTED,
                StreamingParallelParsingCoordinator.WarningSinks.NONE
            )
        ) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    LongBlock salary = (LongBlock) page.getBlock(0);
                    BytesRefBlock name = (BytesRefBlock) page.getBlock(1);
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        salarySum += salary.getLong(i);
                        assertFalse("name column must not be null", name.isNull(i));
                    }
                    seenRows += page.getPositionCount();
                } finally {
                    page.releaseBlocks();
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals("every data row must be read, across every chunk", rows, seenRows);
        // salary is row*2 — binding by position instead of name would have read emp_no here and halved this.
        assertEquals("salary must bind by name, not position", (long) (rows - 1) * rows, salarySum);
    }

    /**
     * The row-width bound on a chunk after the first comes from the header columns the coordinator read once from
     * chunk 0 and passed down, not from a header this chunk can see. This plants a row wider than the file's header
     * deep enough to land past the first chunk and pins that the real coordinator-supplied width rejects it —
     * the reader-level half is pinned by CsvFormatReaderTests#testDeclaredBindingRowWidthOnNonFirstSplit, which
     * hands the header columns over by hand.
     */
    public void testDeclaredHeaderedCsvAppliesRowWidthBoundOnLaterChunks() throws Exception {
        long chunkSize = new CsvFormatReader(blockFactory).minimumSegmentSize();
        StringBuilder csv = new StringBuilder("emp_no,first_name,salary\n");
        int rows = 0;
        int raggedRowIndex = -1;
        while (csv.length() < chunkSize * 2) {
            // One row carrying a fourth field, planted once the content is already past the first chunk so the
            // bound under test is the carried one rather than chunk 0's own header split.
            if (raggedRowIndex < 0 && csv.length() > chunkSize + chunkSize / 2) {
                csv.append(rows).append(",name").append(rows).append(',').append(rows * 2L).append(",SURPLUS\n");
                raggedRowIndex = rows;
            } else {
                csv.append(rows).append(",name").append(rows).append(',').append(rows * 2L).append('\n');
            }
            rows++;
        }
        assertTrue("the ragged row must land past the first chunk", raggedRowIndex > 0);

        List<Attribute> declared = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, null, "first_name", DataType.KEYWORD)
        );
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", true))
            .withSchema(declared);

        List<String> warnings = Collections.synchronizedList(new ArrayList<>());
        InputStream stream = new ByteArrayInputStream(csv.toString().getBytes(StandardCharsets.UTF_8));
        ExecutorService executor = Executors.newFixedThreadPool(4);
        long seenRows = 0;
        try (
            CloseableIterator<Page> pages = StreamingParallelParsingCoordinator.parallelRead(
                (SegmentableFormatReader) reader,
                stream,
                null,
                List.of("salary", "first_name"),
                1000,
                4,
                executor,
                ErrorPolicy.LENIENT,
                declared,
                0L,
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                null,
                -1L,
                StripeColumnScope.PROJECTED,
                new StreamingParallelParsingCoordinator.WarningSinks(null, warnings::add)
            )
        ) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    seenRows += page.getPositionCount();
                } finally {
                    page.releaseBlocks();
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals("exactly the ragged row is dropped", rows - 1L, seenRows);
        assertTrue(
            "expected a header-width warning naming the file's 3 columns, got: " + warnings,
            warnings.stream().anyMatch(w -> w.contains("CSV row has [4] columns but the file's header defines [3] columns"))
        );
    }

    /**
     * The seekable coordinator splits a file that owns its first byte into segments, and only segment 0 sees the header.
     * It reads the header once from the object's leading bytes and hands it to segments 1..N, so a pinned schema in a
     * different order than the file binds by name on every segment, not only the first.
     */
    public void testParallelLeaderSplitBindsEverySegmentByHeader() throws Exception {
        long minSegment = new CsvFormatReader(blockFactory).minimumSegmentSize();
        StringBuilder csv = new StringBuilder("emp_no,first_name,salary\n");
        int rows = appendRows(csv, 0, 3 * minSegment);
        List<Attribute> pinned = salaryThenName();
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", true));
        StorageObject object = new BytesObject(csv.toString().getBytes(StandardCharsets.UTF_8));

        long[] counted = readParallel(reader, object, true, pinned, null);

        assertEquals("every data row must be read, across every segment", rows, counted[0]);
        assertEquals("salary must bind by name on every segment", (long) (rows - 1) * rows, counted[1]);
    }

    /**
     * A macro-split that does not start at the file's first byte sees no header at all. Handed the file's header columns
     * it binds every segment by name; without them it fails rather than binding by position.
     */
    public void testParallelNonLeaderSplitBindsByCarriedHeader() throws Exception {
        long minSegment = new CsvFormatReader(blockFactory).minimumSegmentSize();
        StringBuilder csv = new StringBuilder();
        int rows = appendRows(csv, 0, 3 * minSegment);
        List<Attribute> pinned = salaryThenName();
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", true));
        StorageObject object = new BytesObject(csv.toString().getBytes(StandardCharsets.UTF_8));

        long[] counted = readParallel(reader, object, false, pinned, List.of("emp_no", "first_name", "salary"));
        assertEquals("every data row must be read, across every segment", rows, counted[0]);
        assertEquals("salary must bind by name on every segment", (long) (rows - 1) * rows, counted[1]);

        Exception e = expectThrows(Exception.class, () -> readParallel(reader, object, false, pinned, null));
        assertThat(
            "a split with no header and no carried header columns must fail, not bind by position",
            ExceptionsHelper.stackTrace(e),
            containsString("without the file's header columns")
        );
    }

    private static int appendRows(StringBuilder csv, int from, long untilLength) {
        int row = from;
        while (csv.length() < untilLength) {
            csv.append(row).append(",name").append(row).append(',').append(row * 2L).append('\n');
            row++;
        }
        return row - from;
    }

    private static List<Attribute> salaryThenName() {
        return List.of(
            new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, null, "first_name", DataType.KEYWORD)
        );
    }

    /** Returns {rows read, sum of salary}. */
    private long[] readParallel(
        CsvFormatReader reader,
        StorageObject object,
        boolean splitIncludesFileLeader,
        List<Attribute> pinned,
        List<String> fileHeaderColumns
    ) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        long seenRows = 0;
        long salarySum = 0;
        try (
            CloseableIterator<Page> pages = ParallelParsingCoordinator.parallelRead(
                reader,
                object,
                List.of("salary", "first_name"),
                1000,
                4,
                executor,
                ErrorPolicy.STRICT,
                true,
                splitIncludesFileLeader,
                pinned,
                0L,
                4,
                null,
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                -1L,
                StripeColumnScope.PROJECTED,
                true,
                ExternalSourceMetrics.NOOP,
                null,
                ExternalReadCounters.NOOP,
                null,
                fileHeaderColumns
            )
        ) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    LongBlock salary = (LongBlock) page.getBlock(0);
                    BytesRefBlock name = (BytesRefBlock) page.getBlock(1);
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        salarySum += salary.getLong(i);
                        assertFalse("name column must not be null", name.isNull(i));
                    }
                    seenRows += page.getPositionCount();
                } finally {
                    page.releaseBlocks();
                }
            }
        } finally {
            executor.shutdownNow();
        }
        return new long[] { seenRows, salarySum };
    }

    /** An in-memory file that serves range reads, which the seekable coordinator's segments use. */
    private static final class BytesObject implements StorageObject {
        private final byte[] bytes;

        BytesObject(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public InputStream newStream(long position, long length) {
            int from = (int) position;
            int len = length < 0 ? bytes.length - from : (int) Math.min(length, bytes.length - position);
            return new ByteArrayInputStream(bytes, from, len);
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public long length() {
            return bytes.length;
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
            return StoragePath.of("memory://multi-segment.csv");
        }
    }
}
