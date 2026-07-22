/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

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
import org.elasticsearch.xpack.esql.datasources.StreamingParallelParsingCoordinator;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
            .withDeclaredPathBinding(true)
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
}
