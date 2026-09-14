/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ParallelParsingCoordinator;
import org.elasticsearch.xpack.esql.datasources.StreamingParallelParsingCoordinator;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Every chunk of one file must decode a dotted column against the same schema.
 *
 * <p>A dotted key {@code "a.b"} walks as the nested path {@code a -> b}. The file's schema is
 * resolved once, before any chunk is dispatched, and every chunk decodes against that one
 * answer. A per-chunk inference would let two chunks of the same file disagree whenever a
 * sibling column is not present in every record, so the same query would return different data
 * depending on where the chunk boundaries fell.
 *
 * <p>The sibling here appears only in the first part of the file, which is what makes a
 * per-chunk answer wrong for every chunk after it. The same holds across macro-splits: a
 * non-leading split is a byte range that starts past the file's leading records, so none of
 * its rows carry the sibling at all.
 */
public class NdJsonDottedPrefixChunkConsistencyTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testSiblingColumnAbsentFromLaterChunksStillDecodesFlatKey() throws Exception {
        assertEveryChunkDecodesFlatKey(ErrorPolicy.STRICT);
    }

    public void testSiblingColumnAbsentFromLaterChunksStillDecodesFlatKeyUnderLenientPolicy() throws Exception {
        assertEveryChunkDecodesFlatKey(ErrorPolicy.LENIENT);
    }

    /**
     * A non-leading macro-split is a newline-aligned byte range whose records never carry the
     * sibling scalar, because the sibling appears only before the range starts. The split must
     * still decode the dotted key in every segment of the range.
     */
    public void testNonLeadingMacroSplitDecodesFlatKey() throws Exception {
        assertNonLeadingMacroSplitDecodesFlatKey(ErrorPolicy.STRICT);
    }

    public void testNonLeadingMacroSplitDecodesFlatKeyUnderLenientPolicy() throws Exception {
        assertNonLeadingMacroSplitDecodesFlatKey(ErrorPolicy.LENIENT);
    }

    private void assertEveryChunkDecodesFlatKey(ErrorPolicy errorPolicy) throws Exception {
        Settings settings = segmentSize64Kb();
        long chunkSize = new NdJsonFormatReader(settings, blockFactory).minimumSegmentSize();

        StringBuilder ndjson = new StringBuilder();
        int rows = 0;
        while (ndjson.length() < chunkSize * 3) {
            if (ndjson.length() < chunkSize / 2) {
                // Early records also carry a sibling scalar; later records do not. The dotted key must
                // still decode in every chunk.
                ndjson.append("{\"languages\":\"en\",\"languages.long\":").append((long) rows).append("}\n");
            } else {
                ndjson.append("{\"languages.long\":").append((long) rows).append("}\n");
            }
            rows++;
        }
        assertTrue("fixture must span several chunks", ndjson.length() > chunkSize * 2);

        List<Attribute> bound = List.of(new ReferenceAttribute(Source.EMPTY, "languages.long", DataType.LONG));
        NdJsonFormatReader reader = new NdJsonFormatReader(settings, blockFactory).withSchema(bound);

        InputStream stream = new ByteArrayInputStream(ndjson.toString().getBytes(StandardCharsets.UTF_8));
        ExecutorService executor = Executors.newFixedThreadPool(4);
        long seenRows = 0;
        long nulls = 0;
        long sum = 0;
        try (
            CloseableIterator<Page> pages = StreamingParallelParsingCoordinator.parallelRead(
                (SegmentableFormatReader) reader,
                stream,
                null,
                List.of("languages.long"),
                1000,
                4, // parallelism must exceed 1 or the whole file is read as a single chunk
                executor,
                errorPolicy,
                bound,
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
                    LongBlock block = (LongBlock) page.getBlock(0);
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        if (block.isNull(i)) {
                            nulls++;
                        } else {
                            sum += block.getLong(i);
                        }
                    }
                    seenRows += page.getPositionCount();
                } finally {
                    page.releaseBlocks();
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals("every row must be read", rows, seenRows);
        assertEquals("a chunk whose records omit the sibling must still decode the dotted key, not null it", 0, nulls);
        assertEquals("sum of languages.long", (long) (rows - 1) * rows / 2, sum);
    }

    private void assertNonLeadingMacroSplitDecodesFlatKey(ErrorPolicy errorPolicy) throws Exception {
        Settings settings = segmentSize64Kb();
        NdJsonFormatReader reader = new NdJsonFormatReader(settings, blockFactory);
        long chunkSize = reader.minimumSegmentSize();

        // The leading records carry the sibling scalar; the macro-split range starts after them,
        // on a record boundary, so no record inside the range does.
        StringBuilder head = new StringBuilder();
        int rows = 0;
        while (head.length() < 8192) {
            head.append("{\"languages\":\"en\",\"languages.long\":").append((long) rows).append("}\n");
            rows++;
        }
        int headRows = rows;
        StringBuilder tail = new StringBuilder();
        while (tail.length() < chunkSize * 2 + chunkSize / 2) {
            tail.append("{\"languages.long\":").append((long) rows).append("}\n");
            rows++;
        }
        byte[] tailBytes = tail.toString().getBytes(StandardCharsets.UTF_8);

        List<Attribute> bound = List.of(new ReferenceAttribute(Source.EMPTY, "languages.long", DataType.LONG));
        reader = reader.withSchema(bound);
        BytesStorageObject tailObject = new BytesStorageObject("mem://tail.ndjson", tailBytes);
        int segmentCount = ParallelParsingCoordinator.computeSegments(reader, tailObject, tailBytes.length, 4, reader.minimumSegmentSize())
            .size();
        assertTrue("macro-split range must span several segments", segmentCount > 1);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        long seenRows = 0;
        long nulls = 0;
        long sum = 0;
        try (
            CloseableIterator<Page> pages = ParallelParsingCoordinator.parallelRead(
                reader,
                tailObject,
                List.of("languages.long"),
                1000,
                4,
                executor,
                errorPolicy,
                true, // newline-aligned macro-split start
                false, // the range does not include the file's leading bytes
                bound,
                head.length()
            )
        ) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    LongBlock block = (LongBlock) page.getBlock(0);
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        if (block.isNull(i)) {
                            nulls++;
                        } else {
                            sum += block.getLong(i);
                        }
                    }
                    seenRows += page.getPositionCount();
                } finally {
                    page.releaseBlocks();
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals("every row in the split range must be read", (long) (rows - headRows), seenRows);
        assertEquals("a split whose records omit the sibling must still decode the dotted key, not null it", 0, nulls);
        long expectedSum = (long) (rows - 1) * rows / 2 - (long) (headRows - 1) * headRows / 2;
        assertEquals("sum of languages.long in the split range", expectedSum, sum);
    }

    private static Settings segmentSize64Kb() {
        return Settings.builder().put("esql.external.ndjson.segment_size", "64kb").build();
    }
}
