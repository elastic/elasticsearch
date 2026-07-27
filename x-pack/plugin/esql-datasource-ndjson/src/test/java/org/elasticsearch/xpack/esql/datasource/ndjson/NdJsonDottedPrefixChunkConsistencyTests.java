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
 * Every chunk of one file must decide the flat-versus-nested question the same way.
 *
 * <p>A flat dotted key {@code "a.b"} decodes as one field only when the file also has a column {@code "a"};
 * otherwise it is read as the nested path {@code a -> b}. Answering that per chunk lets two chunks of the
 * same file disagree whenever the sibling column is not present in every record — the chunks that happen not
 * to see it read the column as a nested path and produce nulls, so the same query returns different data
 * depending on where the chunk boundaries fell. The file's schema is therefore resolved once, before any
 * chunk is dispatched, and every chunk decodes against that one answer.
 *
 * <p>The sibling here appears only in the first part of the file, which is what makes a per-chunk answer
 * wrong for every chunk after it.
 */
public class NdJsonDottedPrefixChunkConsistencyTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testSiblingColumnAbsentFromLaterChunksStillDecodesFlatKey() throws Exception {
        Settings settings = segmentSize64Kb();
        long chunkSize = new NdJsonFormatReader(settings, blockFactory).minimumSegmentSize();

        StringBuilder ndjson = new StringBuilder();
        int rows = 0;
        while (ndjson.length() < chunkSize * 3) {
            if (ndjson.length() < chunkSize / 2) {
                // Only these early records carry the sibling scalar that makes "languages.long" a flat key.
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
                ErrorPolicy.STRICT,
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
        assertEquals("a chunk whose records omit the sibling must still decode the flat key, not null it", 0, nulls);
        assertEquals("sum of languages.long", (long) (rows - 1) * rows / 2, sum);
    }

    private static Settings segmentSize64Kb() {
        return Settings.builder().put("esql.datasource.ndjson.segment_size", "64kb").build();
    }
}
