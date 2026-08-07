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
 * A flat dotted key such as {@code "languages.long"} is decoded as a single JSON field only when its
 * prefix column ({@code "languages"}) is also in the effective schema; otherwise the decoder reads it as
 * the nested path {@code languages -> long}. On a file that carries both the flat key and a sibling scalar
 * {@code "languages"}, that nested reading walks into the scalar as if it were an object, which fails the
 * query under a strict policy and null-fills the column under a lenient one.
 *
 * <p>The prefix reaches the effective schema on the streaming path only if the coordinator infers the
 * file's schema from its first chunk. When the planner has bound a read schema that lists the dotted leaf
 * but not the prefix, the coordinator skips that inference, so the prefix is absent and the flat key is
 * misread. This drives the real reader through the real streaming coordinator over a genuinely multi-chunk
 * stream to state the contract: the bound dotted-leaf column must still decode to its flat-key value.
 */
public class NdJsonDottedPrefixStreamingTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testBoundDottedLeafDecodesFlatKeyAcrossChunks() throws Exception {
        long chunkSize = new NdJsonFormatReader(segmentSize64Kb(), blockFactory).minimumSegmentSize();

        // Flat dotted key "languages.long" beside a sibling scalar "languages". The sibling is what makes
        // a first-chunk inference produce the "languages" prefix column, and what turns the misread into a
        // hard scalar-vs-object failure rather than a silent null.
        StringBuilder ndjson = new StringBuilder();
        int rows = 0;
        while (ndjson.length() < chunkSize * 2) {
            ndjson.append("{\"languages\":\"en\",\"languages.long\":").append((long) rows).append("}\n");
            rows++;
        }
        assertTrue("fixture must span more than one chunk", ndjson.length() > chunkSize);

        // The planner-bound schema and the reader's configured schema both project only the dotted leaf,
        // without the "languages" prefix.
        List<Attribute> bound = List.of(new ReferenceAttribute(Source.EMPTY, "languages.long", DataType.LONG));
        NdJsonFormatReader reader = new NdJsonFormatReader(segmentSize64Kb(), blockFactory).withSchema(bound);

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
                4, // parallelism must exceed 1 or the serial path reads the whole file as one chunk
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

        assertEquals("every row must be read across every chunk", rows, seenRows);
        assertEquals("the bound dotted leaf must decode its flat-key value, not null", 0, nulls);
        assertEquals("sum of languages.long", (long) (rows - 1) * rows / 2, sum);
    }

    private static Settings segmentSize64Kb() {
        return Settings.builder().put("esql.datasource.ndjson.segment_size", "64kb").build();
    }
}
