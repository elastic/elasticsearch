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
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The seekable coordinator reads a whole file as several segments, and every segment must decode a column the
 * same way.
 *
 * <p>A dotted leaf decodes as a flat key only when the file also has a column of the leaf's prefix, and a bound
 * schema is a projection that routinely omits it. The coordinator therefore resolves the file's own schema once
 * before the segments run. Resolving it per segment instead lets segments disagree — the ones whose rows happen
 * to mention the prefix decode values, the rest return nulls — which is silent, and depends only on where the
 * segment boundaries fell.
 *
 * <p>The sibling here appears only in the first part of the file, so a per-segment answer is wrong for every
 * segment after it. Its counterpart for the streaming coordinator is
 * {@link NdJsonDottedPrefixChunkConsistencyTests}.
 */
public class NdJsonSegmentedDottedPrefixTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testEverySegmentDecodesTheFlatKeyTheSameWay() throws Exception {
        Settings settings = Settings.builder().put("esql.datasource.ndjson.segment_size", "64kb").build();
        NdJsonFormatReader reader = new NdJsonFormatReader(settings, blockFactory);
        long segmentSize = reader.minimumSegmentSize();

        StringBuilder ndjson = new StringBuilder();
        int rows = 0;
        while (ndjson.length() < segmentSize * 3) {
            if (ndjson.length() < segmentSize / 2) {
                // Only these early rows carry the sibling that makes "languages.long" a flat key.
                ndjson.append("{\"languages\":\"en\",\"languages.long\":").append((long) rows).append("}\n");
            } else {
                ndjson.append("{\"languages.long\":").append((long) rows).append("}\n");
            }
            rows++;
        }
        assertTrue("fixture must span several segments", ndjson.length() > segmentSize * 2);

        List<Attribute> bound = List.of(new ReferenceAttribute(Source.EMPTY, "languages.long", DataType.LONG));
        NdJsonFormatReader boundReader = reader.withSchema(bound);
        BytesStorageObject object = new BytesStorageObject("file:///segmented.ndjson", ndjson.toString().getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(4);
        long seenRows = 0;
        long nulls = 0;
        long sum = 0;
        try (
            CloseableIterator<Page> pages = ParallelParsingCoordinator.parallelRead(
                (SegmentableFormatReader) boundReader,
                object,
                List.of("languages.long"),
                1000,
                4, // parallelism must exceed 1 or the file is read as one segment
                executor,
                ErrorPolicy.STRICT,
                false,
                true,
                bound,
                0L
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
        assertEquals("a segment whose rows omit the sibling must still decode the flat key, not null it", 0, nulls);
        assertEquals("sum of languages.long", (long) (rows - 1) * rows / 2, sum);
    }
}
