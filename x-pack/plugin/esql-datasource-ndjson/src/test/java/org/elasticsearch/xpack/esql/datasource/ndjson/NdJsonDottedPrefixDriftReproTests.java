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
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** REVIEW REPRO (temporary). */
public class NdJsonDottedPrefixDriftReproTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /** Minimal: whole file of flat-only rows, bound schema names only the dotted leaf. */
    public void testFlatOnlyRowsDirectRead() throws Exception {
        String ndjson = "{\"languages.long\":1}\n{\"languages.long\":2}\n";
        NdJsonFormatReader reader = new NdJsonFormatReader(null, blockFactory);
        List<Attribute> bound = List.of(new ReferenceAttribute(Source.EMPTY, "languages.long", DataType.LONG));
        StorageObject object = new BytesStorageObject("file:///flat.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        long nulls = 0;
        long values = 0;
        try (
            CloseableIterator<Page> pages = reader.read(
                object,
                FormatReadContext.builder().projectedColumns(List.of("languages.long")).batchSize(10).readSchema(bound).build()
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
                            values++;
                        }
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        logger.info("--> flat-only direct read: values={}, nulls={}", values, nulls);
        assertEquals("flat-only rows must decode the dotted leaf", 2, values);
    }

    /** Heterogeneous rows across chunks through the streaming coordinator. */
    public void testHeterogeneousRowsDecodeConsistentlyAcrossChunks() throws Exception {
        long chunkSize = new NdJsonFormatReader(segmentSize64Kb(), blockFactory).minimumSegmentSize();

        StringBuilder ndjson = new StringBuilder();
        int rows = 0;
        while (ndjson.length() < chunkSize) {
            ndjson.append("{\"languages\":\"en\",\"languages.long\":1}\n");
            rows++;
        }
        while (ndjson.length() < chunkSize * 3) {
            ndjson.append("{\"languages.long\":1}\n");
            rows++;
        }

        List<Attribute> bound = List.of(new ReferenceAttribute(Source.EMPTY, "languages.long", DataType.LONG));
        NdJsonFormatReader reader = new NdJsonFormatReader(segmentSize64Kb(), blockFactory).withSchema(bound);

        InputStream stream = new ByteArrayInputStream(ndjson.toString().getBytes(StandardCharsets.UTF_8));
        ExecutorService executor = Executors.newFixedThreadPool(4);
        long seenRows = 0;
        long nulls = 0;
        try (
            CloseableIterator<Page> pages = StreamingParallelParsingCoordinator.parallelRead(
                (SegmentableFormatReader) reader,
                stream,
                null,
                List.of("languages.long"),
                1000,
                4,
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

        fail("REPRO NUMBERS: rows=" + rows + " seen=" + seenRows + " nulls=" + nulls);
    }

    private static Settings segmentSize64Kb() {
        return Settings.builder().put("esql.datasource.ndjson.segment_size", "64kb").build();
    }
}
