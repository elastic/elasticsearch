/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Verifies that counters passed via {@link FormatReadContext#readCounters()} are populated after a real
 * read drains an NDJSON file. Complements {@link NdJsonReaderCountersTests} (which exercises the
 * counter struct in isolation) by exercising the full FormatReader → iterator → decoder wiring.
 */
public class NdJsonFormatReaderStatusSnapshotTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testCountersPopulatedAfterDrain() throws IOException {
        String ndjson = """
            {"a": 1, "b": "x"}
            {"a": 2, "b": "y"}
            {"a": 3, "b": "z"}
            """;
        var object = new BytesStorageObject("memory://snapshot-test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);
        NdJsonReaderCounters counters = (NdJsonReaderCounters) reader.newReadCounters();

        FormatReadContext context = FormatReadContext.builder()
            .projectedColumns(List.of("a", "b"))
            .batchSize(10)
            .readCounters(counters)
            .build();
        try (CloseableIterator<Page> iterator = reader.read(object, context)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }

        var snapshot = counters.snapshot();
        assertEquals("no malformed lines in this fixture", 0L, snapshot.parseErrors());
        assertEquals("3 rows in fixture", 3L, snapshot.rowsEmitted());
    }

    public void testSiblingQueryReadersHaveIsolatedCounters() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        NdJsonReaderCounters firstCounters = (NdJsonReaderCounters) reader.newReadCounters();
        NdJsonReaderCounters secondCounters = (NdJsonReaderCounters) reader.newReadCounters();

        drain(reader, firstCounters);

        assertTrue("the reader that ran must report its own work", firstCounters.snapshot().rowsEmitted() > 0);
        assertEquals("the sibling counters must not see it", 0L, secondCounters.snapshot().rowsEmitted());
    }

    public void testWithinScopeSchemaWitherUsesTheSameCounters() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        NdJsonReaderCounters counters = (NdJsonReaderCounters) reader.newReadCounters();
        var scoped = reader.withSchema(SCHEMA);

        drain(scoped, counters);

        assertTrue("scoped reader must have emitted rows", counters.snapshot().rowsEmitted() > 0);
    }

    public void testWithinScopeDateFormatWitherUsesTheSameCounters() throws IOException {
        var reader = new NdJsonFormatReader(null, blockFactory);
        NdJsonReaderCounters counters = (NdJsonReaderCounters) reader.newReadCounters();
        var scoped = reader.withDeclaredDateFormats(Map.of("b", "yyyy-MM-dd"));

        drain(scoped, counters);

        assertTrue("scoped reader must have emitted rows", counters.snapshot().rowsEmitted() > 0);
    }

    public void testPerFileReadConfigCopyUsesTheSameCounters() throws IOException {
        var query = new NdJsonFormatReader(null, blockFactory).withSchema(SCHEMA);
        var perFile = query.withReadConfig("0123456789abcdef0123456789abcdef");
        NdJsonReaderCounters counters = (NdJsonReaderCounters) query.newReadCounters();

        drain(perFile, counters);

        assertTrue(
            "withReadConfig runs per file; passing the same counters object threads work through to the caller",
            counters.snapshot().rowsEmitted() > 0
        );
    }

    private static final List<Attribute> SCHEMA = List.of(
        new ReferenceAttribute(Source.EMPTY, null, "a", DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, null, "b", DataType.KEYWORD)
    );

    private void drain(NdJsonFormatReader reader, NdJsonReaderCounters counters) throws IOException {
        String ndjson = """
            {"a": 1, "b": "x"}
            {"a": 2, "b": "y"}
            {"a": 3, "b": "z"}
            """;
        var object = new BytesStorageObject("memory://lifetime-test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        FormatReadContext context = FormatReadContext.builder()
            .projectedColumns(List.of("a", "b"))
            .batchSize(10)
            .readCounters(counters)
            .build();
        try (CloseableIterator<Page> iterator = reader.read(object, context)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }
    }
}
