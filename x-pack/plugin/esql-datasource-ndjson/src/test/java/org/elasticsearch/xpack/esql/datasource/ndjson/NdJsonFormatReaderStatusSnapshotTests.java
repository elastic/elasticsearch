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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Verifies that {@link NdJsonFormatReader#statusSnapshot()} reports populated counters after a real
 * read drains an NDJSON file. Complements {@link NdJsonReaderCountersTests} (which exercises the
 * counter struct in isolation) by exercising the full FormatReader → iterator → decoder wiring.
 */
public class NdJsonFormatReaderStatusSnapshotTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
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

        // Snapshot before drain: counters should be at zero, format identifier present.
        Map<String, Object> before = reader.statusSnapshot();
        assertEquals("ndjson", before.get("format"));
        assertEquals(0L, before.get("parse_errors"));
        assertEquals(0L, before.get("read_nanos"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("a", "b"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }

        Map<String, Object> after = reader.statusSnapshot();
        assertEquals("ndjson", after.get("format"));
        assertEquals("no malformed lines in this fixture", 0L, after.get("parse_errors"));
        assertTrue("read_nanos should be > 0 after at least one decodePage call", ((Long) after.get("read_nanos")) > 0);
    }
}
