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
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Verifies that {@link NdJsonFormatReader#statusSnapshot()} reports populated counters after a real
 * read drains an NDJSON file. Complements {@link NdJsonReaderCountersTests} (which exercises the
 * counter struct in isolation) by exercising the full FormatReader → iterator → decoder wiring.
 * <p>
 * It also pins the COUNTER LIFETIME, from both directions. The chain's root is the node-lifetime reader the
 * format registry hands out, so a wither that shares its parent's counters shares them for the life of the node:
 * sharing above the per-query seam mixes every concurrent query's telemetry into one set, and NOT sharing at the
 * per-file seam leaves the reader that reads reporting into a copy nobody snapshots. Both are silent — the
 * counters are write-only telemetry — so only a test with two live copies can tell the two apart, which is why
 * the pins below assert an untouched sibling is still ZERO rather than only that a drained reader is non-zero.
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

        // Snapshot before drain: counters should be at zero, format identifier present.
        var before = reader.statusSnapshot();
        assertEquals("ndjson", before.format());
        assertEquals(0L, before.parseErrors());
        assertEquals(0L, before.readNanos());

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("a", "b"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }

        var after = reader.statusSnapshot();
        assertEquals("ndjson", after.format());
        assertEquals("no malformed lines in this fixture", 0L, after.parseErrors());
        assertTrue("read_nanos should be > 0 after at least one decodePage call", after.readNanos() > 0);
    }

    public void testSiblingQueryReadersDoNotShareCounters() throws IOException {
        var base = new NdJsonFormatReader(null, blockFactory);
        var first = (NdJsonFormatReader) base.withConfigTrackingConsumedKeys(Map.of("schema_sample_size", 64)).value();
        var second = (NdJsonFormatReader) base.withConfigTrackingConsumedKeys(Map.of("schema_sample_size", 64)).value();

        drain(first);

        assertTrue("the reader that read must report its own work", first.statusSnapshot().readNanos() > 0);
        assertEquals("a sibling query's reader must not see it", 0L, second.statusSnapshot().readNanos());
        assertEquals("nor may it reach the registry's shared reader", 0L, base.statusSnapshot().readNanos());
    }

    public void testQueryLevelSchemaWitherDoesNotLeakIntoTheSharedReader() throws IOException {
        var base = new NdJsonFormatReader(null, blockFactory);
        var scoped = base.withSchema(SCHEMA);

        drain(scoped);

        assertTrue(scoped.statusSnapshot().readNanos() > 0);
        assertEquals("withSchema resolves per query, so it must fork", 0L, base.statusSnapshot().readNanos());
    }

    public void testQueryLevelDateFormatWitherDoesNotLeakIntoTheSharedReader() throws IOException {
        var base = new NdJsonFormatReader(null, blockFactory);
        var scoped = base.withDeclaredDateFormats(Map.of("b", "yyyy-MM-dd"));

        drain(scoped);

        assertTrue(scoped.statusSnapshot().readNanos() > 0);
        assertEquals("declared date formats resolve per query, so this wither must fork too", 0L, base.statusSnapshot().readNanos());
    }

    public void testPerFileReadConfigCopyReportsThroughItsParent() throws IOException {
        var query = new NdJsonFormatReader(null, blockFactory).withSchema(SCHEMA);
        var perFile = query.withReadConfig("0123456789abcdef0123456789abcdef");

        drain(perFile);

        assertTrue(
            "withReadConfig runs per file, below the reader the status envelope snapshots, so its work must land"
                + " in the parent — a fork here is the zero-read-time defect",
            query.statusSnapshot().readNanos() > 0
        );
    }

    private static final List<Attribute> SCHEMA = List.of(
        new ReferenceAttribute(Source.EMPTY, null, "a", DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, null, "b", DataType.KEYWORD)
    );

    private void drain(NdJsonFormatReader reader) throws IOException {
        String ndjson = """
            {"a": 1, "b": "x"}
            {"a": 2, "b": "y"}
            {"a": 3, "b": "z"}
            """;
        var object = new BytesStorageObject("memory://lifetime-test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        try (CloseableIterator<Page> iterator = reader.read(object, List.of("a", "b"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }
    }
}
