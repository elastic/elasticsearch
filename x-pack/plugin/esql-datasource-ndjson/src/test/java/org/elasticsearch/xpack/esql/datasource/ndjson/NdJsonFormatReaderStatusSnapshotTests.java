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
 * It also pins the COUNTER LIFETIME invariant: every ordinary wither ({@code withSchema},
 * {@code withDeclaredDateFormats}, {@code withReadConfig}, …) preserves the parent's counter struct.
 * {@link NdJsonFormatReader#withFreshCounters()} is the only method that mints a new struct, and the
 * operator factory is its only caller — once per {@code factory.get(DriverContext)} invocation. Two
 * operators from the same factory therefore own two isolated counter structs, so Σ across all operators
 * in a query equals the query total. Only tests using {@code withFreshCounters()} demonstrate isolation;
 * tests using ordinary withers demonstrate sharing.
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
        assertEquals(0L, before.rowsEmitted());

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("a", "b"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }

        var after = reader.statusSnapshot();
        assertEquals("ndjson", after.format());
        assertEquals("no malformed lines in this fixture", 0L, after.parseErrors());
        assertEquals("3 rows in fixture", 3L, after.rowsEmitted());
    }

    public void testSiblingQueryReadersDoNotShareCounters() throws IOException {
        // Simulate two operator mints from the same factory: factory.get() calls withFreshCounters() once per operator.
        var base = new NdJsonFormatReader(null, blockFactory);
        var first = base.withFreshCounters();
        var second = base.withFreshCounters();

        drain(first);

        assertTrue("the minted reader that read must report its own work", first.statusSnapshot().rowsEmitted() > 0);
        assertEquals("a sibling minted reader must not see it", 0L, second.statusSnapshot().rowsEmitted());
        assertEquals("nor may it reach the registry's shared reader", 0L, base.statusSnapshot().rowsEmitted());
    }

    public void testWithinScopeSchemaWitherSharesCounters() throws IOException {
        // Within one operator scope, withSchema preserves the parent's counter struct — no fresh mint.
        var minted = new NdJsonFormatReader(null, blockFactory).withFreshCounters();
        var scoped = minted.withSchema(SCHEMA);

        drain(scoped);

        assertTrue(scoped.statusSnapshot().rowsEmitted() > 0);
        assertEquals(
            "withSchema shares counters within the operator scope: minted reader observes the work scoped reader did",
            scoped.statusSnapshot().rowsEmitted(),
            minted.statusSnapshot().rowsEmitted()
        );
    }

    public void testWithinScopeDateFormatWitherSharesCounters() throws IOException {
        // Within one operator scope, withDeclaredDateFormats preserves the parent's counter struct — no fresh mint.
        var minted = new NdJsonFormatReader(null, blockFactory).withFreshCounters();
        var scoped = minted.withDeclaredDateFormats(Map.of("b", "yyyy-MM-dd"));

        drain(scoped);

        assertTrue(scoped.statusSnapshot().rowsEmitted() > 0);
        assertEquals(
            "withDeclaredDateFormats shares counters within the operator scope: minted reader observes the work scoped reader did",
            scoped.statusSnapshot().rowsEmitted(),
            minted.statusSnapshot().rowsEmitted()
        );
    }

    public void testPerFileReadConfigCopyReportsThroughItsParent() throws IOException {
        var query = new NdJsonFormatReader(null, blockFactory).withSchema(SCHEMA);
        var perFile = query.withReadConfig("0123456789abcdef0123456789abcdef");

        drain(perFile);

        assertTrue(
            "withReadConfig runs per file, below the reader the status envelope snapshots, so its work must land"
                + " in the parent — a fork here is the zero-rowsEmitted defect",
            query.statusSnapshot().rowsEmitted() > 0
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
