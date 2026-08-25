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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * Pins {@link NdJsonPageDecoder}'s identity-keyed field-name cache: a probe on
 * {@code children.get(fieldName)} (HashMap fallback) must happen at most once per unique field
 * name, regardless of how many records carry it. Jackson's {@link
 * com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer} returns stable {@code String} instances
 * for repeat names (within one parser's lifetime — which spans every page this decoder emits),
 * so the cache makes the unprojected-field tax flat-per-name rather than per-record.
 * <p>
 * This is the residual the original 105-column ClickBench profile attributed to {@code
 * HashMap.getNode} on the decoder's side; the Jackson-side name tokenisation cost
 * ({@code parseMediumName2}/{@code findName}) is irreducible at the public API surface and is
 * not what this change targets.
 */
public class NdJsonPageDecoderFieldNameCacheTests extends ESTestCase {

    private static final int COLUMNS = 4;
    private static final int ROWS = 32;

    private static final List<Attribute> SCHEMA = List.of(
        NdJsonSchemaInferrer.attribute("id", DataType.INTEGER, false),
        NdJsonSchemaInferrer.attribute("name", DataType.KEYWORD, false),
        NdJsonSchemaInferrer.attribute("age", DataType.INTEGER, false),
        NdJsonSchemaInferrer.attribute("active", DataType.BOOLEAN, false)
    );

    /** Narrow projection: 1 of 4 fields. The other 3 must be probed at most once. */
    private static final List<String> NARROW_PROJECTION = List.of("id");

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * After the decoder has seen a name once, every subsequent record paying that same name must
     * be served from the identity cache. With {@value #COLUMNS} columns the HashMap-fallback
     * counter must equal exactly {@value #COLUMNS} after all {@value #ROWS} rows decode (one
     * probe per unique name on the first record, zero on every record after).
     */
    public void testNarrowProjectionCachesFieldNamesAcrossRecords() throws IOException {
        byte[] ndjson = fixture();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson),
                null, // DateFormatter
                SCHEMA,
                NARROW_PROJECTION,
                ROWS + 1,
                blockFactory,
                ErrorPolicy.STRICT,
                "test",
                new NdJsonReaderCounters()
            )
        ) {
            Page page = decoder.decodePage();
            assertNotNull(page);
            try {
                assertEquals(ROWS, page.getPositionCount());
            } finally {
                page.releaseBlocks();
            }
            // Exactly one HashMap fallback per unique field name. The other (ROWS-1) * COLUMNS
            // field-name probes must all be served from the identity cache.
            assertEquals(
                "every unique field name should be probed against the slow HashMap at most once",
                COLUMNS,
                decoder.hashMapFallbacks()
            );
        }
    }

    /**
     * Same invariant under {@link ErrorPolicy#LENIENT}: lenient decode uses the same field-name
     * loop, so the cache must do the same work. The page-build path differs (scratch row
     * builders) but {@code decodeObject} does not.
     */
    public void testLenientCachesFieldNamesAcrossRecords() throws IOException {
        byte[] ndjson = fixture();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson),
                null, // DateFormatter
                SCHEMA,
                NARROW_PROJECTION,
                ROWS + 1,
                blockFactory,
                ErrorPolicy.LENIENT,
                "test",
                new NdJsonReaderCounters()
            )
        ) {
            Page page = decoder.decodePage();
            assertNotNull(page);
            try {
                assertEquals(ROWS, page.getPositionCount());
            } finally {
                page.releaseBlocks();
            }
            assertEquals(COLUMNS, decoder.hashMapFallbacks());
        }
    }

    /**
     * The cache must survive page boundaries: each {@code decodePage()} call shares the same
     * underlying {@link com.fasterxml.jackson.core.JsonParser} (and therefore the same
     * canonicaliser child), so the {@code String} identities seen on page 1 must match those on
     * page 2. A regression that, for example, allocates a fresh identity cache per page would
     * make the fallback counter grow linearly in the number of pages.
     */
    public void testCacheSurvivesPageBoundaries() throws IOException {
        byte[] ndjson = fixture();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson),
                null, // DateFormatter
                SCHEMA,
                NARROW_PROJECTION,
                4,
                blockFactory,
                ErrorPolicy.STRICT,
                "test",
                new NdJsonReaderCounters()
            )
        ) {
            int totalRows = 0;
            Page page;
            while ((page = decoder.decodePage()) != null) {
                try {
                    totalRows += page.getPositionCount();
                } finally {
                    page.releaseBlocks();
                }
            }
            assertEquals(ROWS, totalRows);
            assertEquals(
                "the identity cache spans every page the decoder produces, so fallback count must stay at COLUMNS",
                COLUMNS,
                decoder.hashMapFallbacks()
            );
        }
    }

    /**
     * The byte-array constructor takes the same {@code decodeObject} loop as the
     * {@link java.io.InputStream}-based one, so the cache must behave identically. Pinned
     * because the streaming-parallel hot path (every chunk handed to the decoder as a bounded
     * {@code byte[]}) goes through that constructor and is the workload the optimisation
     * actually matters for.
     */
    public void testByteArrayConstructorCachesFieldNamesAcrossRecords() throws IOException {
        byte[] ndjson = fixture();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                ndjson,
                0,
                ndjson.length,
                null, // DateFormatter
                SCHEMA,
                NARROW_PROJECTION,
                ROWS + 1,
                blockFactory,
                ErrorPolicy.STRICT,
                "test",
                new NdJsonReaderCounters()
            )
        ) {
            Page page = decoder.decodePage();
            assertNotNull(page);
            try {
                assertEquals(ROWS, page.getPositionCount());
            } finally {
                page.releaseBlocks();
            }
            assertEquals(COLUMNS, decoder.hashMapFallbacks());
        }
    }

    /**
     * Each level of the decoder tree owns its own identity cache. With nested projection
     * {@code user.id} against records shaped {@code {"user":{"id":..,"name":..},"other":..}}
     * the root decoder probes 2 unique names ({@code user}, {@code other}) and the inner
     * {@code user} decoder probes 2 ({@code id}, {@code name}) — 4 fallbacks total, again
     * regardless of record count.
     */
    public void testNestedProjectionCachesPerSubtree() throws IOException {
        List<Attribute> nestedSchema = List.of(
            NdJsonSchemaInferrer.attribute("user.id", DataType.INTEGER, false),
            NdJsonSchemaInferrer.attribute("user.name", DataType.KEYWORD, false),
            NdJsonSchemaInferrer.attribute("other", DataType.KEYWORD, false)
        );
        List<String> nestedProjection = List.of("user.id");
        byte[] ndjson = nestedFixture();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson),
                null, // DateFormatter
                nestedSchema,
                nestedProjection,
                ROWS + 1,
                blockFactory,
                ErrorPolicy.STRICT,
                "test",
                new NdJsonReaderCounters()
            )
        ) {
            Page page = decoder.decodePage();
            assertNotNull(page);
            try {
                assertEquals(ROWS, page.getPositionCount());
            } finally {
                page.releaseBlocks();
            }
            // Root: {user, other} = 2 unique. Inner "user" decoder: {id, name} = 2 unique. Total = 4.
            assertEquals(
                "each BlockDecoder level owns an independent identity cache; total fallbacks = unique names per level summed",
                4,
                decoder.hashMapFallbacks()
            );
        }
    }

    /**
     * Dynamic-key NDJSON (per-row varying field names: tenant-keyed columns, embedded event
     * ids, sparse extensions) must not grow the identity cache without bound. The cache caps at
     * {@code max(IDENTITY_CACHE_MIN_CAP, children.size() * IDENTITY_CACHE_FANOUT_MULT)} entries
     * per decoder; past the bound, new names fall through to the {@code HashMap} every
     * occurrence. Correctness must hold (the projected {@code id} column is decoded for every
     * row) and the root cache size must stay within the bound regardless of how many unique
     * unprojected names the input carries.
     */
    public void testDynamicKeyInputCapsIdentityCacheSize() throws IOException {
        int uniquePerRecordExtras = 512;
        int rows = 4;
        List<Attribute> schema = List.of(NdJsonSchemaInferrer.attribute("id", DataType.INTEGER, false));
        List<String> projection = List.of("id");
        byte[] ndjson = dynamicKeyFixture(rows, uniquePerRecordExtras);
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson),
                null, // DateFormatter
                schema,
                projection,
                rows + 1,
                blockFactory,
                ErrorPolicy.STRICT,
                "test",
                new NdJsonReaderCounters()
            )
        ) {
            Page page = decoder.decodePage();
            assertNotNull(page);
            try {
                assertEquals(rows, page.getPositionCount());
            } finally {
                page.releaseBlocks();
            }
            // Root schema fanout is 1 ({@code id}), so the cache bound is the floor (256). Without
            // a bound the cache would hold 1 + uniquePerRecordExtras * rows entries (~2049). The
            // cap must keep it well under that.
            int bound = Math.max(256, /* children.size() = */ 1 * 4);
            assertThat(
                "identity cache must be capped on dynamic-key inputs",
                decoder.rootIdentityCacheSize(),
                Matchers.lessThanOrEqualTo(bound)
            );
            // And the cap must actually have engaged here (more unique names than the bound).
            assertThat(
                "test fixture must produce more unique names than the cap so the bound is exercised",
                1 + uniquePerRecordExtras * rows,
                Matchers.greaterThan(bound)
            );
        }
    }

    // -----------------------------------------------------------------------------------------

    /** Generates {@value #ROWS} NDJSON lines with {@value #COLUMNS} fields each. */
    private static byte[] fixture() throws IOException {
        var sw = new StringWriter();
        try (PrintWriter w = new PrintWriter(sw)) {
            for (int i = 0; i < ROWS; i++) {
                w.format(
                    Locale.ROOT,
                    "{\"id\":%d,\"name\":\"name-%d\",\"age\":%d,\"active\":%s}%n",
                    i,
                    i,
                    20 + i,
                    i % 2 == 0 ? "true" : "false"
                );
            }
        }
        return sw.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Generates {@value #ROWS} NDJSON lines shaped {@code {"user":{"id":i,"name":"x-i"},"other":"y-i"}}. */
    private static byte[] nestedFixture() throws IOException {
        var sw = new StringWriter();
        try (PrintWriter w = new PrintWriter(sw)) {
            for (int i = 0; i < ROWS; i++) {
                w.format(Locale.ROOT, "{\"user\":{\"id\":%d,\"name\":\"u-%d\"},\"other\":\"o-%d\"}%n", i, i, i);
            }
        }
        return sw.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generates {@code rows} NDJSON lines each shaped
     * {@code {"id":i,"dyn_<r>_0":...,"dyn_<r>_<extras-1>":...}}, so every record carries the
     * projected {@code id} plus a fresh batch of unique unprojected field names (no name
     * repeats across records). Exercises the identity-cache bound.
     */
    private static byte[] dynamicKeyFixture(int rows, int extrasPerRow) throws IOException {
        var sw = new StringWriter();
        try (PrintWriter w = new PrintWriter(sw)) {
            for (int r = 0; r < rows; r++) {
                var sb = new StringBuilder().append("{\"id\":").append(r);
                for (int e = 0; e < extrasPerRow; e++) {
                    sb.append(",\"dyn_").append(r).append('_').append(e).append("\":").append(e);
                }
                sb.append('}');
                w.println(sb);
            }
        }
        return sw.toString().getBytes(StandardCharsets.UTF_8);
    }
}
