/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.util.JsonParserDelegate;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies the fast-skip restructuring in {@link NdJsonPageDecoder}: field names are
 * read via {@code parser.nextFieldName()}, so field-name advances are not counted as
 * {@code nextToken()} calls and the total {@code nextToken()} count per row drops below the
 * eager-decode baseline (1 START_OBJECT + N FIELD_NAME + N VALUE + 1 END_OBJECT).
 */
public class NdJsonPageDecoderFastSkipTests extends ESTestCase {

    /** 4 columns, 16 rows of monotonically-increasing IDs with string/int/boolean noise columns. */
    private static final int ROWS = 16;
    private static final int COLUMNS = 4; // id (int), name (keyword), age (int), active (boolean)

    private static final List<Attribute> SCHEMA = List.of(
        NdJsonSchemaInferrer.attribute("id", DataType.INTEGER, false),
        NdJsonSchemaInferrer.attribute("name", DataType.KEYWORD, false),
        NdJsonSchemaInferrer.attribute("age", DataType.INTEGER, false),
        NdJsonSchemaInferrer.attribute("active", DataType.BOOLEAN, false)
    );

    private static final List<String> NARROW_PROJECTION = List.of("id");

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * With narrow projection, {@code nextToken()} is called only for the START_OBJECT (outer loop)
     * and for each projected/non-projected field VALUE — field-name advances use
     * {@code nextFieldName()} and are NOT counted. Total per row must be strictly below the
     * all-{@code nextToken()} eager baseline (COLUMNS FIELD_NAME + COLUMNS VALUE + START_OBJECT +
     * END_OBJECT = {@code 2*COLUMNS + 2}).
     */
    public void testNarrowProjectionReducesTokenAdvanceCount() throws IOException {
        AtomicInteger nextTokenCalls = new AtomicInteger();
        JsonFactory countingFactory = countingFactory(nextTokenCalls);

        byte[] ndjson = fixture();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson),
                SCHEMA,
                NARROW_PROJECTION,
                ROWS + 1,
                blockFactory,
                ErrorPolicy.STRICT,
                "test",
                countingFactory
            )
        ) {
            Page page = decoder.decodePage();
            assertNotNull(page);
            try {
                assertEquals(ROWS, page.getPositionCount());
            } finally {
                page.releaseBlocks();
            }
        }

        int eagerBaseline = ROWS * (2 * COLUMNS + 2); // START_OBJECT + N*FIELD_NAME + N*VALUE + END_OBJECT per row
        assertThat(
            "nextToken() count should be below the eager-decode baseline",
            nextTokenCalls.get(),
            org.hamcrest.Matchers.lessThan(eagerBaseline)
        );
    }

    /**
     * The projected "id" column must contain the correct monotonic integer values.
     */
    public void testNarrowProjectionContentIsCorrect() throws IOException {
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
                assertEquals(1, page.getBlockCount());
                assertEquals(ROWS, page.getPositionCount());
                IntBlock ids = (IntBlock) page.getBlock(0);
                for (int i = 0; i < ROWS; i++) {
                    assertEquals("row " + i, i, ids.getInt(i));
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Lenient error policy must also respect the narrow projection and not materialise unprojected
     * columns.
     */
    public void testLenientNarrowProjectionDoesNotMaterializeUnprojected() throws IOException {
        AtomicInteger nextTokenCalls = new AtomicInteger();
        JsonFactory countingFactory = countingFactory(nextTokenCalls);

        byte[] ndjson = fixture();
        try (
            NdJsonPageDecoder decoder = new NdJsonPageDecoder(
                new ByteArrayInputStream(ndjson),
                SCHEMA,
                NARROW_PROJECTION,
                ROWS + 1,
                blockFactory,
                ErrorPolicy.LENIENT,
                "test",
                countingFactory
            )
        ) {
            Page page = decoder.decodePage();
            assertNotNull(page);
            try {
                assertEquals(1, page.getBlockCount());
                assertEquals(ROWS, page.getPositionCount());
                IntBlock ids = (IntBlock) page.getBlock(0);
                for (int i = 0; i < ROWS; i++) {
                    assertEquals("row " + i, i, ids.getInt(i));
                }
            } finally {
                page.releaseBlocks();
            }
        }

        int eagerBaseline = ROWS * (2 * COLUMNS + 2);
        assertThat(
            "nextToken() count under LENIENT should also be below the eager-decode baseline",
            nextTokenCalls.get(),
            org.hamcrest.Matchers.lessThan(eagerBaseline)
        );
    }

    // -----------------------------------------------------------------------------------------

    /** Builds a {@link JsonFactory} that counts every {@code nextToken()} call on its parsers. */
    private static JsonFactory countingFactory(AtomicInteger counter) {
        return new JsonFactory() {
            @Override
            protected JsonParser _createParser(byte[] data, int offset, int len, com.fasterxml.jackson.core.io.IOContext ctxt)
                throws IOException {
                JsonParser base = super._createParser(data, offset, len, ctxt);
                return counting(base, counter);
            }

            @Override
            protected JsonParser _createParser(InputStream in, com.fasterxml.jackson.core.io.IOContext ctxt) throws IOException {
                JsonParser base = super._createParser(in, ctxt);
                return counting(base, counter);
            }
        };
    }

    private static JsonParser counting(JsonParser base, AtomicInteger counter) {
        return new JsonParserDelegate(base) {
            @Override
            public JsonToken nextToken() throws IOException {
                counter.incrementAndGet();
                return super.nextToken();
            }

            /**
             * Route directly to the wrapped parser so the base-class fallback
             * ({@code nextToken() == FIELD_NAME ? currentName() : null}) does not
             * trigger the overridden {@code nextToken()} and double-count field-name
             * advances that the fast-skip path intentionally avoids.
             */
            @Override
            public String nextFieldName() throws IOException {
                return delegate.nextFieldName();
            }
        };
    }

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
}
