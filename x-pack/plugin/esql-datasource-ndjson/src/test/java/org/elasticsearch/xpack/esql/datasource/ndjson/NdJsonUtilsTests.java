/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for {@link NdJsonUtils#JSON_FACTORY}.
 *
 * <p>The shared factory carries non-default tuning that the streaming-parallel NDJSON path
 * relies on; pin those settings here so accidental drift (e.g. a refactor to the central
 * {@code ESJsonFactory}) is caught at build time rather than as a runtime regression.
 */
public class NdJsonUtilsTests extends ESTestCase {

    public void testFactoryDisablesAutoCloseSource() {
        assertFalse(
            "AUTO_CLOSE_SOURCE must be off so recovery from JsonParseException does not close a wrapping codec stream",
            NdJsonUtils.JSON_FACTORY.isEnabled(StreamReadFeature.AUTO_CLOSE_SOURCE)
        );
    }

    public void testFactoryEnablesFastDoubleParser() {
        assertTrue(
            "USE_FAST_DOUBLE_PARSER must be on for numeric-column throughput",
            NdJsonUtils.JSON_FACTORY.isEnabled(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
        );
    }

    public void testFactoryDisablesIncludeSourceInLocation() {
        assertFalse(
            "INCLUDE_SOURCE_IN_LOCATION must be off; we never echo the source payload back on parse errors",
            NdJsonUtils.JSON_FACTORY.isEnabled(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
        );
    }

    public void testFactoryDisablesInternFieldNames() {
        assertFalse(
            "INTERN_FIELD_NAMES must be off so parallel parsers do not serialize on String.intern()'s monitor",
            NdJsonUtils.JSON_FACTORY.isEnabled(JsonFactory.Feature.INTERN_FIELD_NAMES)
        );
    }

    /**
     * Behavioural check for {@code AUTO_CLOSE_SOURCE = false}: closing the parser must not
     * close the underlying stream. Schema inference and parse-error recovery rely on this.
     */
    public void testParserCloseDoesNotCloseUnderlyingStream() throws IOException {
        AtomicBoolean closed = new AtomicBoolean(false);
        InputStream raw = new FilterInputStream(new ByteArrayInputStream("{\"a\":1}\n".getBytes(StandardCharsets.UTF_8))) {
            @Override
            public void close() throws IOException {
                closed.set(true);
                super.close();
            }
        };
        try (JsonParser parser = NdJsonUtils.JSON_FACTORY.createParser(raw)) {
            parser.nextToken();
        }
        assertFalse("Closing the parser must not close the wrapping stream when AUTO_CLOSE_SOURCE is disabled", closed.get());
    }
}
