/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

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
     * {@link NdJsonPageDecoder}'s identity-keyed field-name cache only avoids the {@code
     * HashMap} probe when {@link com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer} returns
     * stable {@code String} instances per name across records. That stability is the default and
     * is driven by {@link JsonFactory.Feature#CANONICALIZE_FIELD_NAMES} being on; pin it here so
     * a future tuning change has to surface the consequence explicitly.
     */
    public void testFactoryEnablesCanonicalizeFieldNames() {
        assertTrue(
            "CANONICALIZE_FIELD_NAMES must be on; NdJsonPageDecoder's identity-keyed field-name cache depends on it",
            NdJsonUtils.JSON_FACTORY.isEnabled(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
        );
    }

    public void testFactoryEnablesStrictDuplicateDetection() {
        assertTrue(
            "STRICT_DUPLICATE_DETECTION must be on so a record naming one field twice is rejected rather than merged",
            NdJsonUtils.JSON_FACTORY.isEnabled(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        );
    }

    /**
     * Behavioural check for {@code STRICT_DUPLICATE_DETECTION = true}, and the pin for the message text that
     * {@code NdJsonPageDecoder}'s failure-kind label matches on. Jackson gives duplicate keys and syntax errors
     * the same exception type, so an upgrade that rewords this would silently relabel the failure "Malformed".
     */
    public void testParserRejectsRepeatedFieldNameInOneObject() throws IOException {
        try (JsonParser parser = NdJsonUtils.JSON_FACTORY.createParser("{\"a.b\":1,\"a.b\":2}")) {
            JsonParseException e = expectThrows(JsonParseException.class, () -> {
                while (parser.nextToken() != null) {
                    // drain: the rejection lands on the repeated name, not at the end of the object
                }
            });
            assertThat(e.getOriginalMessage(), startsWith("Duplicate field"));
            assertThat(e.getOriginalMessage(), containsString("a.b"));
        }
    }

    /**
     * The scope of the rejection above: a repeat is per object instance, so the same name in two elements of an
     * array is not one. {@code NdJsonPageDecoder} merges those into a multivalue, so a false positive here would
     * drop ordinary records.
     */
    public void testParserAcceptsSameFieldNameInDifferentObjects() throws IOException {
        try (JsonParser parser = NdJsonUtils.JSON_FACTORY.createParser("{\"a\":[{\"b\":1},{\"b\":2}]}")) {
            while (parser.nextToken() != null) {
                // drain: no rejection expected
            }
        }
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
