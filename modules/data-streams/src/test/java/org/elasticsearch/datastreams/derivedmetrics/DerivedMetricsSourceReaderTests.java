/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

public class DerivedMetricsSourceReaderTests extends ESTestCase {

    public void testReadsConfiguredPathsAndNothingElse() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        int service = paths.slotFor("service.name");
        int status = paths.slotFor("http.response.status_code");

        Object[] values = read(
            paths,
            Map.of(
                "service",
                Map.of("name", "checkout", "version", "4.1"),
                "http",
                Map.of("response", Map.of("status_code", 503, "bytes", 91)),
                "message",
                "something went wrong"
            )
        );

        assertEquals("checkout", DerivedMetricsSourceReader.stringValue(values, service));
        assertEquals(503.0, DerivedMetricsSourceReader.numericValue(values, status), 0.0);
        assertEquals("only the configured paths are extracted", 2, values.length);
    }

    /**
     * A source may write a nested path either as nested objects or as one dotted field name. Both mean the same path to a mapping, so
     * both have to mean the same path here.
     */
    public void testDottedFieldNamesResolveToTheSamePath() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        int slot = paths.slotFor("service.name");

        assertEquals("checkout", DerivedMetricsSourceReader.stringValue(read(paths, Map.of("service.name", "checkout")), slot));
        assertEquals("checkout", DerivedMetricsSourceReader.stringValue(read(paths, Map.of("service", Map.of("name", "checkout"))), slot));
    }

    public void testAbsentPathsAreLeftNull() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        int slot = paths.slotFor("service.name");
        assertNull(DerivedMetricsSourceReader.stringValue(read(paths, Map.of("host", Map.of("name", "host-1"))), slot));
    }

    /**
     * A path can name both a value and the root of deeper values, and reading one must not stop the other being found.
     */
    public void testAPathCanBeBothAValueAndAPrefix() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        int host = paths.slotFor("host");
        int hostName = paths.slotFor("host.name");

        Object[] values = read(paths, Map.of("host", Map.of("name", "host-1")));
        // the path names an object, which is not usable as a dimension value
        assertNull(DerivedMetricsSourceReader.stringValue(values, host));
        assertEquals("host-1", DerivedMetricsSourceReader.stringValue(values, hostName));
    }

    /**
     * Multi-valued fields are kept as lists so predicates can match any element, but they are not a single dimension value.
     */
    public void testMultiValuedFieldsAreKeptButAreNotDimensionValues() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        int slot = paths.slotFor("tags");

        Object[] values = read(paths, Map.of("tags", List.of("alpha", "beta")));
        assertEquals(List.of("alpha", "beta"), values[slot]);
        assertNull(DerivedMetricsSourceReader.stringValue(values, slot));
    }

    public void testValuesBelowAnArrayOfObjectsAreStillFound() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        int slot = paths.slotFor("host.name");
        Object[] values = read(paths, Map.of("host", List.of(Map.of("name", "host-1"))));
        assertEquals("host-1", DerivedMetricsSourceReader.stringValue(values, slot));
    }

    public void testNumericValuesAreLenientAboutHowTheyWereWritten() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        int slot = paths.slotFor("event.duration");

        assertEquals(1500.0, DerivedMetricsSourceReader.numericValue(read(paths, Map.of("event", Map.of("duration", 1500))), slot), 0.0);
        assertEquals(1500.0, DerivedMetricsSourceReader.numericValue(read(paths, Map.of("event", Map.of("duration", "1500"))), slot), 0.0);
        assertTrue(Double.isNaN(DerivedMetricsSourceReader.numericValue(read(paths, Map.of("event", Map.of("duration", "soon"))), slot)));
        assertTrue(Double.isNaN(DerivedMetricsSourceReader.numericValue(read(paths, Map.of()), slot)));
    }

    /**
     * A document that cannot be parsed must never fail the write it is derived from.
     */
    public void testAMalformedDocumentIsReportedRatherThanThrown() {
        DerivedMetricsSourcePaths paths = new DerivedMetricsSourcePaths();
        paths.slotFor("service.name");
        ParsedDocument document = parsedDocument(new BytesArray("{\"service\": "));
        assertFalse(DerivedMetricsSourceReader.read(document, paths, new Object[paths.size()]));
    }

    private static Object[] read(DerivedMetricsSourcePaths paths, Map<String, Object> source) {
        Object[] values = new Object[paths.size()];
        assertTrue(DerivedMetricsSourceReader.read(parsedDocument(source), paths, values));
        return values;
    }

    /**
     * Shared with {@link DerivedMetricsPredicateTests}, which evaluates predicates against documents read by this reader rather than
     * against a hand-built map.
     */
    static ParsedDocument parsedDocument(Map<String, Object> source) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(source);
            return parsedDocument(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ParsedDocument parsedDocument(BytesReference source) {
        return new ParsedDocument(
            null,
            null,
            "doc-1",
            null,
            List.of(new LuceneDocument()),
            SourceToParse.Source.fromBytes(source, XContentType.JSON),
            null,
            0L
        );
    }
}
