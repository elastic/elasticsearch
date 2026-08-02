/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDocumentReader.Strategies;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDocumentReader.Strategy;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * The optimisation these tests guard is only worth having if it is invisible: reading a value out of the already-parsed document must
 * produce exactly what re-parsing {@code _source} would have produced, or a metric silently changes meaning depending on which reader
 * happened to run. So every test here asserts against the source reader rather than against a hand-written expectation.
 */
public class DerivedMetricsDocumentReaderTests extends MapperServiceTestCase {

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        // a real lowercase normalizer, so the normalizer test exercises the mapping it claims to
        return IndexAnalyzers.of(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.of("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())),
            Map.of()
        );
    }

    /**
     * The mapping almost every ECS-shaped stream has. Dimensions are keywords, so they are read straight from the parsed document.
     */
    public void testKeywordDimensionsAreReadFromTheParsedDocument() throws IOException {
        MapperService mappers = createMapperService(mapping(b -> {
            b.startObject("service.name").field("type", "keyword").endObject();
            b.startObject("cloud.region").field("type", "keyword").endObject();
        }));

        assertReadsSameAsSource(mappers, List.of("service.name", "cloud.region"), """
            {"service.name":"checkout","cloud.region":"eu-west-1"}""");
    }

    /**
     * The default dynamic mapping makes a string {@code text} with a {@code .keyword} sub-field. The parent holds the raw string the
     * parser saw — analysis happens later, inside the index writer — so the dimension is readable without touching the sub-field, and
     * therefore without inheriting its {@code ignore_above}.
     */
    public void testATextFieldStillYieldsTheOriginalString() throws IOException {
        MapperService mappers = createMapperService(mapping(b -> {
            b.startObject("service.name");
            b.field("type", "text");
            b.startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject().endObject();
            b.endObject();
        }));

        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), List.of("service.name"));
        assertEquals(Strategy.TEXT, strategies.bySlot()[0]);
        // deliberately mixed case: a text field is not analysed at parse time, so this must survive verbatim
        assertReadsSameAsSource(mappers, List.of("service.name"), """
            {"service.name":"CheckOut-Service"}""");
    }

    /**
     * A double is stored through a sortable-long encoding rather than as itself. Reading it back without decoding produces a plausible
     * and completely wrong number, which for a metric value is the worst possible failure.
     */
    public void testNumericValuesSurviveTheirStorageEncoding() throws IOException {
        MapperService mappers = createMapperService(mapping(b -> {
            b.startObject("event.duration").field("type", "double").endObject();
            b.startObject("http.response.status_code").field("type", "long").endObject();
        }));

        List<String> paths = List.of("event.duration", "http.response.status_code");
        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), paths);
        assertEquals(Strategy.DOUBLE, strategies.bySlot()[0]);
        assertEquals(Strategy.LONG, strategies.bySlot()[1]);

        Object[] values = readFromDocument(mappers, paths, """
            {"event.duration":18374.652,"http.response.status_code":503}""");
        assertEquals(18374.652, DerivedMetricsSourceReader.numericValue(values, 0), 1e-9);
        assertEquals(503.0, DerivedMetricsSourceReader.numericValue(values, 1), 0.0);
    }

    /**
     * A normalizer rewrites the value before it is stored, so reading the index would hand back something the document never contained
     * and quietly split or merge series. Rare and always deliberate, but never safe to read.
     */
    public void testAKeywordWithANormalizerIsRefused() throws IOException {
        MapperService mappers = createMapperService(mapping(b -> {
            b.startObject("service.name").field("type", "keyword").field("normalizer", "lowercase").endObject();
        }));

        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), List.of("service.name"));
        assertEquals(Strategy.UNSUPPORTED, strategies.bySlot()[0]);
        assertFalse("one unreadable path must send the whole document back to _source", strategies.complete());
    }

    /**
     * An over-long value is not stored at all, and absent-because-too-long cannot be told apart from absent-because-missing — so the
     * dimension would come back null where a source parse produced a value.
     */
    public void testAKeywordWithIgnoreAboveIsRefused() throws IOException {
        MapperService mappers = createMapperService(mapping(b -> {
            b.startObject("service.name").field("type", "keyword").field("ignore_above", 256).endObject();
        }));

        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), List.of("service.name"));
        assertEquals(Strategy.UNSUPPORTED, strategies.bySlot()[0]);
    }

    public void testAnUnmappedPathIsRefused() throws IOException {
        MapperService mappers = createMapperService(mapping(b -> b.startObject("service.name").field("type", "keyword").endObject()));

        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), List.of("service.name", "nothing.maps.this"));
        assertEquals(Strategy.KEYWORD, strategies.bySlot()[0]);
        assertEquals(Strategy.UNSUPPORTED, strategies.bySlot()[1]);
        assertFalse(strategies.complete());
    }

    /**
     * The source reader hands a multi-valued field back as a list, which every consumer downstream then treats as absent. The document
     * reader has to reach the same answer rather than picking one of the values, or the two paths would disagree.
     */
    public void testAMultiValuedFieldIsRefusedRatherThanGuessedAt() throws IOException {
        MapperService mappers = createMapperService(mapping(b -> b.startObject("service.name").field("type", "keyword").endObject()));

        List<String> paths = List.of("service.name");
        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), paths);
        ParsedDocument document = parse(mappers, """
            {"service.name":["checkout","search"]}""");

        Object[] values = new Object[paths.size()];
        assertFalse("two values under one name cannot be resolved to one", DerivedMetricsDocumentReader.read(document, strategies, values));
    }

    /**
     * Asserts the whole point: for the same document, the two readers agree value for value.
     */
    private void assertReadsSameAsSource(MapperService mappers, List<String> paths, String source) throws IOException {
        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), paths);
        assertTrue("this mapping should have been readable from the document", strategies.complete());
        ParsedDocument document = parse(mappers, source);

        Object[] fromDocument = new Object[paths.size()];
        assertTrue(DerivedMetricsDocumentReader.read(document, strategies, fromDocument));

        DerivedMetricsSourcePaths compiled = new DerivedMetricsSourcePaths();
        paths.forEach(compiled::slotFor);
        Object[] fromSource = new Object[paths.size()];
        assertTrue(DerivedMetricsSourceReader.read(document, compiled, fromSource));

        for (int slot = 0; slot < paths.size(); slot++) {
            assertThat(
                "reading [" + paths.get(slot) + "] from the parsed document must match reading it from _source",
                String.valueOf(fromDocument[slot]),
                equalTo(String.valueOf(fromSource[slot]))
            );
        }
    }

    private Object[] readFromDocument(MapperService mappers, List<String> paths, String source) throws IOException {
        Strategies strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), paths);
        assertTrue(strategies.complete());
        Object[] values = new Object[paths.size()];
        assertTrue(DerivedMetricsDocumentReader.read(parse(mappers, source), strategies, values));
        return values;
    }

    private ParsedDocument parse(MapperService mappers, String source) throws IOException {
        return mappers.documentMapper().parse(source(source));
    }
}
