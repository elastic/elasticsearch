/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.analysis.Token;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test for {@link TokenCountFieldMapper}.
 */
public class TokenCountFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new MapperExtrasPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "token_count").field("analyzer", "keyword");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "some words";
    }

    @Override
    protected Object getSampleValueForQuery() {
        return 1;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 1));
        checker.registerConflictCheck("enable_position_increments", b -> b.field("enable_position_increments", false));
        checker.registerUpdateCheck(this::minimalMapping, b -> b.field("type", "token_count").field("analyzer", "standard"), m -> {
            TokenCountFieldMapper tcfm = (TokenCountFieldMapper) m;
            assertThat(tcfm.analyzer(), equalTo("standard"));
        });
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        NamedAnalyzer dflt = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());
        NamedAnalyzer standard = new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer());
        NamedAnalyzer keyword = new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer());
        return IndexAnalyzers.of(Map.of("default", dflt, "standard", standard, "keyword", keyword));
    }

    /**
     *  When position increments are counted, we're looking to make sure that we:
        - don't count tokens without an increment
        - count normal tokens with one increment
        - count funny tokens with more than one increment
        - count the final token increments on the rare token streams that have them
     */
    public void testCountPositionsWithIncrements() throws IOException {
        Analyzer analyzer = createMockAnalyzer();
        assertThat(TokenCountFieldMapper.countPositions(analyzer, "", "", true), equalTo(7));
    }

    /**
     *  When position increments are not counted (only positions are counted), we're looking to make sure that we:
        - don't count tokens without an increment
        - count normal tokens with one increment
        - count funny tokens with more than one increment as only one
        - don't count the final token increments on the rare token streams that have them
     */
    public void testCountPositionsWithoutIncrements() throws IOException {
        Analyzer analyzer = createMockAnalyzer();
        assertThat(TokenCountFieldMapper.countPositions(analyzer, "", "", false), equalTo(2));
    }

    private Analyzer createMockAnalyzer() {
        Token t1 = new Token();      // Token without an increment
        t1.setPositionIncrement(0);
        Token t2 = new Token();
        t2.setPositionIncrement(1);  // Normal token with one increment
        Token t3 = new Token();
        t2.setPositionIncrement(2);  // Funny token with more than one increment
        int finalTokenIncrement = 4; // Final token increment
        Token[] tokens = new Token[] { t1, t2, t3 };
        Collections.shuffle(Arrays.asList(tokens), random());
        final TokenStream tokenStream = new CannedTokenStream(finalTokenIncrement, 0, tokens);
        // TODO: we have no CannedAnalyzer?
        return new Analyzer() {
            @Override
            public TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new MockTokenizer(), tokenStream);
            }
        };
    }

    public void testParseNullValue() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField();
        LuceneDocument doc = parseDocument(mapper, createDocument(null));
        assertNull(doc.getField("test.tc"));
    }

    public void testParseEmptyValue() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField();
        LuceneDocument doc = parseDocument(mapper, createDocument(""));
        assertEquals(0, doc.getField("test.tc").numericValue());
    }

    public void testParseNotNullValue() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField();
        LuceneDocument doc = parseDocument(mapper, createDocument("three tokens string"));
        assertEquals(3, doc.getField("test.tc").numericValue());
    }

    private DocumentMapper createIndexWithTokenCountField() throws IOException {
        return createDocumentMapper(mapping(b -> {
            b.startObject("test");
            {
                b.field("type", "text");
                b.startObject("fields");
                {
                    b.startObject("tc");
                    {
                        b.field("type", "token_count");
                        b.field("analyzer", "standard");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
    }

    private static SourceToParse createDocument(String fieldValue) throws Exception {
        return source(b -> b.field("test", fieldValue));
    }

    private LuceneDocument parseDocument(DocumentMapper mapper, SourceToParse request) {
        return mapper.parse(request).docs().stream().findFirst().orElseThrow(() -> new IllegalStateException("Test object not parsed"));
    }

    @Override
    protected String generateRandomInputValue(MappedFieldType ft) {
        int words = between(1, 1000);
        StringBuilder b = new StringBuilder(words * 5);
        b.append(randomAlphaOfLength(4));
        for (int w = 1; w < words; w++) {
            b.append(' ').append(randomAlphaOfLength(4));
        }
        return b.toString();
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        b.field("type", "token_count").field("analyzer", "standard");
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assertFalse(ignoreMalformed);

        var nullValue = usually() ? null : randomNonNegativeInt();
        return new SyntheticSourceSupport() {
            @Override
            public boolean preservesExactSource() {
                return true;
            }

            public SyntheticSourceExample example(int maxValues) {
                if (randomBoolean()) {
                    var value = generateValue();
                    return new SyntheticSourceExample(value.text, value.text, this::mapping);
                }

                var values = randomList(1, 5, this::generateValue);
                var textArray = values.stream().map(Value::text).toList();

                return new SyntheticSourceExample(textArray, textArray, this::mapping);
            }

            private record Value(String text, Integer tokenCount) {}

            private Value generateValue() {
                if (rarely()) {
                    return new Value(null, nullValue);
                }

                var text = randomList(0, 10, () -> randomAlphaOfLengthBetween(0, 10)).stream().collect(Collectors.joining(" "));
                // with keyword analyzer token count is always 1
                return new Value(text, 1);
            }

            private void mapping(XContentBuilder b) throws IOException {
                b.field("type", "token_count").field("analyzer", "keyword");
                if (rarely()) {
                    b.field("index", false);
                }
                if (rarely()) {
                    b.field("store", true);
                }
                if (nullValue != null) {
                    b.field("null_value", nullValue);
                }
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                return List.of();
            }
        };
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    public void testAggregationsDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertAggregatableConsistency(mapperService.fieldType("field"));
    }
}
