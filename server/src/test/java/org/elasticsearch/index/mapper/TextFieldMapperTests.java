/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.MockSynonymAnalyzer;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.MatchQueryParser;
import org.elasticsearch.index.search.QueryStringQueryParser;
import org.elasticsearch.script.field.TextDocValuesField;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class TextFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return "value";
    }

    public final void testExistsQueryIndexDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("index", false);
            b.field("norms", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public final void testExistsQueryIndexDisabledStoreTrue() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("index", false);
            b.field("norms", false);
            b.field("store", true);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public final void testExistsQueryWithNorms() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("norms", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("fielddata", true), m -> {
            TextFieldType ft = (TextFieldType) m.fieldType();
            assertTrue(ft.fielddata());
        });
        checker.registerUpdateCheck(b -> {
            b.field("fielddata", true);
            b.startObject("fielddata_frequency_filter");
            {
                b.field("min", 10);
                b.field("max", 20);
                b.field("min_segment_size", 100);
            }
            b.endObject();
        }, m -> {
            TextFieldType ft = (TextFieldType) m.fieldType();
            assertEquals(10, ft.fielddataMinFrequency(), 0);
            assertEquals(20, ft.fielddataMaxFrequency(), 0);
            assertEquals(100, ft.fielddataMinSegmentSize());
        });
        checker.registerUpdateCheck(b -> b.field("eager_global_ordinals", "true"), m -> assertTrue(m.fieldType().eagerGlobalOrdinals()));
        checker.registerUpdateCheck(b -> {
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
        }, m -> assertEquals("keyword", m.fieldType().getTextSearchInfo().searchAnalyzer().name()));
        checker.registerUpdateCheck(b -> {
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
            b.field("search_quote_analyzer", "keyword");
        }, m -> assertEquals("keyword", m.fieldType().getTextSearchInfo().searchQuoteAnalyzer().name()));

        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("index_phrases", b -> b.field("index_phrases", true));
        checker.registerConflictCheck("index_prefixes", b -> b.startObject("index_prefixes").endObject());
        checker.registerConflictCheck("index_options", b -> b.field("index_options", "docs"));
        checker.registerConflictCheck("similarity", b -> b.field("similarity", "boolean"));
        checker.registerConflictCheck("analyzer", b -> b.field("analyzer", "keyword"));
        checker.registerConflictCheck("term_vector", b -> b.field("term_vector", "yes"));

        checker.registerConflictCheck("position_increment_gap", b -> b.field("position_increment_gap", 10));

        // norms can be set from true to false, but not vice versa
        checker.registerConflictCheck("norms", fieldMapping(b -> {
            b.field("type", "text");
            b.field("norms", false);
        }), fieldMapping(b -> {
            b.field("type", "text");
            b.field("norms", true);
        }));
        checker.registerUpdateCheck(b -> {
            b.field("type", "text");
            b.field("norms", true);
        }, b -> {
            b.field("type", "text");
            b.field("norms", false);
        }, m -> assertFalse(m.fieldType().getTextSearchInfo().hasNorms()));

    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        NamedAnalyzer dflt = new NamedAnalyzer(
            "default",
            AnalyzerScope.INDEX,
            new StandardAnalyzer(),
            TextFieldMapper.Defaults.POSITION_INCREMENT_GAP
        );
        NamedAnalyzer standard = new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer());
        NamedAnalyzer keyword = new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer());
        NamedAnalyzer whitespace = new NamedAnalyzer("whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer());
        NamedAnalyzer stop = new NamedAnalyzer(
            "my_stop_analyzer",
            AnalyzerScope.INDEX,
            new CustomAnalyzer(
                new StandardTokenizerFactory(indexSettings, null, "standard", indexSettings.getSettings()),
                new CharFilterFactory[0],
                new TokenFilterFactory[] { new TokenFilterFactory() {
                    @Override
                    public String name() {
                        return "stop";
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return new StopFilter(tokenStream, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
                    }
                } }
            )
        );
        return IndexAnalyzers.of(
            Map.of("default", dflt, "standard", standard, "keyword", keyword, "whitespace", whitespace, "my_stop_analyzer", stop),
            Map.of("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer()))
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "text");
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals("1234", fields.get(0).stringValue());
        IndexableFieldType fieldType = fields.get(0).fieldType();
        assertThat(fieldType.omitNorms(), equalTo(false));
        assertTrue(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());
    }

    public void testStoreParameterDefaults() throws IOException {
        var timeSeriesIndexMode = randomBoolean();
        var isStored = randomBoolean();
        var hasKeywordFieldForSyntheticSource = randomBoolean();

        var indexSettingsBuilder = getIndexSettingsBuilder();
        if (timeSeriesIndexMode) {
            indexSettingsBuilder.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dimension")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-08T23:40:53.384Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z");
        }
        var indexSettings = indexSettingsBuilder.build();

        var mapping = mapping(b -> {
            b.startObject("field");
            b.field("type", "text");
            if (isStored) {
                b.field("store", isStored);
            }
            if (hasKeywordFieldForSyntheticSource) {
                b.startObject("fields");
                b.startObject("keyword");
                b.field("type", "keyword");
                b.endObject();
                b.endObject();
            }
            b.endObject();

            if (timeSeriesIndexMode) {
                b.startObject("@timestamp");
                b.field("type", "date");
                b.endObject();
                b.startObject("dimension");
                b.field("type", "keyword");
                b.field("time_series_dimension", "true");
                b.endObject();
            }
        });
        DocumentMapper mapper = createMapperService(getVersion(), indexSettings, () -> true, mapping).documentMapper();

        var source = source(TimeSeriesRoutingHashFieldMapper.DUMMY_ENCODED_VALUE, b -> {
            b.field("field", "1234");
            if (timeSeriesIndexMode) {
                b.field("@timestamp", "2000-10-10T23:40:53.384Z");
                b.field("dimension", "dimension1");
            }
        }, null);
        ParsedDocument doc = mapper.parse(source);
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        IndexableFieldType fieldType = fields.get(0).fieldType();
        if (isStored || (timeSeriesIndexMode && hasKeywordFieldForSyntheticSource == false)) {
            assertTrue(fieldType.stored());
        } else {
            assertFalse(fieldType.stored());
        }
    }

    public void testBWCSerialization() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.field("fielddata", true);
            b.startObject("fields");
            {
                b.startObject("subfield").field("type", "long").endObject();
            }
            b.endObject();
        }));

        assertEquals("""
            {"_doc":{"properties":{"field":{"type":"text","fields":{"subfield":{"type":"long"}},\
            "fielddata":true}}}}""", Strings.toString(mapperService.documentMapper().mapping()));
    }

    public void testEnableStore() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertTrue(fields.get(0).fieldType().stored());
    }

    public void testDisableIndex() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text").field("index", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.size());
    }

    public void testDisableNorms() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text").field("norms", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertTrue(fields.get(0).fieldType().omitNorms());
    }

    public void testIndexOptions() throws IOException {
        Map<String, IndexOptions> supportedOptions = new HashMap<>();
        supportedOptions.put("docs", IndexOptions.DOCS);
        supportedOptions.put("freqs", IndexOptions.DOCS_AND_FREQS);
        supportedOptions.put("positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        supportedOptions.put("offsets", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties");
        for (String option : supportedOptions.keySet()) {
            mapping.startObject(option).field("type", "text").field("index_options", option).endObject();
        }
        mapping.endObject().endObject().endObject();

        DocumentMapper mapper = createDocumentMapper(mapping);
        String serialized = Strings.toString(mapper.mapping());
        assertThat(serialized, containsString("""
            "offsets":{"type":"text","index_options":"offsets"}"""));
        assertThat(serialized, containsString("""
            "freqs":{"type":"text","index_options":"freqs"}"""));
        assertThat(serialized, containsString("""
            "docs":{"type":"text","index_options":"docs"}"""));

        ParsedDocument doc = mapper.parse(source(b -> {
            for (String option : supportedOptions.keySet()) {
                b.field(option, "1234");
            }
        }));

        for (Map.Entry<String, IndexOptions> entry : supportedOptions.entrySet()) {
            String field = entry.getKey();
            IndexOptions options = entry.getValue();
            List<IndexableField> fields = doc.rootDoc().getFields(field);
            assertEquals(1, fields.size());
            assertEquals(options, fields.get(0).fieldType().indexOptions());
        }
    }

    public void testDefaultPositionIncrementGap() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("field", new String[] { "a", "b" })));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertEquals("a", fields.get(0).stringValue());
        assertEquals("b", fields.get(1).stringValue());

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            TermsEnum terms = getOnlyLeafReader(reader).terms("field").iterator();
            assertTrue(terms.seekExact(new BytesRef("b")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(TextFieldMapper.Defaults.POSITION_INCREMENT_GAP + 1, postings.nextPosition());
        });
    }

    public void testDefaultPositionIncrementGapOnSubfields() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.field("index_phrases", true);
            b.startObject("index_prefixes").endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("field", "aargle bargle", "foo bar")));
        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            TermsEnum phraseTerms = getOnlyLeafReader(reader).terms("field._index_phrase").iterator();
            assertTrue(phraseTerms.seekExact(new BytesRef("foo bar")));
            PostingsEnum phrasePostings = phraseTerms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, phrasePostings.nextDoc());
            assertEquals(TextFieldMapper.Defaults.POSITION_INCREMENT_GAP + 1, phrasePostings.nextPosition());

            TermsEnum prefixTerms = getOnlyLeafReader(reader).terms("field._index_prefix").iterator();
            assertTrue(prefixTerms.seekExact(new BytesRef("foo")));
            PostingsEnum prefixPostings = prefixTerms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, prefixPostings.nextDoc());
            assertEquals(TextFieldMapper.Defaults.POSITION_INCREMENT_GAP + 2, prefixPostings.nextPosition());
        });
    }

    public void testPositionIncrementGap() throws IOException {
        final int positionIncrementGap = randomIntBetween(1, 1000);
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "text").field("position_increment_gap", positionIncrementGap))
        );
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("field", new String[] { "a", "b" })));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertEquals("a", fields.get(0).stringValue());
        assertEquals("b", fields.get(1).stringValue());

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            TermsEnum terms = getOnlyLeafReader(reader).terms("field").iterator();
            assertTrue(terms.seekExact(new BytesRef("b")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(positionIncrementGap + 1, postings.nextPosition());
        });
    }

    public void testPositionIncrementGapOnSubfields() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.field("position_increment_gap", 10);
            b.field("index_phrases", true);
            b.startObject("index_prefixes").endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("field", "aargle bargle", "foo bar")));
        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            TermsEnum phraseTerms = getOnlyLeafReader(reader).terms("field._index_phrase").iterator();
            assertTrue(phraseTerms.seekExact(new BytesRef("foo bar")));
            PostingsEnum phrasePostings = phraseTerms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, phrasePostings.nextDoc());
            assertEquals(11, phrasePostings.nextPosition());

            TermsEnum prefixTerms = getOnlyLeafReader(reader).terms("field._index_prefix").iterator();
            assertTrue(prefixTerms.seekExact(new BytesRef("foo")));
            PostingsEnum prefixPostings = prefixTerms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, prefixPostings.nextDoc());
            assertEquals(12, prefixPostings.nextPosition());
        });
    }

    public void testSearchAnalyzerSerialization() throws IOException {
        XContentBuilder mapping = fieldMapping(
            b -> b.field("type", "text").field("analyzer", "standard").field("search_analyzer", "keyword")
        );
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());

        // special case: default index analyzer
        mapping = fieldMapping(b -> b.field("type", "text").field("analyzer", "default").field("search_analyzer", "keyword"));
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());

        // special case: default search analyzer
        mapping = fieldMapping(b -> b.field("type", "text").field("analyzer", "keyword").field("search_analyzer", "default"));
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        createDocumentMapper(fieldMapping(this::minimalMapping)).mapping()
            .toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true")));
        builder.endObject();
        String mappingString = Strings.toString(builder);
        assertTrue(mappingString.contains("analyzer"));
        assertTrue(mappingString.contains("search_analyzer"));
        assertTrue(mappingString.contains("search_quote_analyzer"));
    }

    public void testSearchQuoteAnalyzerSerialization() throws IOException {
        XContentBuilder mapping = fieldMapping(
            b -> b.field("type", "text")
                .field("analyzer", "standard")
                .field("search_analyzer", "standard")
                .field("search_quote_analyzer", "keyword")
        );
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());

        // special case: default index/search analyzer
        mapping = fieldMapping(
            b -> b.field("type", "text")
                .field("analyzer", "default")
                .field("search_analyzer", "default")
                .field("search_quote_analyzer", "keyword")
        );
        assertEquals(Strings.toString(mapping), createDocumentMapper(mapping).mappingSource().toString());
    }

    public void testTermVectors() throws IOException {
        XContentBuilder mapping = mapping(
            b -> b.startObject("field1")
                .field("type", "text")
                .field("term_vector", "no")
                .endObject()
                .startObject("field2")
                .field("type", "text")
                .field("term_vector", "yes")
                .endObject()
                .startObject("field3")
                .field("type", "text")
                .field("term_vector", "with_offsets")
                .endObject()
                .startObject("field4")
                .field("type", "text")
                .field("term_vector", "with_positions")
                .endObject()
                .startObject("field5")
                .field("type", "text")
                .field("term_vector", "with_positions_offsets")
                .endObject()
                .startObject("field6")
                .field("type", "text")
                .field("term_vector", "with_positions_offsets_payloads")
                .endObject()
        );

        DocumentMapper defaultMapper = createDocumentMapper(mapping);

        ParsedDocument doc = defaultMapper.parse(
            source(
                b -> b.field("field1", "1234")
                    .field("field2", "1234")
                    .field("field3", "1234")
                    .field("field4", "1234")
                    .field("field5", "1234")
                    .field("field6", "1234")
            )
        );

        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectors(), equalTo(false));
        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectorOffsets(), equalTo(true));
        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectorPositions(), equalTo(true));
        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectorOffsets(), equalTo(true));
        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectorPositions(), equalTo(true));
        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectorOffsets(), equalTo(true));
        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectorPositions(), equalTo(true));
        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectorPayloads(), equalTo(true));
    }

    public void testEagerGlobalOrdinals() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text").field("eager_global_ordinals", true)));

        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        assertTrue(fieldMapper.fieldType().eagerGlobalOrdinals());
    }

    public void testFielddata() throws IOException {
        MapperService disabledMapper = createMapperService(fieldMapping(this::minimalMapping));
        assertFalse(disabledMapper.fieldType("field").isAggregatable());
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> disabledMapper.fieldType("field")
                .fielddataBuilder(new FieldDataContext("index", null, null, null, MappedFieldType.FielddataOperation.SEARCH))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Fielddata is disabled on [field] in [index]. "
                    + "Text fields are not optimised for operations that require per-document field data"
            )
        );

        MapperService enabledMapper = createMapperService(fieldMapping(b -> b.field("type", "text").field("fielddata", true)));
        enabledMapper.fieldType("field").fielddataBuilder(FieldDataContext.noRuntimeFields("test")); // no exception
        assertTrue(enabledMapper.fieldType("field").isAggregatable());
        e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "text").field("index", false).field("fielddata", true)))
        );
        assertThat(e.getMessage(), containsString("Cannot enable fielddata on a [text] field that is not indexed"));
    }

    public void testFrequencyFilter() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "text")
                    .field("fielddata", true)
                    .startObject("fielddata_frequency_filter")
                    .field("min", 2d)
                    .field("min_segment_size", 1000)
                    .endObject()
            )
        );
        TextFieldType fieldType = (TextFieldType) mapperService.fieldType("field");

        assertThat(fieldType.fielddataMinFrequency(), equalTo(2d));
        assertThat(fieldType.fielddataMaxFrequency(), equalTo((double) Integer.MAX_VALUE));
        assertThat(fieldType.fielddataMinSegmentSize(), equalTo(1000));
    }

    public void testNullConfigValuesFail() throws MapperParsingException {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "text").field("analyzer", (String) null)))
        );
        assertThat(e.getMessage(), containsString("[analyzer] on mapper [field] of type [text] must not have a [null] value"));
    }

    public void testNotIndexedFieldPositionIncrement() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "text").field("index", false).field("position_increment_gap", 10)))
        );
        assertThat(e.getMessage(), containsString("Cannot set position_increment_gap on field [field] without positions enabled"));
    }

    public void testAnalyzedFieldPositionIncrementWithoutPositions() {
        for (String indexOptions : Arrays.asList("docs", "freqs")) {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createDocumentMapper(
                    fieldMapping(b -> b.field("type", "text").field("index_options", indexOptions).field("position_increment_gap", 10))
                )
            );
            assertThat(e.getMessage(), containsString("Cannot set position_increment_gap on field [field] without positions enabled"));
        }
    }

    public void testIndexPrefixIndexTypes() throws IOException {
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "text")
                        .field("analyzer", "standard")
                        .startObject("index_prefixes")
                        .endObject()
                        .field("index_options", "offsets")
                )
            );
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));
            IndexableField field = doc.rootDoc().getField("field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, field.fieldType().indexOptions());
        }

        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "text")
                        .field("analyzer", "standard")
                        .startObject("index_prefixes")
                        .endObject()
                        .field("index_options", "freqs")
                )
            );
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));
            IndexableField field = doc.rootDoc().getField("field._index_prefix");
            assertEquals(IndexOptions.DOCS, field.fieldType().indexOptions());
            assertFalse(field.fieldType().storeTermVectors());
        }

        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "text")
                        .field("analyzer", "standard")
                        .startObject("index_prefixes")
                        .endObject()
                        .field("index_options", "positions")
                )
            );
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));
            IndexableField field = doc.rootDoc().getField("field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, field.fieldType().indexOptions());
            assertFalse(field.fieldType().storeTermVectors());
        }

        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "text")
                        .field("analyzer", "standard")
                        .startObject("index_prefixes")
                        .endObject()
                        .field("term_vector", "with_positions_offsets")
                )
            );
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));
            IndexableField field = doc.rootDoc().getField("field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, field.fieldType().indexOptions());
            assertTrue(field.fieldType().storeTermVectorOffsets());
        }

        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "text")
                        .field("analyzer", "standard")
                        .startObject("index_prefixes")
                        .endObject()
                        .field("term_vector", "with_positions")
                )
            );
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "some text")));
            IndexableField field = doc.rootDoc().getField("field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, field.fieldType().indexOptions());
            assertFalse(field.fieldType().storeTermVectorOffsets());
        }
    }

    public void testNestedIndexPrefixes() throws IOException {
        {
            MapperService mapperService = createMapperService(
                mapping(
                    b -> b.startObject("object")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject("field")
                        .field("type", "text")
                        .startObject("index_prefixes")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
            );
            MappedFieldType textField = mapperService.fieldType("object.field");
            assertNotNull(textField);
            assertThat(textField, instanceOf(TextFieldType.class));
            MappedFieldType prefix = ((TextFieldType) textField).getPrefixFieldType();
            assertEquals(prefix.name(), "object.field._index_prefix");
            ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("object.field", "some text")));
            IndexableField field = doc.rootDoc().getField("object.field._index_prefix");
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, field.fieldType().indexOptions());
            assertFalse(field.fieldType().storeTermVectorOffsets());
        }

        {
            MapperService mapperService = createMapperService(
                mapping(
                    b -> b.startObject("body")
                        .field("type", "text")
                        .startObject("fields")
                        .startObject("with_prefix")
                        .field("type", "text")
                        .startObject("index_prefixes")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
            );
            MappedFieldType textField = mapperService.fieldType("body.with_prefix");
            assertNotNull(textField);
            assertThat(textField, instanceOf(TextFieldType.class));
            MappedFieldType prefix = ((TextFieldType) textField).getPrefixFieldType();
            assertEquals(prefix.name(), "body.with_prefix._index_prefix");
            ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("body", "some text")));
            IndexableField field = doc.rootDoc().getField("body.with_prefix._index_prefix");

            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, field.fieldType().indexOptions());
            assertFalse(field.fieldType().storeTermVectorOffsets());
        }
    }

    public void testFastPhraseMapping() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field").field("type", "text").field("analyzer", "my_stop_analyzer").field("index_phrases", true).endObject();
            // "standard" will be replaced with MockSynonymAnalyzer
            b.startObject("synfield").field("type", "text").field("analyzer", "standard").field("index_phrases", true).endObject();
        }));
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);

        Query q = new MatchPhraseQueryBuilder("field", "two words").toQuery(searchExecutionContext);
        assertThat(q, is(new PhraseQuery("field._index_phrase", "two words")));

        Query q2 = new MatchPhraseQueryBuilder("field", "three words here").toQuery(searchExecutionContext);
        assertThat(q2, is(new PhraseQuery("field._index_phrase", "three words", "words here")));

        Query q3 = new MatchPhraseQueryBuilder("field", "two words").slop(1).toQuery(searchExecutionContext);
        assertThat(q3, is(new PhraseQuery(1, "field", "two", "words")));

        Query q4 = new MatchPhraseQueryBuilder("field", "singleton").toQuery(searchExecutionContext);
        assertThat(q4, is(new TermQuery(new Term("field", "singleton"))));

        Query q5 = new MatchPhraseQueryBuilder("field", "sparkle a stopword").toQuery(searchExecutionContext);
        assertThat(q5, is(new PhraseQuery.Builder().add(new Term("field", "sparkle")).add(new Term("field", "stopword"), 2).build()));

        MatchQueryParser matchQueryParser = new MatchQueryParser(searchExecutionContext);
        matchQueryParser.setAnalyzer(new MockSynonymAnalyzer());
        Query q6 = matchQueryParser.parse(MatchQueryParser.Type.PHRASE, "synfield", "motor dogs");
        assertThat(
            q6,
            is(
                new MultiPhraseQuery.Builder().add(
                    new Term[] { new Term("synfield._index_phrase", "motor dogs"), new Term("synfield._index_phrase", "motor dog") }
                ).build()
            )
        );

        // https://github.com/elastic/elasticsearch/issues/43976
        CannedTokenStream cts = new CannedTokenStream(new Token("foo", 1, 0, 2, 2), new Token("bar", 0, 0, 2), new Token("baz", 1, 0, 2));
        Analyzer synonymAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(reader -> {}, cts);
            }
        };
        matchQueryParser.setAnalyzer(synonymAnalyzer);
        Query q7 = matchQueryParser.parse(MatchQueryParser.Type.BOOLEAN, "synfield", "foo");
        assertThat(
            q7,
            is(
                new BooleanQuery.Builder().add(
                    new BooleanQuery.Builder().add(new TermQuery(new Term("synfield", "foo")), BooleanClause.Occur.SHOULD)
                        .add(
                            new PhraseQuery.Builder().add(new Term("synfield._index_phrase", "bar baz")).build(),
                            BooleanClause.Occur.SHOULD
                        )
                        .build(),
                    BooleanClause.Occur.SHOULD
                ).build()
            )
        );

        ParsedDocument doc = mapperService.documentMapper()
            .parse(source(b -> b.array("field", "Some English text that is going to be very useful", "bad", "Prio 1")));

        List<IndexableField> fields = doc.rootDoc().getFields("field._index_phrase");
        assertEquals(3, fields.size());

        try (TokenStream ts = fields.get(0).tokenStream(mapperService.indexAnalyzer(fields.get(0).name(), f -> null), null)) {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            assertTrue(ts.incrementToken());
            assertEquals("Some English", termAtt.toString());
        }

        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            IndexSearcher searcher = newSearcher(ir);
            MatchPhraseQueryBuilder queryBuilder = new MatchPhraseQueryBuilder("field", "Prio 1");
            TopDocs td = searcher.search(queryBuilder.toQuery(searchExecutionContext), 1);
            assertEquals(1, td.totalHits.value);
        });

        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "text").field("index", "false").field("index_phrases", true)))
        );
        assertThat(e.getMessage(), containsString("Cannot set index_phrases on unindexed field [field]"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(b -> b.field("type", "text").field("index_options", "freqs").field("index_phrases", true))
            )
        );
        assertThat(e.getMessage(), containsString("Cannot set index_phrases on field [field] if positions are not enabled"));
    }

    public void testObjectExistsQuery() throws IOException, ParseException {
        MapperService ms = createMapperService(mapping(b -> {
            b.startObject("foo");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("bar");
                    {
                        b.field("type", "text");
                        b.field("index_phrases", true);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        SearchExecutionContext context = createSearchExecutionContext(ms);
        QueryStringQueryParser parser = new QueryStringQueryParser(context, "f");
        Query q = parser.parse("foo:*");
        assertEquals(new ConstantScoreQuery(new FieldExistsQuery("foo.bar")), q);
    }

    private static void assertAnalyzesTo(Analyzer analyzer, String field, String input, String[] output) throws IOException {
        try (TokenStream ts = analyzer.tokenStream(field, input)) {
            ts.reset();
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            for (String t : output) {
                assertTrue(ts.incrementToken());
                assertEquals(t, termAtt.toString());
            }
            assertFalse(ts.incrementToken());
            ts.end();
        }
    }

    public void testIndexPrefixMapping() throws IOException {

        {
            MapperService ms = createMapperService(
                fieldMapping(
                    b -> b.field("type", "text")
                        .field("analyzer", "standard")
                        .startObject("index_prefixes")
                        .field("min_chars", 2)
                        .field("max_chars", 6)
                        .endObject()
                )
            );

            ParsedDocument doc = ms.documentMapper()
                .parse(source(b -> b.field("field", "Some English text that is going to be very useful")));
            List<IndexableField> fields = doc.rootDoc().getFields("field._index_prefix");
            assertEquals(1, fields.size());
            withLuceneIndex(ms, iw -> iw.addDocument(doc.rootDoc()), ir -> {}); // check we can index

            assertAnalyzesTo(
                ms.indexAnalyzer("field._index_prefix", f -> null),
                "field._index_prefix",
                "tweedledum",
                new String[] { "tw", "twe", "twee", "tweed", "tweedl" }
            );
        }

        {
            MapperService ms = createMapperService(
                fieldMapping(b -> b.field("type", "text").field("analyzer", "standard").startObject("index_prefixes").endObject())
            );
            assertAnalyzesTo(
                ms.indexAnalyzer("field._index_prefix", f -> null),
                "field._index_prefix",
                "tweedledum",
                new String[] { "tw", "twe", "twee", "tweed" }
            );
        }

        {
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").nullField("index_prefixes")));
            expectThrows(
                Exception.class,
                () -> ms.indexAnalyzer("field._index_prefixes", f -> null).tokenStream("field._index_prefixes", "test")
            );
        }

        {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "text").field("analyzer", "standard");
                b.startObject("index_prefixes").field("min_chars", 1).field("max_chars", 10).endObject();
                b.startObject("fields").startObject("_index_prefix").field("type", "text").endObject().endObject();
            })));
            assertThat(e.getMessage(), containsString("Cannot use reserved field name [field._index_prefix]"));
        }

        {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "text").field("analyzer", "standard");
                b.startObject("index_prefixes").field("min_chars", 11).field("max_chars", 10).endObject();
            })));
            assertThat(e.getMessage(), containsString("min_chars [11] must be less than max_chars [10]"));
        }

        {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "text").field("analyzer", "standard");
                b.startObject("index_prefixes").field("min_chars", 0).field("max_chars", 10).endObject();
            })));
            assertThat(e.getMessage(), containsString("min_chars [0] must be greater than zero"));
        }

        {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "text").field("analyzer", "standard");
                b.startObject("index_prefixes").field("min_chars", 1).field("max_chars", 25).endObject();
            })));
            assertThat(e.getMessage(), containsString("max_chars [25] must be less than 20"));
        }

        {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "text").field("analyzer", "standard").field("index", false);
                b.startObject("index_prefixes").endObject();
            })));
            assertThat(e.getMessage(), containsString("Cannot set index_prefixes on unindexed field [field]"));
        }
    }

    public void testFastPhrasePrefixes() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "text");
                b.field("analyzer", "my_stop_analyzer");
                b.startObject("index_prefixes").field("min_chars", 2).field("max_chars", 10).endObject();
            }
            b.endObject();
            b.startObject("synfield");
            {
                b.field("type", "text");
                b.field("analyzer", "standard"); // "standard" will be replaced with MockSynonymAnalyzer
                b.field("index_phrases", true);
                b.startObject("index_prefixes").field("min_chars", 2).field("max_chars", 10).endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("synfield", "some text which we will index")));
        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {}); // check indexing

        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "two words").toQuery(searchExecutionContext);
            Query expected = new SpanNearQuery.Builder("field", true).addClause(new SpanTermQuery(new Term("field", "two")))
                .addClause(new FieldMaskingSpanQuery(new SpanTermQuery(new Term("field._index_prefix", "words")), "field"))
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "three words here").toQuery(searchExecutionContext);
            Query expected = new SpanNearQuery.Builder("field", true).addClause(new SpanTermQuery(new Term("field", "three")))
                .addClause(new SpanTermQuery(new Term("field", "words")))
                .addClause(new FieldMaskingSpanQuery(new SpanTermQuery(new Term("field._index_prefix", "here")), "field"))
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "two words").slop(1).toQuery(searchExecutionContext);
            MultiPhrasePrefixQuery mpq = new MultiPhrasePrefixQuery("field");
            mpq.setSlop(1);
            mpq.add(new Term("field", "two"));
            mpq.add(new Term("field", "words"));
            assertThat(q, equalTo(mpq));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "singleton").toQuery(searchExecutionContext);
            assertThat(
                q,
                is(new SynonymQuery.Builder("field._index_prefix").addTerm(new Term("field._index_prefix", "singleton")).build())
            );
        }

        {

            Query q = new MatchPhrasePrefixQueryBuilder("field", "sparkle a stopword").toQuery(searchExecutionContext);
            Query expected = new SpanNearQuery.Builder("field", true).addClause(new SpanTermQuery(new Term("field", "sparkle")))
                .addGap(1)
                .addClause(new FieldMaskingSpanQuery(new SpanTermQuery(new Term("field._index_prefix", "stopword")), "field"))
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            MatchQueryParser matchQueryParser = new MatchQueryParser(searchExecutionContext);
            matchQueryParser.setAnalyzer(new MockSynonymAnalyzer());
            Query q = matchQueryParser.parse(MatchQueryParser.Type.PHRASE_PREFIX, "synfield", "motor dogs");
            Query expected = new SpanNearQuery.Builder("synfield", true).addClause(new SpanTermQuery(new Term("synfield", "motor")))
                .addClause(
                    new SpanOrQuery(
                        new FieldMaskingSpanQuery(new SpanTermQuery(new Term("synfield._index_prefix", "dogs")), "synfield"),
                        new FieldMaskingSpanQuery(new SpanTermQuery(new Term("synfield._index_prefix", "dog")), "synfield")
                    )
                )
                .build();
            assertThat(q, equalTo(expected));
        }

        {
            MatchQueryParser matchQueryParser = new MatchQueryParser(searchExecutionContext);
            matchQueryParser.setPhraseSlop(1);
            matchQueryParser.setAnalyzer(new MockSynonymAnalyzer());
            Query q = matchQueryParser.parse(MatchQueryParser.Type.PHRASE_PREFIX, "synfield", "two dogs");
            MultiPhrasePrefixQuery mpq = new MultiPhrasePrefixQuery("synfield");
            mpq.setSlop(1);
            mpq.add(new Term("synfield", "two"));
            mpq.add(new Term[] { new Term("synfield", "dogs"), new Term("synfield", "dog") });
            assertThat(q, equalTo(mpq));
        }

        {
            Query q = new MatchPhrasePrefixQueryBuilder("field", "motor d").toQuery(searchExecutionContext);
            MultiPhrasePrefixQuery mpq = new MultiPhrasePrefixQuery("field");
            mpq.add(new Term("field", "motor"));
            mpq.add(new Term("field", "d"));
            assertThat(q, equalTo(mpq));
        }
    }

    public void testSimpleMerge() throws IOException {
        XContentBuilder startingMapping = fieldMapping(
            b -> b.field("type", "text").startObject("index_prefixes").endObject().field("index_phrases", true)
        );
        MapperService mapperService = createMapperService(startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(TextFieldMapper.class));

        merge(mapperService, startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(TextFieldMapper.class));

        XContentBuilder differentPrefix = fieldMapping(
            b -> b.field("type", "text").startObject("index_prefixes").field("min_chars", "3").endObject().field("index_phrases", true)
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, differentPrefix));
        assertThat(e.getMessage(), containsString("Cannot update parameter [index_prefixes]"));

        XContentBuilder differentPhrases = fieldMapping(
            b -> b.field("type", "text").startObject("index_prefixes").endObject().field("index_phrases", false)
        );
        e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, differentPhrases));
        assertThat(e.getMessage(), containsString("Cannot update parameter [index_phrases]"));

        XContentBuilder newField = mapping(b -> {
            b.startObject("field").field("type", "text").startObject("index_prefixes").endObject().field("index_phrases", true).endObject();
            b.startObject("other_field").field("type", "keyword").endObject();
        });
        merge(mapperService, newField);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(TextFieldMapper.class));
        assertThat(mapperService.documentMapper().mappers().getMapper("other_field"), instanceOf(KeywordFieldMapper.class));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("We don't have a way to assert things here", true);
        return null;
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        assumeFalse("We don't have a way to assert things here", true);
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assumeFalse("ignore_malformed not supported", ignoreMalformed);
        return TextFieldFamilySyntheticSourceTestSetup.syntheticSourceSupport("text", true);
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected void validateRoundTripReader(String syntheticSource, DirectoryReader reader, DirectoryReader roundTripReader) {
        TextFieldFamilySyntheticSourceTestSetup.validateRoundTripReader(syntheticSource, reader, roundTripReader);
    }

    public void testUnknownAnalyzerOnLegacyIndex() throws IOException {
        XContentBuilder startingMapping = fieldMapping(b -> b.field("type", "text").field("analyzer", "does_not_exist"));

        expectThrows(MapperParsingException.class, () -> createMapperService(startingMapping));

        MapperService mapperService = createMapperService(IndexVersion.fromId(5000099), startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(TextFieldMapper.class));

        merge(mapperService, startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(TextFieldMapper.class));

        // check that analyzer can be swapped out on legacy index
        XContentBuilder differentAnalyzer = fieldMapping(b -> b.field("type", "text").field("analyzer", "keyword"));
        merge(mapperService, differentAnalyzer);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(TextFieldMapper.class));
    }

    public void testIgnoreFieldDataOnLegacyIndex() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "text").field("fielddata", true));
        MapperService mapperService = createMapperService(mapping);
        assertTrue(((TextFieldMapper) mapperService.documentMapper().mappers().getMapper("field")).fieldType().fielddata());

        mapperService = createMapperService(IndexVersion.fromId(5000099), mapping);
        assertFalse(((TextFieldMapper) mapperService.documentMapper().mappers().getMapper("field")).fieldType().fielddata());

        MapperService finalMapperService = mapperService;
        expectThrows(
            IllegalArgumentException.class,
            () -> ((TextFieldMapper) finalMapperService.documentMapper().mappers().getMapper("field")).fieldType()
                .fielddataBuilder(FieldDataContext.noRuntimeFields("test"))
        );
    }

    public void testIgnoreEagerGlobalOrdinalsOnLegacyIndex() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "text").field("eager_global_ordinals", true));
        MapperService mapperService = createMapperService(mapping);
        assertTrue(((TextFieldMapper) mapperService.documentMapper().mappers().getMapper("field")).fieldType().eagerGlobalOrdinals());

        mapperService = createMapperService(IndexVersion.fromId(5000099), mapping);
        assertFalse(((TextFieldMapper) mapperService.documentMapper().mappers().getMapper("field")).fieldType().eagerGlobalOrdinals());
    }

    public void testDocValues() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "text")));
        for (String input : new String[] {
            "foo",       // Won't be tokenized
            "foo bar",   // Will be tokenized. But script doc values still returns the whole field.
        }) {
            assertScriptDocValues(mapper, input, equalTo(List.of(input)));
        }
    }

    public void testDocValuesLoadedFromStoredSynthetic() throws IOException {
        MapperService mapper = createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "text").field("store", true)));
        for (String input : new String[] {
            "foo",       // Won't be tokenized
            "foo bar",   // Will be tokenized. But script doc values still returns the whole field.
        }) {
            assertScriptDocValues(mapper, input, equalTo(List.of(input)));
        }
    }

    public void testDocValuesLoadedFromSubKeywordSynthetic() throws IOException {
        MapperService mapper = createSytheticSourceMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("fields");
            {
                b.startObject("raw").field("type", "keyword").endObject();
            }
            b.endObject();
        }));
        for (String input : new String[] {
            "foo",       // Won't be tokenized
            "foo bar",   // Will be tokenized. But script doc values still returns the whole field.
        }) {
            assertScriptDocValues(mapper, input, equalTo(List.of(input)));
        }
    }

    public void testDocValuesLoadedFromSubStoredKeywordSynthetic() throws IOException {
        MapperService mapper = createSytheticSourceMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("fields");
            {
                b.startObject("raw").field("type", "keyword").field("doc_values", false).field("store", true).endObject();
            }
            b.endObject();
        }));
        for (String input : new String[] {
            "foo",       // Won't be tokenized
            "foo bar",   // Will be tokenized. But script doc values still returns the whole field.
        }) {
            assertScriptDocValues(mapper, input, equalTo(List.of(input)));
        }
    }

    public void testEmpty() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "text")));
        var d0 = source(b -> b.field("field", new String[0]));
        var d1 = source(b -> b.field("field", ""));
        var d2 = source(b -> b.field("field", "hello"));
        var d3 = source(b -> b.nullField("field"));
        withLuceneIndex(mapperService, iw -> {
            for (SourceToParse src : List.of(d0, d1, d2, d3)) {
                iw.addDocument(mapperService.documentMapper().parse(src).rootDoc());
            }
        }, reader -> {
            IndexSearcher searcher = newSearcher(reader);
            MappedFieldType ft = mapperService.fieldType("field");
            SourceProvider sourceProvider = mapperService.mappingLookup().isSourceSynthetic() ? (ctx, doc) -> {
                throw new IllegalArgumentException("Can't load source in scripts in synthetic mode");
            } : SourceProvider.fromStoredFields();
            SearchLookup searchLookup = new SearchLookup(null, null, sourceProvider);
            IndexFieldData<?> sfd = ft.fielddataBuilder(
                new FieldDataContext("", null, () -> searchLookup, Set::of, MappedFieldType.FielddataOperation.SCRIPT)
            ).build(null, null);
            LeafFieldData lfd = sfd.load(getOnlyLeafReader(searcher.getIndexReader()).getContext());
            TextDocValuesField scriptDV = (TextDocValuesField) lfd.getScriptFieldFactory("field");
            SortedBinaryDocValues dv = scriptDV.getInput();
            assertFalse(dv.advanceExact(0));
            assertTrue(dv.advanceExact(1));
            assertTrue(dv.advanceExact(2));
            assertFalse(dv.advanceExact(3));
        });
    }
}
