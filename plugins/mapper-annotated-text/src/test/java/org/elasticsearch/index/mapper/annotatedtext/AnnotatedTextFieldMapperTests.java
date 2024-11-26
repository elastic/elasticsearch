/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.annotatedtext;

import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.TextFieldFamilySyntheticSourceTestSetup;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AnnotatedTextFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new AnnotatedTextPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "annotated_text");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "some text";
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {

        checker.registerUpdateCheck(b -> {
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
        }, m -> assertEquals("keyword", m.fieldType().getTextSearchInfo().searchAnalyzer().name()));
        checker.registerUpdateCheck(b -> {
            b.field("analyzer", "default");
            b.field("search_analyzer", "keyword");
            b.field("search_quote_analyzer", "keyword");
        }, m -> assertEquals("keyword", m.fieldType().getTextSearchInfo().searchQuoteAnalyzer().name()));

        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("index_options", b -> b.field("index_options", "docs"));
        checker.registerConflictCheck("similarity", b -> b.field("similarity", "boolean"));
        checker.registerConflictCheck("analyzer", b -> b.field("analyzer", "keyword"));
        checker.registerConflictCheck("term_vector", b -> b.field("term_vector", "yes"));

        checker.registerConflictCheck("position_increment_gap", b -> b.field("position_increment_gap", 10));

        // norms can be set from true to false, but not vice versa
        checker.registerConflictCheck("norms", fieldMapping(b -> {
            b.field("type", "annotated_text");
            b.field("norms", false);
        }), fieldMapping(b -> {
            b.field("type", "annotated_text");
            b.field("norms", true);
        }));
        checker.registerUpdateCheck(b -> {
            b.field("type", "annotated_text");
            b.field("norms", true);
        }, b -> {
            b.field("type", "annotated_text");
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

    public void testAnnotationInjection() throws IOException {

        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));

        // Use example of typed and untyped annotations
        String annotatedText = "He paid [Stormy Daniels](Stephanie+Clifford&Payee) hush money";
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", annotatedText)));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());

        assertEquals(annotatedText, fields.get(0).stringValue());

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {

            LeafReader leaf = reader.leaves().get(0).reader();
            TermsEnum terms = leaf.terms("field").iterator();

            assertTrue(terms.seekExact(new BytesRef("stormy")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(2, postings.nextPosition());

            assertTrue(terms.seekExact(new BytesRef("Stephanie Clifford")));
            postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(2, postings.nextPosition());

            assertTrue(terms.seekExact(new BytesRef("Payee")));
            postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(2, postings.nextPosition());

            assertTrue(terms.seekExact(new BytesRef("hush")));
            postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(4, postings.nextPosition());
        });
    }

    public void testToleranceForBadAnnotationMarkup() throws IOException {

        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));

        String annotatedText = "foo [bar](MissingEndBracket baz";
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", annotatedText)));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());

        assertEquals(annotatedText, fields.get(0).stringValue());

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            LeafReader leaf = reader.leaves().get(0).reader();
            TermsEnum terms = leaf.terms("field").iterator();

            assertTrue(terms.seekExact(new BytesRef("foo")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(0, postings.nextPosition());

            assertTrue(terms.seekExact(new BytesRef("bar")));
            postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(1, postings.nextPosition());

            assertFalse(terms.seekExact(new BytesRef("MissingEndBracket")));
            // Bad markup means value is treated as plain text and fed through tokenisation
            assertTrue(terms.seekExact(new BytesRef("missingendbracket")));

        });
    }

    public void testIndexedTermVectors() throws IOException {

        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "annotated_text");
            b.field("term_vector", "with_positions_offsets_payloads");
        }));

        String text = "the quick [brown](Color) fox jumped over the lazy dog";
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", text)));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            LeafReader leaf = reader.leaves().get(0).reader();
            Terms terms = leaf.termVectors().get(0, "field");
            TermsEnum iterator = terms.iterator();
            BytesRef term;
            Set<String> foundTerms = new HashSet<>();
            while ((term = iterator.next()) != null) {
                foundTerms.add(term.utf8ToString());
            }
            // Check we have both text and annotation tokens
            assertTrue(foundTerms.contains("brown"));
            assertTrue(foundTerms.contains("Color"));
            assertTrue(foundTerms.contains("fox"));
        });
    }

    public void testDefaults() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

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

    public void testEnableStore() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "annotated_text");
            b.field("store", true);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertTrue(fields.get(0).fieldType().stored());
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
            b.field("type", "annotated_text");
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

    public void testDisableNorms() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "annotated_text");
            b.field("norms", false);
        }));

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

        for (String option : supportedOptions.keySet()) {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "annotated_text");
                b.field("index_options", option);
            }));
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
            List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertEquals(1, fields.size());
            assertEquals(supportedOptions.get(option), fields.get(0).fieldType().indexOptions());
        }
    }

    public void testDefaultPositionIncrementGap() throws IOException {

        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("field", "a", "b")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.size());

        assertEquals("a", fields.get(0).stringValue());
        assertEquals("b", fields.get(1).stringValue());

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            LeafReader leaf = reader.leaves().get(0).reader();
            TermsEnum terms = leaf.terms("field").iterator();
            assertTrue(terms.seekExact(new BytesRef("b")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(TextFieldMapper.Defaults.POSITION_INCREMENT_GAP + 1, postings.nextPosition());
        });
    }

    public void testPositionIncrementGap() throws IOException {
        final int positionIncrementGap = randomIntBetween(1, 1000);

        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "annotated_text");
            b.field("position_increment_gap", positionIncrementGap);
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("field", "a", "b")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertEquals("a", fields.get(0).stringValue());
        assertEquals("b", fields.get(1).stringValue());

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            LeafReader leaf = reader.leaves().get(0).reader();
            TermsEnum terms = leaf.terms("field").iterator();
            assertTrue(terms.seekExact(new BytesRef("b")));
            PostingsEnum postings = terms.postings(null, PostingsEnum.POSITIONS);
            assertEquals(0, postings.nextDoc());
            assertEquals(positionIncrementGap + 1, postings.nextPosition());
        });
    }

    public void testSearchAnalyzerSerialization() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("field")
                .field("type", "annotated_text")
                .field("analyzer", "standard")
                .field("search_analyzer", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(mapping, mapper.mappingSource().toString());

        // special case: default index analyzer
        mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("field")
                .field("type", "annotated_text")
                .field("analyzer", "default")
                .field("search_analyzer", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        mapper = createDocumentMapper(mapping);
        assertEquals(mapping, mapper.mappingSource().toString());

        mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("field")
                .field("type", "annotated_text")
                .field("analyzer", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        mapper = createDocumentMapper(mapping);
        assertEquals(mapping, mapper.mappingSource().toString());

        // special case: default search analyzer
        mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("field")
                .field("type", "annotated_text")
                .field("analyzer", "keyword")
                .field("search_analyzer", "default")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        mapper = createDocumentMapper(mapping);
        assertEquals(mapping, mapper.mappingSource().toString());

        mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("field")
                .field("type", "annotated_text")
                .field("analyzer", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        mapper = createDocumentMapper(mapping);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        mapper.mapping().toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true")));
        builder.endObject();

        String mappingString = Strings.toString(builder);
        assertTrue(mappingString.contains("analyzer"));
        assertTrue(mappingString.contains("search_analyzer"));
        assertTrue(mappingString.contains("search_quote_analyzer"));
    }

    public void testSearchQuoteAnalyzerSerialization() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("field")
                .field("type", "annotated_text")
                .field("analyzer", "standard")
                .field("search_analyzer", "standard")
                .field("search_quote_analyzer", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(mapping, mapper.mappingSource().toString());

        // special case: default index/search analyzer
        mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("field")
                .field("type", "annotated_text")
                .field("analyzer", "default")
                .field("search_analyzer", "default")
                .field("search_quote_analyzer", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        mapper = createDocumentMapper(mapping);
        assertEquals(mapping, mapper.mappingSource().toString());
    }

    public void testTermVectors() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {
            b.startObject("field1").field("type", "annotated_text").field("term_vector", "no").endObject();
            b.startObject("field2").field("type", "annotated_text").field("term_vector", "yes").endObject();
            b.startObject("field3").field("type", "annotated_text").field("term_vector", "with_offsets").endObject();
            b.startObject("field4").field("type", "annotated_text").field("term_vector", "with_positions").endObject();
            b.startObject("field5").field("type", "annotated_text").field("term_vector", "with_positions_offsets").endObject();
            b.startObject("field6").field("type", "annotated_text").field("term_vector", "with_positions_offsets_payloads").endObject();
        }));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("field1", "1234");
            b.field("field2", "1234");
            b.field("field3", "1234");
            b.field("field4", "1234");
            b.field("field5", "1234");
            b.field("field6", "1234");
        }));

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

    public void testNullConfigValuesFail() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "annotated_text");
            b.nullField("analyzer");
        })));
        assertThat(e.getMessage(), containsString("must not have a [null] value"));
    }

    public void testNotIndexedField() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "annotated_text");
            b.field("index", false);
        })));
        assertEquals("Failed to parse mapping: unknown parameter [index] on mapper [field] of type [annotated_text]", e.getMessage());
    }

    public void testAnalyzedFieldPositionIncrementWithoutPositions() {
        for (String indexOptions : Arrays.asList("docs", "freqs")) {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "annotated_text");
                b.field("index_options", indexOptions);
                b.field("position_increment_gap", 0);
            })));
            assertThat(e.getMessage(), containsString("Cannot set position_increment_gap on field [field] without positions enabled"));
        }
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("annotated_text doesn't have fielddata so we can't check against anything here.", true);
        return null;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assumeFalse("ignore_malformed not supported", ignoreMalformed);
        return TextFieldFamilySyntheticSourceTestSetup.syntheticSourceSupport("annotated_text", false);
    }

    @Override
    protected BlockReaderSupport getSupportedReaders(MapperService mapper, String loaderFieldName) {
        return TextFieldFamilySyntheticSourceTestSetup.getSupportedReaders(mapper, loaderFieldName);
    }

    @Override
    protected Function<Object, Object> loadBlockExpected(BlockReaderSupport blockReaderSupport, boolean columnReader) {
        return TextFieldFamilySyntheticSourceTestSetup.loadBlockExpected(blockReaderSupport, columnReader);
    }

    @Override
    protected void validateRoundTripReader(String syntheticSource, DirectoryReader reader, DirectoryReader roundTripReader) {
        TextFieldFamilySyntheticSourceTestSetup.validateRoundTripReader(syntheticSource, reader, roundTripReader);
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
