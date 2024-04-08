/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.tests.analysis.MockLowerCaseFilter;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class KeywordFieldMapperTests extends MapperTestCase {
    /**
     * Creates a copy of the lowercase token filter which we use for testing merge errors.
     */
    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {
        @Override
        public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
            return singletonList(PreConfiguredTokenFilter.singleton("mock_other_lowercase", true, MockLowerCaseFilter::new));
        }

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
            return singletonMap(
                "keyword",
                (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(
                    name,
                    () -> new MockTokenizer(MockTokenizer.KEYWORD, false)
                )
            );
        }
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "value";
    }

    public final void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
            if (randomBoolean()) {
                b.field("norms", false);
            }
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public final void testExistsQueryDocValuesDisabledWithNorms() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
            b.field("norms", true);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testAggregationsDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertAggregatableConsistency(mapperService.fieldType("field"));
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new MockAnalysisPlugin());
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.ofEntries(
                Map.entry("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())),
                Map.entry("other_lowercase", new NamedAnalyzer("other_lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())),
                Map.entry("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new LowercaseNormalizer()))
            ),
            Map.of(
                "lowercase",
                new NamedAnalyzer(
                    "lowercase",
                    AnalyzerScope.INDEX,
                    new CustomAnalyzer(
                        TokenizerFactory.newFactory("lowercase", WhitespaceTokenizer::new),
                        new CharFilterFactory[0],
                        new TokenFilterFactory[] { new TokenFilterFactory() {

                            @Override
                            public String name() {
                                return "lowercase";
                            }

                            @Override
                            public TokenStream create(TokenStream tokenStream) {
                                return new LowerCaseFilter(tokenStream);
                            }
                        } }
                    )
                )
            )
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "keyword");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("index_options", b -> b.field("index_options", "freqs"));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "foo"));
        checker.registerConflictCheck("similarity", b -> b.field("similarity", "boolean"));
        checker.registerConflictCheck("normalizer", b -> b.field("normalizer", "lowercase"));

        checker.registerUpdateCheck(b -> b.field("eager_global_ordinals", true), m -> assertTrue(m.fieldType().eagerGlobalOrdinals()));
        checker.registerUpdateCheck(
            b -> b.field("ignore_above", 256),
            m -> assertEquals(256, ((KeywordFieldMapper) m).fieldType().ignoreAbove())
        );
        checker.registerUpdateCheck(
            b -> b.field("split_queries_on_whitespace", true),
            m -> assertEquals("_whitespace", m.fieldType().getTextSearchInfo().searchAnalyzer().name())
        );

        // norms can be set from true to false, but not vice versa
        checker.registerConflictCheck("norms", b -> b.field("norms", true));
        checker.registerUpdateCheck(b -> {
            minimalMapping(b);
            b.field("norms", true);
        }, b -> {
            minimalMapping(b);
            b.field("norms", false);
        }, m -> assertFalse(m.fieldType().getTextSearchInfo().hasNorms()));

        registerDimensionChecks(checker);
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());

        assertEquals(new BytesRef("1234"), fields.get(0).binaryValue());
        IndexableFieldType fieldType = fields.get(0).fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());
        assertSame(KeywordFieldMapper.Defaults.FIELD_TYPE, fieldType);

        // used by TermVectorsService
        assertThat(TermVectorsService.getValues(doc.rootDoc().getFields("field")), contains("1234"));
    }

    public void testIgnoreAbove() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("ignore_above", 5)));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "elk")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        fields = doc.rootDoc().getFields("_ignored");
        assertEquals(0, fields.size());

        doc = mapper.parse(source(b -> b.field("field", "elasticsearch")));
        fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.size());

        fields = doc.rootDoc().getFields("_ignored");
        assertEquals(1, fields.size());
        assertEquals("field", fields.get(0).stringValue());
    }

    public void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getFields("field"), empty());

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("null_value", "uri")));
        doc = mapper.parse(source(b -> {}));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.size());
        doc = mapper.parse(source(b -> b.nullField("field")));
        fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals(new BytesRef("uri"), fields.get(0).binaryValue());
    }

    public void testEnableStore() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertTrue(fields.get(0).fieldType().stored());
    }

    public void testDisableIndex() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("index", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals(IndexOptions.NONE, fields.get(0).fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, fields.get(0).fieldType().docValuesType());
    }

    public void testDisableDocValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("doc_values", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals(DocValuesType.NONE, fields.get(0).fieldType().docValuesType());
    }

    public void testIndexOptions() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("index_options", "freqs")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals(IndexOptions.DOCS_AND_FREQS, fields.get(0).fieldType().indexOptions());

        for (String indexOptions : Arrays.asList("positions", "offsets")) {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> b.field("type", "keyword").field("index_options", indexOptions)))
            );
            assertThat(
                e.getMessage(),
                containsString("Unknown value [" + indexOptions + "] for field [index_options] - accepted values are [docs, freqs]")
            );
        }
    }

    public void testEnableNorms() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "keyword").field("doc_values", false).field("norms", true))
        );
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertFalse(fields.get(0).fieldType().omitNorms());

        List<IndexableField> fieldNamesFields = doc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertThat(fieldNamesFields, empty());
    }

    public void testDimension() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        KeywordFieldMapper.KeywordFieldType ft = (KeywordFieldMapper.KeywordFieldType) mapperService.fieldType("field");
        assertFalse(ft.isDimension());

        assertDimension(true, KeywordFieldMapper.KeywordFieldType::isDimension);
        assertDimension(false, KeywordFieldMapper.KeywordFieldType::isDimension);
    }

    public void testDimensionAndIgnoreAbove() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true).field("ignore_above", 2048);
        })));
        assertThat(
            e.getCause().getMessage(),
            containsString("Field [ignore_above] cannot be set in conjunction with field [time_series_dimension]")
        );
    }

    public void testDimensionAndNormalizer() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true).field("normalizer", "my_normalizer");
        })));
        assertThat(
            e.getCause().getMessage(),
            containsString("Field [normalizer] cannot be set in conjunction with field [time_series_dimension]")
        );
    }

    public void testDimensionIndexedAndDocvalues() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", false).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", true).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", false).field("doc_values", true);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
    }

    public void testDimensionMultiValuedField() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        });
        DocumentMapper mapper = randomBoolean() ? createDocumentMapper(mapping) : createTimeSeriesModeDocumentMapper(mapping);

        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.array("field", "1234", "45678"))));
        assertThat(e.getCause().getMessage(), containsString("Dimension field [field] cannot be a multi-valued field"));
    }

    public void testDimensionExtraLongKeyword() throws IOException {
        DocumentMapper mapper = createTimeSeriesModeDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }));

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(
                source(
                    b -> b.field("field", randomAlphaOfLengthBetween(IndexWriter.MAX_TERM_LENGTH + 1, IndexWriter.MAX_TERM_LENGTH + 100))
                )
            )
        );
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "Document contains at least one immense term in field=\"field\" (whose UTF8 encoding is longer than the max length 32766"
            )
        );
    }

    public void testConfigureSimilarity() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("similarity", "boolean")));
        MappedFieldType ft = mapperService.documentMapper().mappers().fieldTypesLookup().get("field");
        assertEquals("boolean", ft.getTextSearchInfo().similarity().name());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "keyword").field("similarity", "BM25")))
        );
        assertThat(e.getMessage(), containsString("Cannot update parameter [similarity] from [boolean] to [BM25]"));
    }

    public void testNormalizer() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword").field("normalizer", "lowercase")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "AbC")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());

        assertEquals(new BytesRef("abc"), fields.get(0).binaryValue());
        IndexableFieldType fieldType = fields.get(0).fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());
    }

    public void testNormalizerNamedDefault() throws IOException {
        // you can call a normalizer 'default' but it won't be applied unless you specifically ask for it
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field").field("type", "keyword").endObject();
            b.startObject("field2").field("type", "keyword").field("normalizer", "default").endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("field", "FOO");
            b.field("field2", "FOO");
        }));
        assertEquals(new BytesRef("FOO"), doc.rootDoc().getField("field").binaryValue());
        assertEquals(new BytesRef("foo"), doc.rootDoc().getField("field2").binaryValue());
    }

    public void testParsesKeywordNestedEmptyObjectStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        DocumentParsingException ex = expectThrows(
            DocumentParsingException.class,
            () -> defaultMapper.parse(source(b -> b.startObject("field").endObject()))
        );
        assertEquals(
            "[1:11] failed to parse field [field] of type [keyword] in document with id '1'. " + "Preview of field's value: '{}'",
            ex.getMessage()
        );
    }

    public void testParsesKeywordNestedListStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> defaultMapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject();
                {
                    b.startArray("array_name").value("inner_field_first").value("inner_field_second").endArray();
                }
                b.endObject();
            }
            b.endArray();
        })));
        assertEquals(
            "[1:67] failed to parse field [field] of type [keyword] in document with id '1'. "
                + "Preview of field's value: '{array_name=[inner_field_first, inner_field_second]}'",
            ex.getMessage()
        );
    }

    public void testParsesKeywordNullStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> defaultMapper.parse(source(b -> b.startObject("field").nullField("field_name").endObject()))
        );
        assertEquals(
            "[1:28] failed to parse field [field] of type [keyword] in document with id '1'. "
                + "Preview of field's value: '{field_name=null}'",
            e.getMessage()
        );
    }

    public void testUpdateNormalizer() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("normalizer", "lowercase")));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "keyword").field("normalizer", "other_lowercase")))
        );
        assertEquals(
            "Mapper for [field] conflicts with existing mapper:\n"
                + "\tCannot update parameter [normalizer] from [lowercase] to [other_lowercase]",
            e.getMessage()
        );
    }

    public void testSplitQueriesOnWhitespace() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field").field("type", "keyword").endObject();
            b.startObject("field_with_normalizer");
            {
                b.field("type", "keyword");
                b.field("normalizer", "lowercase");
                b.field("split_queries_on_whitespace", true);
            }
            b.endObject();
        }));

        MappedFieldType fieldType = mapperService.fieldType("field");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        KeywordFieldMapper.KeywordFieldType ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        Analyzer a = ft.getTextSearchInfo().searchAnalyzer();
        assertTokenStreamContents(a.tokenStream("", "Hello World"), new String[] { "Hello World" });

        fieldType = mapperService.fieldType("field_with_normalizer");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertThat(ft.getTextSearchInfo().searchAnalyzer().name(), equalTo("lowercase"));
        assertTokenStreamContents(
            ft.getTextSearchInfo().searchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] { "hello", "world" }
        );
        Analyzer q = ft.getTextSearchInfo().searchQuoteAnalyzer();
        assertTokenStreamContents(q.tokenStream("", "Hello World"), new String[] { "hello world" });

        mapperService = createMapperService(mapping(b -> {
            b.startObject("field").field("type", "keyword").field("split_queries_on_whitespace", true).endObject();
            b.startObject("field_with_normalizer");
            {
                b.field("type", "keyword");
                b.field("normalizer", "lowercase");
                b.field("split_queries_on_whitespace", false);
            }
            b.endObject();
        }));

        fieldType = mapperService.fieldType("field");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertTokenStreamContents(
            ft.getTextSearchInfo().searchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] { "Hello", "World" }
        );

        fieldType = mapperService.fieldType("field_with_normalizer");
        assertThat(fieldType, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        ft = (KeywordFieldMapper.KeywordFieldType) fieldType;
        assertThat(ft.getTextSearchInfo().searchAnalyzer().name(), equalTo("lowercase"));
        assertTokenStreamContents(
            ft.getTextSearchInfo().searchAnalyzer().analyzer().tokenStream("", "Hello World"),
            new String[] { "hello world" }
        );
    }

    public void testScriptAndPrecludedParameters() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "keyword");
            b.field("script", "test");
            b.field("null_value", true);
        })));
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: Field [null_value] cannot be set in conjunction with field [script]"));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        return switch (between(0, 4)) {
            case 0 -> randomAlphaOfLengthBetween(1, 100);
            case 1 -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 100);
            case 2 -> randomLong();
            case 3 -> randomDouble();
            case 4 -> randomBoolean();
            default -> throw new IllegalStateException();
        };
    }

    @Override
    protected boolean dedupAfterFetch() {
        return true;
    }

    public void testDimensionInRoutingPath() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("time_series_dimension", true)));
        IndexSettings settings = createIndexSettings(
            IndexVersion.current(),
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build()
        );
        mapper.documentMapper().validate(settings, false);  // Doesn't throw
    }

    public void testKeywordFieldUtf8LongerThan32766() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "keyword")));
        StringBuilder stringBuilder = new StringBuilder(32768);
        for (int i = 0; i < 32768; i++) {
            stringBuilder.append("a");
        }
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", stringBuilder.toString())))
        );
        assertThat(e.getCause().getMessage(), containsString("UTF8 encoding is longer than the max length"));
    }

    /**
     * Test that we don't error on exceeding field size if field is neither indexed nor has doc values nor stored
     */
    public void testKeywordFieldUtf8LongerThan32766SourceOnly() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "keyword");
            b.field("index", false);
            b.field("doc_values", false);
            b.field("store", false);
        }));
        StringBuilder stringBuilder = new StringBuilder(32768);
        for (int i = 0; i < 32768; i++) {
            stringBuilder.append("a");
        }
        mapper.parse(source(b -> b.field("field", stringBuilder.toString())));
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected Function<Object, Object> loadBlockExpected() {
        return v -> ((BytesRef) v).utf8ToString();
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assertFalse("keyword doesn't support ignore_malformed", ignoreMalformed);
        return new KeywordSyntheticSourceSupport(
            randomBoolean() ? null : between(10, 100),
            randomBoolean(),
            usually() ? null : randomAlphaOfLength(2),
            true
        );
    }

    static class KeywordSyntheticSourceSupport implements SyntheticSourceSupport {
        private final Integer ignoreAbove;
        private final boolean allIgnored;
        private final boolean store;
        private final boolean docValues;
        private final String nullValue;
        private final boolean exampleSortsUsingIgnoreAbove;

        KeywordSyntheticSourceSupport(Integer ignoreAbove, boolean store, String nullValue, boolean exampleSortsUsingIgnoreAbove) {
            this.ignoreAbove = ignoreAbove;
            this.allIgnored = ignoreAbove != null && rarely();
            this.store = store;
            this.nullValue = nullValue;
            this.exampleSortsUsingIgnoreAbove = exampleSortsUsingIgnoreAbove;
            this.docValues = store ? randomBoolean() : true;
        }

        @Override
        public SyntheticSourceExample example(int maxValues) {
            return example(maxValues, false);
        }

        public SyntheticSourceExample example(int maxValues, boolean loadBlockFromSource) {
            if (randomBoolean()) {
                Tuple<String, String> v = generateValue();
                Object loadBlock = v.v2();
                if (loadBlockFromSource == false && ignoreAbove != null && v.v2().length() > ignoreAbove) {
                    loadBlock = null;
                }
                return new SyntheticSourceExample(v.v1(), v.v2(), loadBlock, this::mapping);
            }
            List<Tuple<String, String>> values = randomList(1, maxValues, this::generateValue);
            List<String> in = values.stream().map(Tuple::v1).toList();
            List<String> outPrimary = new ArrayList<>();
            List<String> outExtraValues = new ArrayList<>();
            values.stream().map(Tuple::v2).forEach(v -> {
                if (exampleSortsUsingIgnoreAbove && ignoreAbove != null && v.length() > ignoreAbove) {
                    outExtraValues.add(v);
                } else {
                    outPrimary.add(v);
                }
            });
            List<String> outList = store ? outPrimary : new HashSet<>(outPrimary).stream().sorted().collect(Collectors.toList());
            List<String> loadBlock;
            if (loadBlockFromSource) {
                // The block loader infrastructure will never return nulls. Just zap them all.
                loadBlock = in.stream().filter(m -> m != null).toList();
            } else if (docValues) {
                loadBlock = new HashSet<>(outPrimary).stream().sorted().collect(Collectors.toList());
            } else {
                loadBlock = List.copyOf(outList);
            }
            Object loadBlockResult = loadBlock.size() == 1 ? loadBlock.get(0) : loadBlock;
            outList.addAll(outExtraValues);
            Object out = outList.size() == 1 ? outList.get(0) : outList;
            return new SyntheticSourceExample(in, out, loadBlockResult, this::mapping);
        }

        private Tuple<String, String> generateValue() {
            if (nullValue != null && randomBoolean()) {
                return Tuple.tuple(null, nullValue);
            }
            int length = 5;
            if (ignoreAbove != null && (allIgnored || randomBoolean())) {
                length = ignoreAbove + 5;
            }
            String v = randomAlphaOfLength(length);
            return Tuple.tuple(v, v);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "keyword");
            if (nullValue != null) {
                b.field("null_value", nullValue);
            }
            if (ignoreAbove != null) {
                b.field("ignore_above", ignoreAbove);
            }
            if (store) {
                b.field("store", true);
            }
            if (docValues == false) {
                b.field("doc_values", false);
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of(
                new SyntheticSourceInvalidExample(
                    equalTo(
                        "field [field] of type [keyword] doesn't support synthetic source because "
                            + "it doesn't have doc values and isn't stored"
                    ),
                    b -> b.field("type", "keyword").field("doc_values", false)
                ),
                new SyntheticSourceInvalidExample(
                    equalTo("field [field] of type [keyword] doesn't support synthetic source because it declares a normalizer"),
                    b -> b.field("type", "keyword").field("normalizer", "lowercase")
                )
            );
        }
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        return new IngestScriptSupport() {
            @Override
            protected StringFieldScript.Factory emptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new StringFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {}
                };
            }

            @Override
            protected StringFieldScript.Factory nonEmptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new StringFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {
                        emit("foo");
                    }
                };
            }
        };
    }

    public void testLegacyField() throws Exception {
        // check that unknown normalizers are treated leniently on old indices
        MapperService service = createMapperService(IndexVersion.fromId(5000099), Settings.EMPTY, () -> false, mapping(b -> {
            b.startObject("mykeyw");
            b.field("type", "keyword");
            b.field("normalizer", "unknown-normalizer");
            b.endObject();
        }));
        assertThat(service.fieldType("mykeyw"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        assertEquals(Lucene.KEYWORD_ANALYZER, ((KeywordFieldMapper.KeywordFieldType) service.fieldType("mykeyw")).normalizer());

        // check that normalizer can be updated
        merge(service, mapping(b -> {
            b.startObject("mykeyw");
            b.field("type", "keyword");
            b.field("normalizer", "lowercase");
            b.endObject();
        }));
        assertThat(service.fieldType("mykeyw"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        assertNotEquals(Lucene.KEYWORD_ANALYZER, ((KeywordFieldMapper.KeywordFieldType) service.fieldType("mykeyw")).normalizer());
    }

    public void testDocValues() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        assertScriptDocValues(mapper, "foo", equalTo(List.of("foo")));
    }

    public void testDocValuesLoadedFromSource() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("doc_values", false)));
        assertScriptDocValues(mapper, "foo", equalTo(List.of("foo")));
    }

    public void testDocValuesLoadedFromStoredSynthetic() throws IOException {
        MapperService mapper = createMapperService(
            syntheticSourceFieldMapping(b -> b.field("type", "keyword").field("doc_values", false).field("store", true))
        );
        assertScriptDocValues(mapper, "foo", equalTo(List.of("foo")));
    }
}
