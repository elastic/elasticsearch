/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PatternTextFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new LogsDBPlugin(Settings.EMPTY));
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "value";
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        assertThat(query, instanceOf(FieldExistsQuery.class));
        FieldExistsQuery fieldExistsQuery = (FieldExistsQuery) query;
        assertThat(fieldExistsQuery.getField(), equalTo("field.template_id"));
        assertNoFieldNamesField(fields);
    }

    public void testExistsStandardSource() throws IOException {
        assertExistsQuery(createMapperService(fieldMapping(b -> b.field("type", "pattern_text"))));
    }

    public void testExistsSyntheticSource() throws IOException {
        assertExistsQuery(createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "pattern_text"))));
    }

    public void testPhraseQueryStandardSource() throws IOException {
        assertPhraseQuery(createMapperService(fieldMapping(b -> b.field("type", "pattern_text"))));
    }

    public void testPhraseQuerySyntheticSource() throws IOException {
        assertPhraseQuery(createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "pattern_text"))));
    }

    public void testPhraseQueryStandardSourceDisableTemplating() throws IOException {
        assertPhraseQuery(createMapperService(fieldMapping(b -> b.field("type", "pattern_text").field("disable_templating", true))));
    }

    public void testPhraseQuerySyntheticSourceDisableTemplating() throws IOException {
        assertPhraseQuery(
            createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "pattern_text").field("disable_templating", true)))
        );
    }

    private void assertPhraseQuery(MapperService mapperService) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            LuceneDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "the quick brown fox 1"))).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SearchExecutionContext context = createSearchExecutionContext(mapperService, newSearcher(reader));
                MatchPhraseQueryBuilder queryBuilder = new MatchPhraseQueryBuilder("field", "brown fox 1");
                TopDocs docs = context.searcher().search(queryBuilder.toQuery(context), 1);
                assertThat(docs.totalHits.value(), equalTo(1L));
                assertThat(docs.totalHits.relation(), equalTo(TotalHits.Relation.EQUAL_TO));
                assertThat(docs.scoreDocs[0].doc, equalTo(0));
            }
        }
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(
            b -> { b.field("meta", Collections.singletonMap("format", "mysql.access")); },
            m -> assertEquals(Collections.singletonMap("format", "mysql.access"), m.fieldType().meta())
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "pattern_text");
    }

    @Override
    protected void minimalStoreMapping(XContentBuilder b) throws IOException {
        // 'store' is always true
        minimalMapping(b);
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createMapperService(fieldMapping(this::minimalMapping)).documentMapper();
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        {
            List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertEquals(1, fields.size());
            assertEquals("1234", fields.get(0).stringValue());
            IndexableFieldType fieldType = fields.get(0).fieldType();
            assertThat(fieldType.omitNorms(), equalTo(true));
            assertTrue(fieldType.tokenized());
            assertFalse(fieldType.stored());
            assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
            assertThat(fieldType.storeTermVectors(), equalTo(false));
            assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
            assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
            assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
            assertEquals(DocValuesType.NONE, fieldType.docValuesType());
        }

        {
            List<IndexableField> fields = doc.rootDoc().getFields("field.template_id");
            assertEquals(1, fields.size());
            // Template is an empty string, so the templateId hash has value AAAAAAAAAAA
            assertEquals("AAAAAAAAAAA", fields.get(0).binaryValue().utf8ToString());
            IndexableFieldType fieldType = fields.get(0).fieldType();
            assertThat(fieldType.omitNorms(), equalTo(true));
            assertFalse(fieldType.tokenized());
            assertFalse(fieldType.stored());
            assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
            assertThat(fieldType.storeTermVectors(), equalTo(false));
            assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
            assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
            assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
            assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());
        }
    }

    public void testNullConfigValuesFail() throws MapperParsingException {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "pattern_text").field("meta", (String) null)))
        );
        assertThat(e.getMessage(), containsString("[meta] on mapper [field] of type [pattern_text] must not have a [null] value"));
    }

    public void testSimpleMerge() throws IOException {
        XContentBuilder startingMapping = fieldMapping(b -> b.field("type", "pattern_text"));
        MapperService mapperService = createMapperService(startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(PatternTextFieldMapper.class));

        merge(mapperService, startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(PatternTextFieldMapper.class));

        XContentBuilder newField = mapping(b -> {
            b.startObject("field").field("type", "pattern_text").startObject("meta").field("key", "value").endObject().endObject();
            b.startObject("other_field").field("type", "keyword").endObject();
        });
        merge(mapperService, newField);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(PatternTextFieldMapper.class));
        assertThat(mapperService.documentMapper().mappers().getMapper("other_field"), instanceOf(KeywordFieldMapper.class));
    }

    public void testDisableTemplatingParameter() throws IOException {
        {
            XContentBuilder mapping = fieldMapping(b -> b.field("type", "pattern_text"));
            MapperService mapperService = createMapperService(mapping);
            var mapper = (PatternTextFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            assertFalse(mapper.fieldType().disableTemplating());
        }

        {
            XContentBuilder mapping = fieldMapping(b -> b.field("type", "pattern_text").field("disable_templating", true));
            MapperService mapperService = createMapperService(mapping);
            var mapper = (PatternTextFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            assertTrue(mapper.fieldType().disableTemplating());
        }

        {
            XContentBuilder mapping = fieldMapping(b -> b.field("type", "pattern_text").field("disable_templating", false));
            MapperService mapperService = createMapperService(mapping);
            var mapper = (PatternTextFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            assertFalse(mapper.fieldType().disableTemplating());
        }
    }

    public void testDisableTemplatingParameterWhenDisallowedByLicense() throws IOException {
        Settings indexSettings = Settings.builder()
            .put(getIndexSettings())
            .put(PatternTextFieldMapper.DISABLE_TEMPLATING_SETTING.getKey(), true)
            .build();
        {
            XContentBuilder mapping = fieldMapping(b -> b.field("type", "pattern_text"));
            MapperService mapperService = createMapperService(getVersion(), indexSettings, () -> true, mapping);
            var mapper = (PatternTextFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            assertTrue(mapper.fieldType().disableTemplating());
        }

        {
            XContentBuilder mapping = fieldMapping(b -> b.field("type", "pattern_text").field("disable_templating", true));
            MapperService mapperService = createMapperService(getVersion(), indexSettings, () -> true, mapping);
            var mapper = (PatternTextFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            assertTrue(mapper.fieldType().disableTemplating());
        }

        {
            XContentBuilder mapping = fieldMapping(b -> b.field("type", "pattern_text").field("disable_templating", false));
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(getVersion(), indexSettings, () -> true, mapping)
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "value [false] for mapping parameter [disable_templating] contradicts value [true] for index "
                        + "setting [index.mapping.pattern_text.disable_templating]"
                )
            );
        }
    }

    public void testDisabledSource() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        {
            mapping.startObject("properties");
            {
                mapping.startObject("foo");
                {
                    mapping.field("type", "pattern_text");
                }
                mapping.endObject();
            }
            mapping.endObject();

            mapping.startObject("_source");
            {
                mapping.field("enabled", false);
            }
            mapping.endObject();
        }
        mapping.endObject().endObject();

        MapperService mapperService = createMapperService(mapping);
        MappedFieldType ft = mapperService.fieldType("foo");
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 4, 7));

        // Allowed even if source is disabled.
        ft.phraseQuery(ts, 0, true, context);
        ft.termQuery("a", context);
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        return PatternTextIntegrationTests.randomMessageMaybeLarge();
    }

    @Override
    protected void assertFetchMany(MapperService mapperService, String field, Object value, String format, int count) throws IOException {
        assumeFalse("pattern_text currently don't support multiple values in the same field", false);
    }

    /**
     * pattern_text does not allow sorting or aggregation and thus only allow field data operations
     * of type SCRIPT to access field data. We still want to use `testFetch` to compare value fetchers against doc
     * values. This method copies MapperTestCase.assertFetch, but uses field data operation type SCRIPT.
     */
    @Override
    protected void assertFetch(MapperService mapperService, String field, Object value, String format) throws IOException {
        MappedFieldType ft = mapperService.fieldType(field);
        SourceToParse source = source(b -> b.field(ft.name(), value));
        var fielddataContext = new FieldDataContext("", null, () -> null, Set::of, MappedFieldType.FielddataOperation.SCRIPT);
        var fdt = fielddataContext.fielddataOperation();
        ValueFetcher docValueFetcher = new DocValueFetcher(
            ft.docValueFormat(format, null),
            ft.fielddataBuilder(fielddataContext).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
        );
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));
        when(searchExecutionContext.getForField(ft, fdt)).thenAnswer(inv -> fieldDataLookup(mapperService).apply(ft, () -> {
            throw new UnsupportedOperationException();
        }, fdt));
        ValueFetcher nativeFetcher = ft.valueFetcher(searchExecutionContext, format);
        ParsedDocument doc = mapperService.documentMapper().parse(source);
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            Source s = SourceProvider.fromLookup(mapperService.mappingLookup(), null, mapperService.getMapperMetrics().sourceFieldMetrics())
                .getSource(ir.leaves().get(0), 0);
            docValueFetcher.setNextReader(ir.leaves().get(0));
            nativeFetcher.setNextReader(ir.leaves().get(0));
            List<Object> fromDocValues = docValueFetcher.fetchValues(s, 0, new ArrayList<>());
            List<Object> fromNative = nativeFetcher.fetchValues(s, 0, new ArrayList<>());
            /*
             * The native fetcher uses byte, short, etc but doc values always
             * uses long or double. This difference is fine because on the outside
             * users can't see it.
             */
            fromNative = fromNative.stream().map(o -> {
                if (o instanceof Integer || o instanceof Short || o instanceof Byte) {
                    return ((Number) o).longValue();
                }
                if (o instanceof Float) {
                    return ((Float) o).doubleValue();
                }
                return o;
            }).collect(toList());

            if (dedupAfterFetch()) {
                fromNative = fromNative.stream().distinct().collect(Collectors.toList());
            }
            /*
             * Doc values sort according to something appropriate to the field
             * and the native fetchers usually don't sort. We're ok with this
             * difference. But we have to convince the test we're ok with it.
             */
            assertThat("fetching " + value, fromNative, containsInAnyOrder(fromDocValues.toArray()));
        });
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assertFalse("pattern_text doesn't support ignoreMalformed", ignoreMalformed);
        return new PatternTextSyntheticSourceSupport();
    }

    static class PatternTextSyntheticSourceSupport implements SyntheticSourceSupport {
        @Override
        public SyntheticSourceExample example(int maxValues) {
            Tuple<String, String> v = generateValue();
            return new SyntheticSourceExample(v.v1(), v.v2(), this::mapping);
        }

        private Tuple<String, String> generateValue() {
            var value = PatternTextIntegrationTests.randomMessage();
            return Tuple.tuple(value, value);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "pattern_text");
            if (randomBoolean()) {
                b.field("disable_templating", true);
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of();
        }
    }

    public void testDocValues() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "pattern_text")));
        assertScriptDocValues(mapper, "foo", equalTo(List.of("foo")));
    }

    public void testDocValuesSynthetic() throws IOException {
        MapperService mapper = createSytheticSourceMapperService(fieldMapping(b -> b.field("type", "pattern_text")));
        assertScriptDocValues(mapper, "foo", equalTo(List.of("foo")));
    }

    public void testAnalyzerAttributeDefault() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "pattern_text")));
        var fieldMapper = (PatternTextFieldMapper) mapper.mappingLookup().getMapper("field");
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        fieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        var result = (Map<?, ?>) XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2().get("field");
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("type"), equalTo("pattern_text"));
    }

    public void testAnalyzerAttributeStandard() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "pattern_text").field("analyzer", "standard")));
        var fieldMapper = (PatternTextFieldMapper) mapper.mappingLookup().getMapper("field");
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        fieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        var result = (Map<?, ?>) XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2().get("field");
        assertThat(result.size(), equalTo(2));
        assertThat(result.get("type"), equalTo("pattern_text"));
        assertThat(result.get("analyzer"), equalTo("standard"));
    }

    public void testAnalyzerAttributeLog() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "pattern_text").field("analyzer", "delimiter")));
        var fieldMapper = (PatternTextFieldMapper) mapper.mappingLookup().getMapper("field");
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        fieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        var result = (Map<?, ?>) XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2().get("field");
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("type"), equalTo("pattern_text"));
        assertThat(fieldMapper.getAnalyzer(), equalTo(DelimiterAnalyzer.INSTANCE));
    }

    public void testAnalyzerAttributeIllegal() throws IOException {
        IllegalArgumentException e = (IllegalArgumentException) expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "pattern_text").field("analyzer", "whitespace")))
        ).getCause();
        assertThat(e.getMessage(), equalTo("unsupported analyzer [whitespace] for field [field], supported analyzers are [standard, log]"));
    }

    @Override
    public void testSyntheticSourceKeepArrays() {
        // This mapper does not allow arrays
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of();
    }
}
