/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.KeywordScriptFieldType;
import org.elasticsearch.index.mapper.LongScriptFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TestRuntimeField;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafFieldLookupProvider;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchExecutionContextTests extends ESTestCase {

    public void testFailIfFieldMappingNotFound() {
        SearchExecutionContext context = createSearchExecutionContext(IndexMetadata.INDEX_UUID_NA_VALUE, null);
        context.setAllowUnmappedFields(false);
        MappedFieldType fieldType = new TextFieldMapper.TextFieldType("text", randomBoolean());
        MappedFieldType result = context.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        QueryShardException e = expectThrows(QueryShardException.class, () -> context.failIfFieldMappingNotFound("name", null));
        assertEquals("No field mapping can be found for the field with name [name]", e.getMessage());

        context.setAllowUnmappedFields(true);
        result = context.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        result = context.failIfFieldMappingNotFound("name", null);
        assertThat(result, nullValue());

        context.setAllowUnmappedFields(false);
        context.setMapUnmappedFieldAsString(true);
        result = context.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        result = context.failIfFieldMappingNotFound("name", null);
        assertThat(result, notNullValue());
        assertThat(result, instanceOf(TextFieldMapper.TextFieldType.class));
        assertThat(result.name(), equalTo("name"));
    }

    public void testBuildAnonymousFieldType() {
        SearchExecutionContext context = createSearchExecutionContext("uuid", null);
        assertThat(context.buildAnonymousFieldType("keyword"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        assertThat(context.buildAnonymousFieldType("long"), instanceOf(NumberFieldMapper.NumberFieldType.class));
    }

    public void testToQueryFails() {
        SearchExecutionContext context = createSearchExecutionContext(IndexMetadata.INDEX_UUID_NA_VALUE, null);
        Exception exc = expectThrows(Exception.class, () -> context.toQuery(new DummyQueryBuilder() {
            @Override
            protected Query doToQuery(SearchExecutionContext context) throws IOException {
                throw new RuntimeException("boom");
            }
        }));
        assertThat(exc.getMessage(), equalTo("failed to create query: boom"));
    }

    public void testClusterAlias() throws IOException {
        final String clusterAlias = randomBoolean() ? null : "remote_cluster";
        SearchExecutionContext context = createSearchExecutionContext(IndexMetadata.INDEX_UUID_NA_VALUE, clusterAlias);

        IndexFieldMapper mapper = new IndexFieldMapper();

        IndexFieldData<?> forField = context.getForField(mapper.fieldType(), MappedFieldType.FielddataOperation.SEARCH);
        String expected = clusterAlias == null
            ? context.getIndexSettings().getIndexMetadata().getIndex().getName()
            : clusterAlias + ":" + context.getIndexSettings().getIndex().getName();
        assertEquals(expected, ((AbstractLeafOrdinalsFieldData) forField.load(null)).getOrdinalsValues().lookupOrd(0).utf8ToString());
    }

    public void testGetFullyQualifiedIndex() {
        String clusterAlias = randomAlphaOfLengthBetween(5, 10);
        String indexUuid = randomAlphaOfLengthBetween(3, 10);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(indexUuid, clusterAlias);
        assertThat(searchExecutionContext.getFullyQualifiedIndex().getName(), equalTo(clusterAlias + ":index"));
        assertThat(searchExecutionContext.getFullyQualifiedIndex().getUUID(), equalTo(indexUuid));
    }

    public void testIndexSortedOnField() {
        Settings settings = indexSettings(Version.CURRENT, 1, 1).put("index.sort.field", "sort_field").build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();

        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        SearchExecutionContext context = new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            null,
            null,
            MappingLookup.EMPTY,
            null,
            null,
            XContentParserConfiguration.EMPTY,
            new NamedWriteableRegistry(Collections.emptyList()),
            null,
            null,
            () -> 0L,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        );

        assertTrue(context.indexSortedOnField("sort_field"));
        assertFalse(context.indexSortedOnField("second_sort_field"));
        assertFalse(context.indexSortedOnField("non_sort_field"));
    }

    public void testFielddataLookupSelfReference() {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            // simulate a runtime field that depends on itself e.g. field: doc['field']
            runtimeField("field", leafLookup -> leafLookup.doc().get("field").toString())
        );
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("field", searchExecutionContext));
        assertEquals("Cyclic dependency detected while resolving runtime fields: field -> field", iae.getMessage());
    }

    public void testFielddataLookupLooseLoop() {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            // simulate a runtime field cycle: 1: doc['2'] 2: doc['3'] 3: doc['4'] 4: doc['1']
            runtimeField("1", leafLookup -> leafLookup.doc().get("2").get(0).toString()),
            runtimeField("2", leafLookup -> leafLookup.doc().get("3").get(0).toString()),
            runtimeField("3", leafLookup -> leafLookup.doc().get("4").get(0).toString()),
            runtimeField("4", leafLookup -> leafLookup.doc().get("1").get(0).toString())
        );
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("1", searchExecutionContext));
        assertEquals("Cyclic dependency detected while resolving runtime fields: 1 -> 2 -> 3 -> 4 -> 1", iae.getMessage());
    }

    public void testFielddataLookupTerminatesInLoop() {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            // simulate a runtime field cycle: 1: doc['2'] 2: doc['3'] 3: doc['4'] 4: doc['4']
            runtimeField("1", leafLookup -> leafLookup.doc().get("2").get(0).toString()),
            runtimeField("2", leafLookup -> leafLookup.doc().get("3").get(0).toString()),
            runtimeField("3", leafLookup -> leafLookup.doc().get("4").get(0).toString()),
            runtimeField("4", leafLookup -> leafLookup.doc().get("4").get(0).toString())
        );
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("1", searchExecutionContext));
        assertEquals("Cyclic dependency detected while resolving runtime fields: 1 -> 2 -> 3 -> 4 -> 4", iae.getMessage());
    }

    public void testFielddataLookupSometimesLoop() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            // simulate a runtime field cycle in the second doc: 1: doc['2'] 2: doc['3'] 3: doc['4'] 4: doc['4']
            runtimeField("1", leafLookup -> leafLookup.doc().get("2").get(0).toString()),
            runtimeField("2", leafLookup -> leafLookup.doc().get("3").get(0).toString()),
            runtimeField("3", leafLookup -> leafLookup.doc().get("4").get(0).toString()),
            runtimeField("4", (leafLookup, docId) -> {
                if (docId == 0) {
                    return "escape!";
                }
                return leafLookup.doc().get("4").get(0).toString();
            })
        );
        List<String> values = collect("1", searchExecutionContext, new TermQuery(new Term("indexed_field", "first")));
        assertEquals(List.of("escape!"), values);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("1", searchExecutionContext));
        assertEquals("Cyclic dependency detected while resolving runtime fields: 1 -> 2 -> 3 -> 4 -> 4", iae.getMessage());
    }

    public void testFielddataLookupBeyondMaxDepth() {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            runtimeField("1", leafLookup -> leafLookup.doc().get("2").get(0).toString()),
            runtimeField("2", leafLookup -> leafLookup.doc().get("3").get(0).toString()),
            runtimeField("3", leafLookup -> leafLookup.doc().get("4").get(0).toString()),
            runtimeField("4", leafLookup -> leafLookup.doc().get("5").get(0).toString()),
            runtimeField("5", leafLookup -> leafLookup.doc().get("6").get(0).toString()),
            runtimeField("6", leafLookup -> "cat")
        );
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("1", searchExecutionContext));
        assertEquals("Field requires resolving too many dependent fields: 1 -> 2 -> 3 -> 4 -> 5 -> 6", iae.getMessage());
    }

    public void testFielddataLookupReferencesBelowMaxDepth() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            runtimeField("1", leafLookup -> leafLookup.doc().get("2").get(0).toString()),
            runtimeField("2", leafLookup -> leafLookup.doc().get("3").get(0).toString()),
            runtimeField("3", leafLookup -> leafLookup.doc().get("4").get(0).toString()),
            runtimeField("4", leafLookup -> leafLookup.doc().get("5").get(0).toString()),
            runtimeField("5", (leafLookup, docId) -> "cat on doc " + docId)
        );
        assertEquals(List.of("cat on doc 0", "cat on doc 1"), collect("1", searchExecutionContext));
    }

    public void testFielddataLookupOneFieldManyReferences() throws IOException {
        int numFields = randomIntBetween(5, 20);
        List<RuntimeField> fields = new ArrayList<>(numFields + 1);
        fields.add(runtimeField("root", leafLookup -> {
            StringBuilder value = new StringBuilder();
            for (int i = 0; i < numFields; i++) {
                value.append(leafLookup.doc().get(i).get(0));
            }
            return value.toString();
        }));
        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < numFields; i++) {
            String fieldValue = Integer.toString(i);
            fields.add(runtimeField(Integer.toString(i), leafLookup -> fieldValue));
            expected.append(i);
        }
        assertEquals(
            List.of(expected.toString(), expected.toString()),
            collect("root", createSearchExecutionContext("uuid", null, createMappingLookup(List.of(), fields), Map.of()))
        );
    }

    private static MappingLookup createMappingLookup(List<MappedFieldType> concreteFields, List<RuntimeField> runtimeFields) {
        List<FieldMapper> mappers = concreteFields.stream().<FieldMapper>map(MockFieldMapper::new).toList();
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder("_doc", ObjectMapper.Defaults.SUBOBJECTS);
        Map<String, RuntimeField> runtimeFieldTypes = runtimeFields.stream().collect(Collectors.toMap(RuntimeField::name, r -> r));
        builder.addRuntimeFields(runtimeFieldTypes);
        Mapping mapping = new Mapping(builder.build(MapperBuilderContext.root(false)), new MetadataFieldMapper[0], Collections.emptyMap());
        return MappingLookup.fromMappers(mapping, mappers, Collections.emptyList(), Collections.emptyList());
    }

    public void testSearchRequestRuntimeFields() {
        /*
         * Making these immutable here test that we don't modify them.
         * Modifying them would cause all kinds of problems if two
         * shards are parsed on the same node.
         */
        Map<String, Object> runtimeMappings = Map.ofEntries(
            Map.entry("cat", Map.of("type", "keyword")),
            Map.entry("dog", Map.of("type", "long"))
        );
        SearchExecutionContext context = createSearchExecutionContext(
            "uuid",
            null,
            createMappingLookup(
                List.of(new MockFieldMapper.FakeFieldType("pig"), new MockFieldMapper.FakeFieldType("cat")),
                List.of(new TestRuntimeField("runtime", "long"))
            ),
            runtimeMappings
        );
        assertTrue(context.isFieldMapped("cat"));
        assertThat(context.getFieldType("cat"), instanceOf(KeywordScriptFieldType.class));
        assertThat(context.getMatchingFieldNames("cat"), equalTo(Set.of("cat")));
        assertTrue(context.isFieldMapped("dog"));
        assertThat(context.getFieldType("dog"), instanceOf(LongScriptFieldType.class));
        assertThat(context.getMatchingFieldNames("dog"), equalTo(Set.of("dog")));
        assertTrue(context.isFieldMapped("pig"));
        assertThat(context.getFieldType("pig"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(context.getMatchingFieldNames("pig"), equalTo(Set.of("pig")));
        assertThat(context.getMatchingFieldNames("*"), equalTo(Set.of("cat", "dog", "pig", "runtime")));
    }

    public void testSearchRequestRuntimeFieldsWrongFormat() {
        Map<String, Object> runtimeMappings = new HashMap<>();
        runtimeMappings.put("field", Arrays.asList("test1", "test2"));
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> createSearchExecutionContext(
                "uuid",
                null,
                createMappingLookup(List.of(new MockFieldMapper.FakeFieldType("pig"), new MockFieldMapper.FakeFieldType("cat")), List.of()),
                runtimeMappings
            )
        );
        assertEquals("Expected map for runtime field [field] definition but got a java.util.Arrays$ArrayList", exception.getMessage());
    }

    public void testSearchRequestRuntimeFieldsRemoval() {
        Map<String, Object> runtimeMappings = new HashMap<>();
        runtimeMappings.put("field", null);
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> createSearchExecutionContext(
                "uuid",
                null,
                createMappingLookup(List.of(new MockFieldMapper.FakeFieldType("pig"), new MockFieldMapper.FakeFieldType("cat")), List.of()),
                runtimeMappings
            )
        );
        assertEquals("Runtime field [field] was set to null but its removal is not supported in this context", exception.getMessage());
    }

    public void testSearchRequestRuntimeFieldsAndMultifieldDetection() {
        Map<String, Object> runtimeMappings = Map.ofEntries(
            Map.entry("cat", Map.of("type", "keyword")),
            Map.entry("cat.subfield", Map.of("type", "keyword")),
            Map.entry("dog", Map.of("type", "long"))
        );
        MappingLookup mappingLookup = createMappingLookup(
            List.of(
                new MockFieldMapper.FakeFieldType("pig"),
                new MockFieldMapper.FakeFieldType("pig.subfield"),
                new MockFieldMapper.FakeFieldType("cat"),
                new MockFieldMapper.FakeFieldType("cat.subfield")
            ),
            List.of(new TestRuntimeField("runtime", "long"))
        );
        SearchExecutionContext context = createSearchExecutionContext("uuid", null, mappingLookup, runtimeMappings);
        assertTrue(context.isMultiField("pig.subfield"));
        assertFalse(context.isMultiField("cat.subfield"));
        assertTrue(mappingLookup.isMultiField("cat.subfield"));
    }

    public void testSyntheticSourceScriptLoading() throws IOException {

        // Build a mapping using synthetic source
        SourceFieldMapper sourceMapper = new SourceFieldMapper.Builder(null).setSynthetic().build();
        RootObjectMapper root = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).build(MapperBuilderContext.root(true));
        Mapping mapping = new Mapping(root, new MetadataFieldMapper[] { sourceMapper }, Map.of());
        MappingLookup lookup = MappingLookup.fromMapping(mapping);

        SearchExecutionContext sec = createSearchExecutionContext("index", "", lookup, Map.of());

        // Attempting to access synthetic source via this context should throw an error
        SearchLookup searchLookup = sec.lookup();
        Exception e = expectThrows(IllegalArgumentException.class, () -> searchLookup.getSource(null, 0));
        assertThat(e.getMessage(), equalTo("Cannot access source from scripts in synthetic mode"));

        // Setting the source provider explicitly then gives us a new SearchLookup that can use source
        Source source = Source.fromMap(Map.of("field", "value"), XContentType.JSON);
        sec.setLookupProviders((ctx, doc) -> source, LeafFieldLookupProvider.fromStoredFields());
        SearchLookup searchLookup1 = sec.lookup();
        assertNotSame(searchLookup, searchLookup1);
        assertSame(source, searchLookup1.getSource(null, 0));

    }

    public static SearchExecutionContext createSearchExecutionContext(String indexUuid, String clusterAlias) {
        return createSearchExecutionContext(indexUuid, clusterAlias, MappingLookup.EMPTY, Map.of());
    }

    private static SearchExecutionContext createSearchExecutionContext(RuntimeField... fieldTypes) {
        return createSearchExecutionContext(
            "uuid",
            null,
            createMappingLookup(Collections.emptyList(), List.of(fieldTypes)),
            Collections.emptyMap()
        );
    }

    private static SearchExecutionContext createSearchExecutionContext(
        String indexUuid,
        String clusterAlias,
        MappingLookup mappingLookup,
        Map<String, Object> runtimeMappings
    ) {
        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder("index");
        indexMetadataBuilder.settings(indexSettings(Version.CURRENT, 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, indexUuid));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        MapperService mapperService = createMapperService(indexSettings, mappingLookup);
        final long nowInMillis = randomNonNegativeLong();
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            (mappedFieldType, fdc) -> mappedFieldType.fielddataBuilder(fdc).build(null, null),
            mapperService,
            mappingLookup,
            null,
            null,
            XContentParserConfiguration.EMPTY,
            new NamedWriteableRegistry(Collections.emptyList()),
            null,
            null,
            () -> nowInMillis,
            clusterAlias,
            null,
            () -> true,
            null,
            runtimeMappings
        );
    }

    private static MapperService createMapperService(IndexSettings indexSettings, MappingLookup mappingLookup) {
        IndexAnalyzers indexAnalyzers = IndexAnalyzers.of(singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, null)));
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        Supplier<SearchExecutionContext> searchExecutionContextSupplier = () -> { throw new UnsupportedOperationException(); };
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        when(mapperService.parserContext()).thenReturn(
            new MappingParserContext(
                null,
                type -> mapperRegistry.getMapperParser(type, indexSettings.getIndexVersionCreated()),
                mapperRegistry.getRuntimeFieldParsers()::get,
                indexSettings.getIndexVersionCreated(),
                () -> TransportVersion.current(),
                searchExecutionContextSupplier,
                ScriptCompiler.NONE,
                indexAnalyzers,
                indexSettings,
                indexSettings.getMode().buildIdFieldMapper(() -> true)
            )
        );
        when(mapperService.isMultiField(anyString())).then(
            (Answer<Boolean>) invocation -> mappingLookup.isMultiField(invocation.getArgument(0))
        );
        return mapperService;
    }

    private static RuntimeField runtimeField(String name, Function<LeafSearchLookup, String> runtimeDocValues) {
        return runtimeField(name, (leafLookup, docId) -> runtimeDocValues.apply(leafLookup));
    }

    private static RuntimeField runtimeField(String name, BiFunction<LeafSearchLookup, Integer, String> runtimeDocValues) {
        TestRuntimeField.TestRuntimeFieldType fieldType = new TestRuntimeField.TestRuntimeFieldType(name, null) {
            @Override
            public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
                return (cache, breakerService) -> new IndexFieldData<>() {
                    @Override
                    public String getFieldName() {
                        return name;
                    }

                    @Override
                    public ValuesSourceType getValuesSourceType() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public LeafFieldData load(LeafReaderContext context) {
                        return new LeafFieldData() {
                            @Override
                            public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                                return new DelegateDocValuesField(new ScriptDocValues<String>(new ScriptDocValues.Supplier<String>() {
                                    String value;

                                    @Override
                                    public int size() {
                                        return 1;
                                    }

                                    @Override
                                    public String getInternal(int index) {
                                        assert index == 0;
                                        return value;
                                    }

                                    @Override
                                    public void setNextDocId(int docId) {
                                        assert docId >= 0;
                                        LeafSearchLookup leafLookup = fieldDataContext.lookupSupplier().get().getLeafSearchLookup(context);
                                        leafLookup.setDocument(docId);
                                        value = runtimeDocValues.apply(leafLookup, docId);
                                    }
                                }) {
                                    @Override
                                    public int size() {
                                        return supplier.size();
                                    }

                                    @Override
                                    public String get(int i) {
                                        return supplier.getInternal(i);
                                    }
                                }, name);
                            }

                            @Override
                            public SortedBinaryDocValues getBytesValues() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public long ramBytesUsed() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public void close() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }

                    @Override
                    public LeafFieldData loadDirect(LeafReaderContext context) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SortField sortField(
                        Object missingValue,
                        MultiValueMode sortMode,
                        XFieldComparatorSource.Nested nested,
                        boolean reverse
                    ) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BucketedSort newBucketedSort(
                        BigArrays bigArrays,
                        Object missingValue,
                        MultiValueMode sortMode,
                        XFieldComparatorSource.Nested nested,
                        SortOrder sortOrder,
                        DocValueFormat format,
                        int bucketSize,
                        BucketedSort.ExtraData extra
                    ) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
        return new TestRuntimeField(name, Collections.singleton(fieldType));
    }

    private static List<String> collect(String field, SearchExecutionContext searchExecutionContext) throws IOException {
        return collect(field, searchExecutionContext, new MatchAllDocsQuery());
    }

    private static List<String> collect(String field, SearchExecutionContext searchExecutionContext, Query query) throws IOException {
        List<String> result = new ArrayList<>();
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            indexWriter.addDocument(List.of(new StringField("indexed_field", "first", Field.Store.NO)));
            indexWriter.addDocument(List.of(new StringField("indexed_field", "second", Field.Store.NO)));
            try (DirectoryReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                MappedFieldType fieldType = searchExecutionContext.getFieldType(field);
                IndexFieldData<?> indexFieldData;
                if (randomBoolean()) {
                    indexFieldData = searchExecutionContext.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                } else {
                    indexFieldData = searchExecutionContext.lookup().getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                }
                searcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) {
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) {}

                            @Override
                            public void collect(int doc) throws IOException {
                                ScriptDocValues<?> scriptDocValues;
                                if (randomBoolean()) {
                                    LeafDocLookup leafDocLookup = searchExecutionContext.lookup().getLeafSearchLookup(context).doc();
                                    leafDocLookup.setDocument(doc);
                                    scriptDocValues = leafDocLookup.get(field);
                                } else {
                                    scriptDocValues = indexFieldData.load(context).getScriptFieldFactory("test").toScriptDocValues();
                                    ;
                                }
                                scriptDocValues.getSupplier().setNextDocId(doc);
                                for (int i = 0; i < scriptDocValues.size(); i++) {
                                    result.add(scriptDocValues.get(i).toString());
                                }
                            }
                        };
                    }
                });
            }
            return result;
        }
    }
}
