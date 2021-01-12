/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.query;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.RuntimeFieldType;
import org.elasticsearch.index.mapper.TestRuntimeField;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryShardContextTests extends ESTestCase {

    public void testFailIfFieldMappingNotFound() {
        QueryShardContext context = createQueryShardContext(IndexMetadata.INDEX_UUID_NA_VALUE, null);
        context.setAllowUnmappedFields(false);
        MappedFieldType fieldType = new TextFieldMapper.TextFieldType("text");
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
        QueryShardContext context = createQueryShardContext("uuid", null);
        assertThat(context.buildAnonymousFieldType("keyword"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        assertThat(context.buildAnonymousFieldType("long"), instanceOf(NumberFieldMapper.NumberFieldType.class));
    }

    public void testToQueryFails() {
        QueryShardContext context = createQueryShardContext(IndexMetadata.INDEX_UUID_NA_VALUE, null);
        Exception exc = expectThrows(Exception.class,
            () -> context.toQuery(new AbstractQueryBuilder() {
                @Override
                public String getWriteableName() {
                    return null;
                }

                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {

                }

                @Override
                protected void doXContent(XContentBuilder builder, Params params) throws IOException {

                }

                @Override
                protected Query doToQuery(QueryShardContext context) throws IOException {
                    throw new RuntimeException("boom");
                }

                @Override
                protected boolean doEquals(AbstractQueryBuilder other) {
                    return false;
                }

                @Override
                protected int doHashCode() {
                    return 0;
                }
            }));
        assertThat(exc.getMessage(), equalTo("failed to create query: boom"));
    }

    public void testClusterAlias() throws IOException {
        final String clusterAlias = randomBoolean() ? null : "remote_cluster";
        QueryShardContext context = createQueryShardContext(IndexMetadata.INDEX_UUID_NA_VALUE, clusterAlias);

        IndexFieldMapper mapper = new IndexFieldMapper();

        IndexFieldData<?> forField = context.getForField(mapper.fieldType());
        String expected = clusterAlias == null ? context.getIndexSettings().getIndexMetadata().getIndex().getName()
            : clusterAlias + ":" + context.getIndexSettings().getIndex().getName();
        assertEquals(expected, ((AbstractLeafOrdinalsFieldData)forField.load(null)).getOrdinalsValues().lookupOrd(0).utf8ToString());
    }

    public void testGetFullyQualifiedIndex() {
        String clusterAlias = randomAlphaOfLengthBetween(5, 10);
        String indexUuid = randomAlphaOfLengthBetween(3, 10);
        QueryShardContext shardContext = createQueryShardContext(indexUuid, clusterAlias);
        assertThat(shardContext.getFullyQualifiedIndex().getName(), equalTo(clusterAlias + ":index"));
        assertThat(shardContext.getFullyQualifiedIndex().getUUID(), equalTo(indexUuid));
    }

    public void testIndexSortedOnField() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put("index.sort.field", "sort_field")
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index")
            .settings(settings)
            .build();

        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        QueryShardContext context = new QueryShardContext(
            0,
            0,
            indexSettings,
            null,
            null,
            null,
            null,
            null,
            null,
            NamedXContentRegistry.EMPTY,
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
        QueryShardContext queryShardContext = createQueryShardContext(
            // simulate a runtime field that depends on itself e.g. field: doc['field']
            runtimeField("field", leafLookup -> leafLookup.doc().get("field").toString())
        );
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("field", queryShardContext));
        assertEquals("Cyclic dependency detected while resolving runtime fields: field -> field", iae.getMessage());
    }

    public void testFielddataLookupLooseLoop() {
        QueryShardContext queryShardContext = createQueryShardContext(
            // simulate a runtime field cycle: 1: doc['2'] 2: doc['3'] 3: doc['4'] 4: doc['1']
            runtimeField("1", leafLookup -> leafLookup.doc().get("2").get(0).toString()),
            runtimeField("2", leafLookup -> leafLookup.doc().get("3").get(0).toString()),
            runtimeField("3", leafLookup -> leafLookup.doc().get("4").get(0).toString()),
            runtimeField("4", leafLookup -> leafLookup.doc().get("1").get(0).toString())
        );
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("1", queryShardContext));
        assertEquals("Cyclic dependency detected while resolving runtime fields: 1 -> 2 -> 3 -> 4 -> 1", iae.getMessage());
    }

    public void testFielddataLookupTerminatesInLoop() {
        QueryShardContext queryShardContext = createQueryShardContext(
            // simulate a runtime field cycle: 1: doc['2'] 2: doc['3'] 3: doc['4'] 4: doc['4']
            runtimeField("1", leafLookup -> leafLookup.doc().get("2").get(0).toString()),
            runtimeField("2", leafLookup -> leafLookup.doc().get("3").get(0).toString()),
            runtimeField("3", leafLookup -> leafLookup.doc().get("4").get(0).toString()),
            runtimeField("4", leafLookup -> leafLookup.doc().get("4").get(0).toString())
        );
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("1", queryShardContext));
        assertEquals("Cyclic dependency detected while resolving runtime fields: 1 -> 2 -> 3 -> 4 -> 4", iae.getMessage());
    }

    public void testFielddataLookupSometimesLoop() throws IOException {
        QueryShardContext queryShardContext = createQueryShardContext(
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
        List<String> values = collect("1", queryShardContext, new TermQuery(new Term("indexed_field", "first")));
        assertEquals(List.of("escape!"), values);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("1", queryShardContext));
        assertEquals("Cyclic dependency detected while resolving runtime fields: 1 -> 2 -> 3 -> 4 -> 4", iae.getMessage());
    }

    public void testFielddataLookupBeyondMaxDepth() {
        QueryShardContext queryShardContext = createQueryShardContext(
            runtimeField("1", leafLookup -> leafLookup.doc().get("2").get(0).toString()),
            runtimeField("2", leafLookup -> leafLookup.doc().get("3").get(0).toString()),
            runtimeField("3", leafLookup -> leafLookup.doc().get("4").get(0).toString()),
            runtimeField("4", leafLookup -> leafLookup.doc().get("5").get(0).toString()),
            runtimeField("5", leafLookup -> leafLookup.doc().get("6").get(0).toString()),
            runtimeField("6", leafLookup -> "cat")
        );
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> collect("1", queryShardContext));
        assertEquals("Field requires resolving too many dependent fields: 1 -> 2 -> 3 -> 4 -> 5 -> 6", iae.getMessage());
    }

    public void testFielddataLookupReferencesBelowMaxDepth() throws IOException {
        QueryShardContext queryShardContext = createQueryShardContext(
            runtimeField("1", leafLookup -> leafLookup.doc().get("2").get(0).toString()),
            runtimeField("2", leafLookup -> leafLookup.doc().get("3").get(0).toString()),
            runtimeField("3", leafLookup -> leafLookup.doc().get("4").get(0).toString()),
            runtimeField("4", leafLookup -> leafLookup.doc().get("5").get(0).toString()),
            runtimeField("5", (leafLookup, docId) -> "cat on doc " + docId)
        );
        assertEquals(List.of("cat on doc 0", "cat on doc 1"), collect("1", queryShardContext));
    }

    public void testFielddataLookupOneFieldManyReferences() throws IOException {
        int numFields = randomIntBetween(5, 20);
        List<RuntimeFieldType> fields = new ArrayList<>(numFields + 1);
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
            collect("root", createQueryShardContext("uuid", null, createMappingLookup(List.of(), fields), Map.of(), List.of()))
        );
    }

    private static MappingLookup createMappingLookup(List<MappedFieldType> concreteFields, List<RuntimeFieldType> runtimeFields) {
        List<FieldMapper> mappers = concreteFields.stream().map(MockFieldMapper::new).collect(Collectors.toList());
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder("_doc", Version.CURRENT);
        runtimeFields.forEach(builder::addRuntime);
        Mapping mapping = new Mapping(builder.build(new ContentPath()), new MetadataFieldMapper[0], Collections.emptyMap());
        return new MappingLookup(mapping, mappers, Collections.emptyList(), Collections.emptyList(), null, null, null);
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
        QueryShardContext qsc = createQueryShardContext(
            "uuid",
            null,
            createMappingLookup(List.of(new MockFieldMapper.FakeFieldType("pig"), new MockFieldMapper.FakeFieldType("cat")), List.of()),
            runtimeMappings,
            Collections.singletonList(new TestRuntimeField.Plugin()));
        assertTrue(qsc.isFieldMapped("cat"));
        assertThat(qsc.getFieldType("cat"), instanceOf(TestRuntimeField.class));
        assertThat(qsc.simpleMatchToIndexNames("cat"), equalTo(Set.of("cat")));
        assertTrue(qsc.isFieldMapped("dog"));
        assertThat(qsc.getFieldType("dog"), instanceOf(TestRuntimeField.class));
        assertThat(qsc.simpleMatchToIndexNames("dog"), equalTo(Set.of("dog")));
        assertTrue(qsc.isFieldMapped("pig"));
        assertThat(qsc.getFieldType("pig"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(qsc.simpleMatchToIndexNames("pig"), equalTo(Set.of("pig")));
        assertThat(qsc.simpleMatchToIndexNames("*"), equalTo(Set.of("cat", "dog", "pig")));
    }

    public static QueryShardContext createQueryShardContext(String indexUuid, String clusterAlias) {
        return createQueryShardContext(indexUuid, clusterAlias, MappingLookup.EMPTY, Map.of(), List.of());
    }

    private static QueryShardContext createQueryShardContext(RuntimeFieldType... fieldTypes) {
        return createQueryShardContext(
            "uuid",
            null,
            createMappingLookup(Collections.emptyList(), List.of(fieldTypes)),
            Collections.emptyMap(),
            Collections.emptyList()
        );
    }

    private static QueryShardContext createQueryShardContext(
        String indexUuid,
        String clusterAlias,
        MappingLookup mappingLookup,
        Map<String, Object> runtimeMappings,
        List<MapperPlugin> mapperPlugins
    ) {
        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder("index");
        indexMetadataBuilder.settings(Settings.builder().put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
        );
        IndexMetadata indexMetadata = indexMetadataBuilder.build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        MapperService mapperService = createMapperService(indexSettings, mapperPlugins);
        final long nowInMillis = randomNonNegativeLong();
        return new QueryShardContext(
            0,
            0,
            indexSettings,
            null,
            (mappedFieldType, idxName, searchLookup) -> mappedFieldType.fielddataBuilder(idxName, searchLookup).build(null, null),
            mapperService,
            mappingLookup,
            null,
            null,
            NamedXContentRegistry.EMPTY,
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

    private static MapperService createMapperService(
        IndexSettings indexSettings,
        List<MapperPlugin> mapperPlugins
    ) {
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(
            singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, null)),
            emptyMap(),
            emptyMap()
        );
        IndicesModule indicesModule = new IndicesModule(mapperPlugins);
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        Supplier<QueryShardContext> queryShardContextSupplier = () -> { throw new UnsupportedOperationException(); };
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        when(mapperService.parserContext()).thenReturn(new Mapper.TypeParser.ParserContext(
            null,
            mapperRegistry.getMapperParsers()::get,
            mapperRegistry.getRuntimeFieldTypeParsers()::get,
            indexSettings.getIndexVersionCreated(),
            queryShardContextSupplier,
            null,
            null,
            indexAnalyzers,
            indexSettings,
            () -> true,
            false
        ));
        return mapperService;
    }

    private static RuntimeFieldType runtimeField(String name, Function<LeafSearchLookup, String> runtimeDocValues) {
        return runtimeField(name, (leafLookup, docId) -> runtimeDocValues.apply(leafLookup));
    }

    private static RuntimeFieldType runtimeField(String name, BiFunction<LeafSearchLookup, Integer, String> runtimeDocValues) {
        return new TestRuntimeField(name, null) {
            @Override
            public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName,
                                                           Supplier<SearchLookup> searchLookup) {
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
                            public ScriptDocValues<?> getScriptValues() {
                                return new ScriptDocValues<>() {
                                    String value;

                                    @Override
                                    public int size() {
                                        return 1;
                                    }

                                    @Override
                                    public String get(int index) {
                                        assert index == 0;
                                        return value;
                                    }

                                    @Override
                                    public void setNextDocId(int docId) {
                                        assert docId >= 0;
                                        LeafSearchLookup leafLookup = searchLookup.get()
                                            .getLeafSearchLookup(context);
                                        leafLookup.setDocument(docId);
                                        value = runtimeDocValues.apply(leafLookup, docId);
                                    }
                                };
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
                    public SortField sortField(Object missingValue,
                                               MultiValueMode sortMode,
                                               XFieldComparatorSource.Nested nested,
                                               boolean reverse) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BucketedSort newBucketedSort(BigArrays bigArrays,
                                                        Object missingValue,
                                                        MultiValueMode sortMode,
                                                        XFieldComparatorSource.Nested nested,
                                                        SortOrder sortOrder,
                                                        DocValueFormat format,
                                                        int bucketSize,
                                                        BucketedSort.ExtraData extra) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    private static List<String> collect(String field, QueryShardContext queryShardContext) throws IOException {
        return collect(field, queryShardContext, new MatchAllDocsQuery());
    }

    private static List<String> collect(String field, QueryShardContext queryShardContext, Query query) throws IOException {
        List<String> result = new ArrayList<>();
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            indexWriter.addDocument(List.of(new StringField("indexed_field", "first", Field.Store.NO)));
            indexWriter.addDocument(List.of(new StringField("indexed_field", "second", Field.Store.NO)));
            try (DirectoryReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                MappedFieldType fieldType = queryShardContext.getFieldType(field);
                IndexFieldData<?> indexFieldData;
                if (randomBoolean()) {
                    indexFieldData = queryShardContext.getForField(fieldType);
                } else {
                    indexFieldData = queryShardContext.lookup().doc().getForField(fieldType);
                }
                searcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) {
                        ScriptDocValues<?> scriptValues = indexFieldData.load(context).getScriptValues();
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) {}

                            @Override
                            public void collect(int doc) throws IOException {
                                ScriptDocValues<?> scriptDocValues;
                                if(randomBoolean()) {
                                    LeafDocLookup leafDocLookup = queryShardContext.lookup().doc().getLeafDocLookup(context);
                                    leafDocLookup.setDocument(doc);
                                    scriptDocValues = leafDocLookup.get(field);
                                } else {
                                    scriptDocValues = scriptValues;
                                }
                                scriptDocValues.setNextDocId(doc);
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
