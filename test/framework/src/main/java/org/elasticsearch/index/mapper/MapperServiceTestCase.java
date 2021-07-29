/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.BucketedSort.ExtraData;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test case that lets you easilly build {@link MapperService} based on some
 * mapping. Useful when you don't need to spin up an entire index but do
 * need most of the trapping of the mapping.
 */
public abstract class MapperServiceTestCase extends ESTestCase {

    protected static final Settings SETTINGS = Settings.builder().put("index.version.created", Version.CURRENT).build();

    protected static final ToXContent.Params INCLUDE_DEFAULTS = new ToXContent.MapParams(Map.of("include_defaults", "true"));

    protected Collection<? extends Plugin> getPlugins() {
        return emptyList();
    }

    protected Settings getIndexSettings() {
        return SETTINGS;
    }

    protected final Settings.Builder getIndexSettingsBuilder() {
        return Settings.builder().put(getIndexSettings());
    }

    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return createIndexAnalyzers();
    }

    protected static IndexAnalyzers createIndexAnalyzers() {
        return new IndexAnalyzers(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.of(),
            Map.of()
        );
    }

    protected final String randomIndexOptions() {
        return randomFrom("docs", "freqs", "positions", "offsets");
    }

    protected final DocumentMapper createDocumentMapper(XContentBuilder mappings) throws IOException {
        return createMapperService(mappings).documentMapper();
    }

    protected final DocumentMapper createDocumentMapper(Version version, XContentBuilder mappings) throws IOException {
        return createMapperService(version, mappings).documentMapper();
    }

    protected final DocumentMapper createDocumentMapper(String mappings) throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(mapperService, mappings);
        return mapperService.documentMapper();
    }

    protected MapperService createMapperService(XContentBuilder mappings) throws IOException {
        return createMapperService(Version.CURRENT, mappings);
    }

    protected MapperService createMapperService(Settings settings, XContentBuilder mappings) throws IOException {
        return createMapperService(Version.CURRENT, settings, () -> true, mappings);
    }

    protected MapperService createMapperService(BooleanSupplier idFieldEnabled, XContentBuilder mappings) throws IOException {
        return createMapperService(Version.CURRENT, getIndexSettings(), idFieldEnabled, mappings);
    }

    protected final MapperService createMapperService(String mappings) throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(mapperService, mappings);
        return mapperService;
    }

    protected final MapperService createMapperService(Settings settings, String mappings) throws IOException {
        MapperService mapperService = createMapperService(Version.CURRENT, settings, () -> true, mapping(b -> {}));
        merge(mapperService, mappings);
        return mapperService;
    }

    protected MapperService createMapperService(Version version, XContentBuilder mapping) throws IOException {
        return createMapperService(version, getIndexSettings(), () -> true, mapping);
    }

    /**
     * Create a {@link MapperService} like we would for an index.
     */
    protected final MapperService createMapperService(Version version,
                                                      Settings settings,
                                                      BooleanSupplier idFieldDataEnabled,
                                                      XContentBuilder mapping) throws IOException {

        MapperService mapperService = createMapperService(version, settings, idFieldDataEnabled);
        merge(mapperService, mapping);
        return mapperService;
    }

    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        throw new UnsupportedOperationException("Cannot compile script " + Strings.toString(script));
    }

    protected final MapperService createMapperService(Version version,
                                                      Settings settings,
                                                      BooleanSupplier idFieldDataEnabled) {
        IndexSettings indexSettings = createIndexSettings(version, settings);
        MapperRegistry mapperRegistry = new IndicesModule(
            getPlugins().stream().filter(p -> p instanceof MapperPlugin).map(p -> (MapperPlugin) p).collect(toList())
        ).getMapperRegistry();


        SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        return new MapperService(
            indexSettings,
            createIndexAnalyzers(indexSettings),
            xContentRegistry(),
            similarityService,
            mapperRegistry,
            () -> { throw new UnsupportedOperationException(); },
            idFieldDataEnabled,
            this::compileScript
        );
    }

    protected static IndexSettings createIndexSettings(Version version, Settings settings) {
        settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put(settings)
            .put("index.version.created", version)
            .build();
        IndexMetadata meta = IndexMetadata.builder("index")
            .settings(settings)
            .build();
        return new IndexSettings(meta, settings);
    }

    protected final void withLuceneIndex(
        MapperService mapperService,
        CheckedConsumer<RandomIndexWriter, IOException> builder,
        CheckedConsumer<IndexReader, IOException> test
    ) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(IndexShard.buildIndexAnalyzer(mapperService));
        try (
            Directory dir = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir,iwc)
        ) {
            builder.accept(iw);
            try (IndexReader reader = iw.getReader()) {
                test.accept(reader);
            }
        }
    }

    protected final SourceToParse source(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        return source("1", build, null);
    }

    protected final SourceToParse source(String id, CheckedConsumer<XContentBuilder, IOException> build, @Nullable String routing)
        throws IOException {
        return source(id, build, routing, null, Map.of());
    }

    protected final SourceToParse source(
        String id,
        CheckedConsumer<XContentBuilder, IOException> build,
        @Nullable String routing,
        @Nullable BytesReference timeSeriesId,
        Map<String, String> dynamicTemplates
    ) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        build.accept(builder);
        builder.endObject();
        return new SourceToParse("test", id, BytesReference.bytes(builder), XContentType.JSON, routing, timeSeriesId, dynamicTemplates);
    }

    protected final SourceToParse source(String source) {
        return new SourceToParse("test", "1", new BytesArray(source), XContentType.JSON);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService}.
     */
    protected final void merge(MapperService mapperService, XContentBuilder mapping) throws IOException {
        merge(mapperService, MapperService.MergeReason.MAPPING_UPDATE, mapping);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService}.
     */
    protected final void merge(MapperService mapperService, String mapping) throws IOException {
        mapperService.merge(null, new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
    }

    protected final void merge(MapperService mapperService, MapperService.MergeReason reason, String mapping) throws IOException {
        mapperService.merge(null, new CompressedXContent(mapping), reason);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService} with a specific {@code MergeReason}
     */
    protected final void merge(MapperService mapperService,
                               MapperService.MergeReason reason,
                               XContentBuilder mapping) throws IOException {
        mapperService.merge(null, new CompressedXContent(BytesReference.bytes(mapping)), reason);
    }

    protected final XContentBuilder topMapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        buildFields.accept(builder);
        return builder.endObject().endObject();
    }

    protected final XContentBuilder mapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
        buildFields.accept(builder);
        return builder.endObject().endObject().endObject();
    }

    protected final XContentBuilder dynamicMapping(Mapping dynamicMapping) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        dynamicMapping.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder.endObject();
    }

    protected final XContentBuilder fieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return mapping(b -> {
            b.startObject("field");
            buildField.accept(b);
            b.endObject();
        });
    }

    protected final XContentBuilder runtimeFieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return runtimeMapping(b -> {
            b.startObject("field");
            buildField.accept(b);
            b.endObject();
        });
    }

    protected final XContentBuilder runtimeMapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("runtime");
        buildFields.accept(builder);
        return builder.endObject().endObject().endObject();
    }

    private AggregationContext aggregationContext(
        ValuesSourceRegistry valuesSourceRegistry,
        MapperService mapperService,
        IndexSearcher searcher,
        Query query
    ) {
        return new AggregationContext() {
            private final CircuitBreaker breaker = mock(CircuitBreaker.class);
            private final MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumer(Integer.MAX_VALUE, breaker);

            @Override
            public IndexSearcher searcher() {
                return searcher;
            }

            @Override
            public Aggregator profileIfEnabled(Aggregator agg) throws IOException {
                return agg;
            }

            @Override
            public boolean profiling() {
                return false;
            }

            @Override
            public Query query() {
                return query;
            }

            @Override
            public long nowInMillis() {
                return 0;
            }

            @Override
            public boolean isFieldMapped(String field) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SearchLookup lookup() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ValuesSourceRegistry getValuesSourceRegistry() {
                return valuesSourceRegistry;
            }

            @Override
            public IndexSettings getIndexSettings() {
                throw new UnsupportedOperationException();
            }

            @Override
            public MappedFieldType getFieldType(String path) {
                return mapperService.fieldType(path);
            }

            @Override
            public Set<String> getMatchingFieldNames(String pattern) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Query buildQuery(QueryBuilder builder) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Query filterQuery(Query query) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected IndexFieldData<?> buildFieldData(MappedFieldType ft) {
                return ft.fielddataBuilder("test", null).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
            }

            @Override
            public BigArrays bigArrays() {
                return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
            }

            @Override
            public ObjectMapper getObjectMapper(String path) {
                throw new UnsupportedOperationException();
            }

            @Override
            public NestedScope nestedScope() {
                throw new UnsupportedOperationException();
            }

            @Override
            public SubSearchContext subSearchContext() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void addReleasable(Aggregator aggregator) {
                // TODO we'll have to handle this in the tests eventually
            }

            @Override
            public MultiBucketConsumer multiBucketConsumer() {
                return multiBucketConsumer;
            }

            @Override
            public BitsetFilterCache bitsetFilterCache() {
                throw new UnsupportedOperationException();
            }

            @Override
            public BucketedSort buildBucketedSort(SortBuilder<?> sort, int size, ExtraData values) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int shardRandomSeed() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getRelativeTimeInMillis() {
                return 0;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public CircuitBreaker breaker() {
                return breaker;
            }

            @Override
            public Analyzer getIndexAnalyzer(Function<String, NamedAnalyzer> unindexedFieldAnalyzer) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isCacheable() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean enableRewriteToFilterByFilter() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                throw new UnsupportedOperationException();
            }
        };
    }

    protected final void withAggregationContext(
        MapperService mapperService,
        List<SourceToParse> docs,
        CheckedConsumer<AggregationContext, IOException> test
    ) throws IOException {
        withAggregationContext(null, mapperService, docs, null, test);
    }

    protected final void withAggregationContext(
        ValuesSourceRegistry valuesSourceRegistry,
        MapperService mapperService,
        List<SourceToParse> docs,
        Query query,
        CheckedConsumer<AggregationContext, IOException> test
    ) throws IOException {
        withLuceneIndex(
            mapperService,
            writer -> {
                for (SourceToParse doc: docs) {
                    writer.addDocuments(mapperService.documentMapper().parse(doc).docs());
                }
            },
            reader -> test.accept(aggregationContext(valuesSourceRegistry, mapperService, new IndexSearcher(reader), query))
        );
    }

    protected SearchExecutionContext createSearchExecutionContext(MapperService mapperService) {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.getFieldType(anyString())).thenAnswer(inv -> mapperService.fieldType(inv.getArguments()[0].toString()));
        when(searchExecutionContext.isFieldMapped(anyString()))
            .thenAnswer(inv -> mapperService.fieldType(inv.getArguments()[0].toString()) != null);
        when(searchExecutionContext.getIndexAnalyzers()).thenReturn(mapperService.getIndexAnalyzers());
        when(searchExecutionContext.getIndexSettings()).thenReturn(mapperService.getIndexSettings());
        when(searchExecutionContext.getObjectMapper(anyString())).thenAnswer(
            inv -> mapperService.mappingLookup().objectMappers().get(inv.getArguments()[0].toString()));
        when(searchExecutionContext.getMatchingFieldNames(anyObject())).thenAnswer(
            inv -> mapperService.mappingLookup().getMatchingFieldNames(inv.getArguments()[0].toString())
        );
        when(searchExecutionContext.allowExpensiveQueries()).thenReturn(true);
        when(searchExecutionContext.lookup()).thenReturn(new SearchLookup(mapperService::fieldType, (ft, s) -> {
            throw new UnsupportedOperationException("search lookup not available");
        }));

        SimilarityService similarityService = new SimilarityService(mapperService.getIndexSettings(), null, Map.of());
        when(searchExecutionContext.getDefaultSimilarity()).thenReturn(similarityService.getDefaultSimilarity());

        return searchExecutionContext;
    }
}
