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
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NameOrDefinition;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentSizeObserver;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.BucketedSort.ExtraData;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.test.FieldMaskingReader;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

/**
 * Test case that lets you easilly build {@link MapperService} based on some
 * mapping. Useful when you don't need to spin up an entire index but do
 * need most of the trapping of the mapping.
 */
public abstract class MapperServiceTestCase extends FieldTypeTestCase {

    protected static final Settings SETTINGS = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
        .build();

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
        return IndexAnalyzers.of(Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())));
    }

    protected static String randomIndexOptions() {
        return randomFrom("docs", "freqs", "positions", "offsets");
    }

    protected final DocumentMapper createDocumentMapper(XContentBuilder mappings) throws IOException {
        return createMapperService(mappings).documentMapper();
    }

    protected final DocumentMapper createTimeSeriesModeDocumentMapper(XContentBuilder mappings) throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "uid")
            .build();
        return createMapperService(settings, mappings).documentMapper();
    }

    protected final DocumentMapper createDocumentMapper(IndexVersion version, XContentBuilder mappings) throws IOException {
        return createMapperService(version, mappings).documentMapper();
    }

    protected final DocumentMapper createDocumentMapper(String mappings) throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(mapperService, mappings);
        return mapperService.documentMapper();
    }

    public final MapperService createMapperService(XContentBuilder mappings) throws IOException {
        return createMapperService(getVersion(), mappings);
    }

    protected IndexVersion getVersion() {
        return IndexVersion.current();
    }

    protected final MapperService createMapperService(Settings settings, XContentBuilder mappings) throws IOException {
        return createMapperService(getVersion(), settings, () -> true, mappings);
    }

    protected final MapperService createMapperService(BooleanSupplier idFieldEnabled, XContentBuilder mappings) throws IOException {
        return createMapperService(getVersion(), getIndexSettings(), idFieldEnabled, mappings);
    }

    protected final MapperService createMapperService(String mappings) throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(mapperService, mappings);
        return mapperService;
    }

    protected final MapperService createMapperService(Settings settings, String mappings) throws IOException {
        MapperService mapperService = createMapperService(IndexVersion.current(), settings, () -> true, mapping(b -> {}));
        merge(mapperService, mappings);
        return mapperService;
    }

    protected final MapperService createMapperService(IndexVersion version, XContentBuilder mapping) throws IOException {
        return createMapperService(version, getIndexSettings(), () -> true, mapping);
    }

    /**
     * Create a {@link MapperService} like we would for an index.
     */
    protected final MapperService createMapperService(
        IndexVersion version,
        Settings settings,
        BooleanSupplier idFieldDataEnabled,
        XContentBuilder mapping
    ) throws IOException {

        MapperService mapperService = createMapperService(version, settings, idFieldDataEnabled);
        merge(mapperService, mapping);
        return mapperService;
    }

    protected final MapperService createMapperService(IndexVersion version, Settings settings, BooleanSupplier idFieldDataEnabled) {
        IndexSettings indexSettings = createIndexSettings(version, settings);
        MapperRegistry mapperRegistry = new IndicesModule(
            getPlugins().stream().filter(p -> p instanceof MapperPlugin).map(p -> (MapperPlugin) p).collect(toList())
        ).getMapperRegistry();

        SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        return new MapperService(
            () -> TransportVersion.current(),
            indexSettings,
            createIndexAnalyzers(indexSettings),
            parserConfig(),
            similarityService,
            mapperRegistry,
            () -> {
                throw new UnsupportedOperationException();
            },
            indexSettings.getMode().buildIdFieldMapper(idFieldDataEnabled),
            this::compileScript
        );
    }

    /**
     *  This is the injection point for tests that require mock scripts.  Test cases should override this to return the
     *  mock script factory of their choice.
     */
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        throw new UnsupportedOperationException("Cannot compile script " + Strings.toString(script));
    }

    protected static IndexSettings createIndexSettings(IndexVersion version, Settings settings) {
        settings = indexSettings(1, 0).put(settings).put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        IndexMetadata meta = IndexMetadata.builder("index").settings(settings).build();
        return new IndexSettings(meta, settings);
    }

    protected static void withLuceneIndex(
        MapperService mapperService,
        CheckedConsumer<RandomIndexWriter, IOException> builder,
        CheckedConsumer<DirectoryReader, IOException> test
    ) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(IndexShard.buildIndexAnalyzer(mapperService)).setCodec(
            new PerFieldMapperCodec(Lucene99Codec.Mode.BEST_SPEED, mapperService, BigArrays.NON_RECYCLING_INSTANCE)
        );
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc)) {
            builder.accept(iw);
            try (DirectoryReader reader = iw.getReader()) {
                test.accept(reader);
            }
        }
    }

    /**
     * Build a {@link SourceToParse} with the id {@code "1"} and without any dynamic templates.
     */
    public static SourceToParse source(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        return source("1", build, null);
    }

    /**
     * Build a {@link SourceToParse} without any dynamic templates.
     */
    protected static SourceToParse source(
        @Nullable String id,
        CheckedConsumer<XContentBuilder, IOException> build,
        @Nullable String routing
    ) throws IOException {
        return source(id, build, routing, Map.of());
    }

    /**
     * Build a {@link SourceToParse}.
     */
    protected static SourceToParse source(
        @Nullable String id,
        CheckedConsumer<XContentBuilder, IOException> build,
        @Nullable String routing,
        Map<String, String> dynamicTemplates
    ) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        build.accept(builder);
        builder.endObject();
        return new SourceToParse(
            id,
            BytesReference.bytes(builder),
            XContentType.JSON,
            routing,
            dynamicTemplates,
            DocumentSizeObserver.EMPTY_INSTANCE
        );
    }

    /**
     * Build a {@link SourceToParse} with an id of {@code "1"}.
     */
    protected static SourceToParse source(String source) {
        return new SourceToParse("1", new BytesArray(source), XContentType.JSON);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService}.
     */
    protected static void merge(MapperService mapperService, XContentBuilder mapping) throws IOException {
        merge(mapperService, MapperService.MergeReason.MAPPING_UPDATE, mapping);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService}.
     */
    protected static void merge(MapperService mapperService, String mapping) throws IOException {
        mapperService.merge(null, new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
    }

    protected static void merge(MapperService mapperService, MapperService.MergeReason reason, String mapping) throws IOException {
        mapperService.merge(null, new CompressedXContent(mapping), reason);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService} with a specific {@code MergeReason}
     */
    protected static void merge(MapperService mapperService, MapperService.MergeReason reason, XContentBuilder mapping) throws IOException {
        mapperService.merge(null, new CompressedXContent(BytesReference.bytes(mapping)), reason);
    }

    protected static XContentBuilder topMapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        buildFields.accept(builder);
        return builder.endObject().endObject();
    }

    protected static XContentBuilder mappingNoSubobjects(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        return topMapping(xContentBuilder -> {
            xContentBuilder.field("subobjects", false);
            xContentBuilder.startObject("properties");
            buildFields.accept(xContentBuilder);
            xContentBuilder.endObject();
        });
    }

    public static XContentBuilder mapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
        buildFields.accept(builder);
        return builder.endObject().endObject().endObject();
    }

    protected static XContentBuilder dynamicMapping(Mapping dynamicMapping) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        dynamicMapping.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder.endObject();
    }

    protected static XContentBuilder fieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return mapping(b -> {
            b.startObject("field");
            buildField.accept(b);
            b.endObject();
        });
    }

    protected static XContentBuilder runtimeFieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return runtimeMapping(b -> {
            b.startObject("field");
            buildField.accept(b);
            b.endObject();
        });
    }

    protected static XContentBuilder runtimeMapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("runtime");
        buildFields.accept(builder);
        return builder.endObject().endObject().endObject();
    }

    private AggregationContext aggregationContext(
        ValuesSourceRegistry valuesSourceRegistry,
        MapperService mapperService,
        IndexSearcher searcher,
        Query query,
        Supplier<SearchLookup> lookupSupplier
    ) {
        return new AggregationContext() {
            private final CircuitBreaker breaker = mock(CircuitBreaker.class);

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
            public Analyzer getNamedAnalyzer(String analyzer) {
                return null;
            }

            @Override
            public Analyzer buildCustomAnalyzer(
                IndexSettings indexSettings,
                boolean normalizer,
                NameOrDefinition tokenizer,
                List<NameOrDefinition> charFilters,
                List<NameOrDefinition> tokenFilters
            ) {
                return null;
            }

            @Override
            public SearchLookup lookup() {
                return lookupSupplier.get();
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
            public ClusterSettings getClusterSettings() {
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
            @SuppressWarnings("unchecked")
            public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                return compileScript(script, context);
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
                return ft.fielddataBuilder(FieldDataContext.noRuntimeFields("test"))
                    .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
            }

            @Override
            public BigArrays bigArrays() {
                return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
            }

            @Override
            public NestedLookup nestedLookup() {
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
            public void removeReleasable(Aggregator aggregator) {
                // TODO we'll have to handle this in the tests eventually
            }

            @Override
            public int maxBuckets() {
                return Integer.MAX_VALUE;
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
            public boolean isInSortOrderExecutionRequired() {
                return false;
            }

            @Override
            public Set<String> sourcePath(String fullName) {
                return Set.of(fullName);
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
        withAggregationContext(mapperService, docs, test, () -> { throw new UnsupportedOperationException(); });
    }

    protected final void withAggregationContext(
        MapperService mapperService,
        List<SourceToParse> docs,
        CheckedConsumer<AggregationContext, IOException> test,
        Supplier<SearchLookup> lookupSupplier
    ) throws IOException {
        withAggregationContext(null, mapperService, docs, null, test, lookupSupplier);
    }

    protected final void withAggregationContext(
        ValuesSourceRegistry valuesSourceRegistry,
        MapperService mapperService,
        List<SourceToParse> docs,
        Query query,
        CheckedConsumer<AggregationContext, IOException> test
    ) throws IOException {
        withAggregationContext(
            valuesSourceRegistry,
            mapperService,
            docs,
            query,
            test,
            () -> { throw new UnsupportedOperationException(); }
        );
    }

    protected final void withAggregationContext(
        ValuesSourceRegistry valuesSourceRegistry,
        MapperService mapperService,
        List<SourceToParse> docs,
        Query query,
        CheckedConsumer<AggregationContext, IOException> test,
        Supplier<SearchLookup> lookupSupplier
    ) throws IOException {
        withLuceneIndex(mapperService, writer -> {
            for (SourceToParse doc : docs) {
                writer.addDocuments(mapperService.documentMapper().parse(doc).docs());

            }
        }, reader -> test.accept(aggregationContext(valuesSourceRegistry, mapperService, newSearcher(reader), query, lookupSupplier)));
    }

    protected SearchExecutionContext createSearchExecutionContext(MapperService mapperService) {
        return createSearchExecutionContext(mapperService, null, Settings.EMPTY);
    }

    protected SearchExecutionContext createSearchExecutionContext(MapperService mapperService, IndexSearcher searcher) {
        return createSearchExecutionContext(mapperService, searcher, Settings.EMPTY);
    }

    protected SearchExecutionContext createSearchExecutionContext(MapperService mapperService, IndexSearcher searcher, Settings settings) {
        Settings mergedSettings = Settings.builder().put(mapperService.getIndexSettings().getSettings()).put(settings).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(mapperService.getIndexSettings().getIndexMetadata())
            .settings(mergedSettings)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        final SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        final long nowInMillis = randomNonNegativeLong();
        return new SearchExecutionContext(0, 0, indexSettings, new BitsetFilterCache(indexSettings, new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {

            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {

            }
        }),
            (ft, fdc) -> ft.fielddataBuilder(fdc).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService()),
            mapperService,
            mapperService.mappingLookup(),
            similarityService,
            MapperServiceTestCase.this::compileScript,
            parserConfig(),
            writableRegistry(),
            null,
            searcher,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            Collections.emptyMap()
        );
    }

    protected TriFunction<MappedFieldType, Supplier<SearchLookup>, MappedFieldType.FielddataOperation, IndexFieldData<?>> fieldDataLookup(
        MapperService mapperService
    ) {
        return fieldDataLookup(mapperService.mappingLookup()::sourcePaths);
    }

    protected TriFunction<MappedFieldType, Supplier<SearchLookup>, MappedFieldType.FielddataOperation, IndexFieldData<?>> fieldDataLookup(
        Function<String, Set<String>> sourcePathsLookup
    ) {
        return (mft, lookupSource, fdo) -> mft.fielddataBuilder(new FieldDataContext("test", lookupSource, sourcePathsLookup, fdo))
            .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
    }

    protected final String syntheticSource(DocumentMapper mapper, CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            LuceneDocument doc = mapper.parse(source(build)).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                String syntheticSource = syntheticSource(mapper, reader, 0);
                roundTripSyntheticSource(mapper, syntheticSource, reader);
                return syntheticSource;
            }
        }
    }

    /*
     * Use the synthetic source to build a *second* index and verify
     * that the synthetic source it produces is the same. And *then*
     * verify that the index's contents are exactly the same. Except
     * for _recovery_source - that won't be the same because it's a
     * precise copy of the bits in _source. And we *know* that frequently
     * the synthetic source won't be the same as the original source.
     * That's the point, really. It'll just be "close enough" for
     * round tripping.
     */
    private void roundTripSyntheticSource(DocumentMapper mapper, String syntheticSource, DirectoryReader reader) throws IOException {
        try (Directory roundTripDirectory = newDirectory()) {
            RandomIndexWriter roundTripIw = new RandomIndexWriter(random(), roundTripDirectory);
            roundTripIw.addDocument(mapper.parse(new SourceToParse("1", new BytesArray(syntheticSource), XContentType.JSON)).rootDoc());
            roundTripIw.close();
            try (DirectoryReader roundTripReader = DirectoryReader.open(roundTripDirectory)) {
                String roundTripSyntheticSource = syntheticSource(mapper, roundTripReader, 0);
                assertThat(roundTripSyntheticSource, equalTo(syntheticSource));
                validateRoundTripReader(syntheticSource, reader, roundTripReader);
            }
        }
    }

    private static String syntheticSource(DocumentMapper mapper, IndexReader reader, int docId) throws IOException {
        SourceProvider provider = SourceProvider.fromSyntheticSource(mapper.mapping());
        Source synthetic = provider.getSource(getOnlyLeafReader(reader).getContext(), docId);
        return synthetic.internalSourceRef().utf8ToString();
    }

    protected static LeafStoredFieldLoader syntheticSourceStoredFieldLoader(
        DocumentMapper mapper,
        LeafReader leafReader,
        SourceLoader loader
    ) throws IOException {
        if (loader.requiredStoredFields().isEmpty()) {
            return StoredFieldLoader.empty().getLoader(leafReader.getContext(), null);
        }
        LeafStoredFieldLoader storedFields = StoredFieldLoader.create(false, loader.requiredStoredFields())
            .getLoader(leafReader.getContext(), null);
        storedFields.advanceTo(0);
        return storedFields;
    }

    protected void validateRoundTripReader(String syntheticSource, DirectoryReader reader, DirectoryReader roundTripReader)
        throws IOException {
        assertReaderEquals(
            "round trip " + syntheticSource,
            new FieldMaskingReader(SourceFieldMapper.RECOVERY_SOURCE_NAME, reader),
            new FieldMaskingReader(SourceFieldMapper.RECOVERY_SOURCE_NAME, roundTripReader)
        );
    }

    protected static XContentBuilder syntheticSourceMapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        return topMapping(b -> {
            b.startObject("_source").field("mode", "synthetic").endObject();
            b.startObject("properties");
            buildFields.accept(b);
            b.endObject();
        });
    }

    protected static XContentBuilder syntheticSourceFieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField)
        throws IOException {
        return syntheticSourceMapping(b -> {
            b.startObject("field");
            buildField.accept(b);
            b.endObject();
        });
    }
}
