/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

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

    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
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

    protected final MapperService createMapperService(Version version, XContentBuilder mapping) throws IOException {
        return createMapperService(version, getIndexSettings(), () -> true, mapping);
    }

    /**
     * Create a {@link MapperService} like we would for an index.
     */
    protected final MapperService createMapperService(Version version,
                                                      Settings settings,
                                                      BooleanSupplier idFieldDataEnabled,
                                                      XContentBuilder mapping) throws IOException {
        settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put(settings)
            .put("index.version.created", version)
            .build();
        IndexMetadata meta = IndexMetadata.builder("index")
            .settings(settings)
            .build();
        IndexSettings indexSettings = new IndexSettings(meta, settings);
        MapperRegistry mapperRegistry = new IndicesModule(
            getPlugins().stream().filter(p -> p instanceof MapperPlugin).map(p -> (MapperPlugin) p).collect(toList())
        ).getMapperRegistry();
        ScriptModule scriptModule = new ScriptModule(
            Settings.EMPTY,
            getPlugins().stream().filter(p -> p instanceof ScriptPlugin).map(p -> (ScriptPlugin) p).collect(toList())
        );
        ScriptService scriptService = new ScriptService(Settings.EMPTY, scriptModule.engines, scriptModule.contexts);
        SimilarityService similarityService = new SimilarityService(indexSettings, scriptService, Map.of());
        MapperService mapperService = new MapperService(
            indexSettings,
            createIndexAnalyzers(indexSettings),
            xContentRegistry(),
            similarityService,
            mapperRegistry,
            () -> { throw new UnsupportedOperationException(); },
            idFieldDataEnabled,
            scriptService
        );
        merge(mapperService, mapping);
        return mapperService;
    }

    protected final void withLuceneIndex(
        MapperService mapperService,
        CheckedConsumer<RandomIndexWriter, IOException> builder,
        CheckedConsumer<IndexReader, IOException> test
    ) throws IOException {
        try (
            Directory dir = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir, new IndexWriterConfig(mapperService.indexAnalyzer()))
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
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        build.accept(builder);
        builder.endObject();
        return new SourceToParse("test", id, BytesReference.bytes(builder), XContentType.JSON, routing);
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

    private AggregationContext aggregationContext(MapperService mapperService, IndexSearcher searcher, Query query) {
        return new AggregationContext() {
            @Override
            public IndexSearcher searcher() {
                return searcher;
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
                throw new UnsupportedOperationException();
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
        };
    }

    protected final void withAggregationContext(
        MapperService mapperService,
        List<SourceToParse> docs,
        CheckedConsumer<AggregationContext, IOException> test
    ) throws IOException {
        withAggregationContext(mapperService, docs, null, test);
    }

    protected final void withAggregationContext(
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
            reader -> test.accept(aggregationContext(mapperService, new IndexSearcher(reader), query))
        );
    }

    protected QueryShardContext createQueryShardContext(MapperService mapperService) {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.getFieldType(anyString())).thenAnswer(inv -> mapperService.fieldType(inv.getArguments()[0].toString()));
        when(queryShardContext.isFieldMapped(anyString()))
            .thenAnswer(inv -> mapperService.fieldType(inv.getArguments()[0].toString()) != null);
        when(queryShardContext.getIndexAnalyzers()).thenReturn(mapperService.getIndexAnalyzers());
        when(queryShardContext.getIndexSettings()).thenReturn(mapperService.getIndexSettings());
        when(queryShardContext.simpleMatchToIndexNames(anyObject())).thenAnswer(
            inv -> mapperService.simpleMatchToFullName(inv.getArguments()[0].toString())
        );
        when(queryShardContext.allowExpensiveQueries()).thenReturn(true);
        when(queryShardContext.lookup()).thenReturn(new SearchLookup(mapperService, (ft, s) -> {
            throw new UnsupportedOperationException("search lookup not available");
        }));
        return queryShardContext;
    }
}
