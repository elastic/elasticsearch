/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStream.BACKING_INDEX_PREFIX;
import static org.elasticsearch.cluster.metadata.DataStream.DATE_FORMATTER;
import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.DataStream.getDefaultFailureStoreName;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.test.ESTestCase.generateRandomStringArray;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomMap;
import static org.elasticsearch.test.ESTestCase.randomMillisUpToYear9999;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class DataStreamTestHelper {

    private static final Settings.Builder SETTINGS = ESTestCase.settings(IndexVersion.current()).put("index.hidden", true);
    private static final int NUMBER_OF_SHARDS = 1;
    private static final int NUMBER_OF_REPLICAS = 1;

    public static DataStream newInstance(String name, List<Index> indices) {
        return newInstance(name, indices, indices.size(), null);
    }

    public static DataStream newInstance(String name, List<Index> indices, List<Index> failureIndices) {
        return newInstance(name, indices, indices.size(), null, false, null, failureIndices);
    }

    public static DataStream newInstance(String name, List<Index> indices, long generation, Map<String, Object> metadata) {
        return newInstance(name, indices, generation, metadata, false);
    }

    public static DataStream newInstance(
        String name,
        List<Index> indices,
        long generation,
        Map<String, Object> metadata,
        boolean replicated
    ) {
        return newInstance(name, indices, generation, metadata, replicated, null);
    }

    public static DataStream newInstance(
        String name,
        List<Index> indices,
        long generation,
        Map<String, Object> metadata,
        boolean replicated,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        return newInstance(name, indices, generation, metadata, replicated, lifecycle, List.of());
    }

    public static DataStream newInstance(
        String name,
        List<Index> indices,
        long generation,
        Map<String, Object> metadata,
        boolean replicated,
        @Nullable DataStreamLifecycle lifecycle,
        @Nullable DataStreamAutoShardingEvent autoShardingEvent
    ) {
        return new DataStream(
            name,
            indices,
            generation,
            metadata,
            false,
            replicated,
            false,
            false,
            null,
            lifecycle,
            false,
            List.of(),
            autoShardingEvent
        );
    }

    public static DataStream newInstance(
        String name,
        List<Index> indices,
        long generation,
        Map<String, Object> metadata,
        boolean replicated,
        @Nullable DataStreamLifecycle lifecycle,
        List<Index> failureStores
    ) {
        return new DataStream(
            name,
            indices,
            generation,
            metadata,
            false,
            replicated,
            false,
            false,
            null,
            lifecycle,
            failureStores.size() > 0,
            failureStores,
            null
        );
    }

    public static String getLegacyDefaultBackingIndexName(
        String dataStreamName,
        long generation,
        long epochMillis,
        boolean isNewIndexNameFormat
    ) {
        if (isNewIndexNameFormat) {
            return String.format(
                Locale.ROOT,
                BACKING_INDEX_PREFIX + "%s-%s-%06d",
                dataStreamName,
                DATE_FORMATTER.formatMillis(epochMillis),
                generation
            );
        } else {
            return getLegacyDefaultBackingIndexName(dataStreamName, generation);
        }
    }

    public static String getLegacyDefaultBackingIndexName(String dataStreamName, long generation) {
        return String.format(Locale.ROOT, BACKING_INDEX_PREFIX + "%s-%06d", dataStreamName, generation);
    }

    public static IndexMetadata.Builder createFirstBackingIndex(String dataStreamName) {
        return createBackingIndex(dataStreamName, 1, System.currentTimeMillis());
    }

    public static IndexMetadata.Builder createFirstBackingIndex(String dataStreamName, long epochMillis) {
        return createBackingIndex(dataStreamName, 1, epochMillis);
    }

    public static IndexMetadata.Builder createBackingIndex(String dataStreamName, int generation) {
        return createBackingIndex(dataStreamName, generation, System.currentTimeMillis());
    }

    public static IndexMetadata.Builder createBackingIndex(String dataStreamName, int generation, long epochMillis) {
        return IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, generation, epochMillis))
            .settings(SETTINGS)
            .numberOfShards(NUMBER_OF_SHARDS)
            .numberOfReplicas(NUMBER_OF_REPLICAS);
    }

    public static IndexMetadata.Builder createFirstFailureStore(String dataStreamName) {
        return createFailureStore(dataStreamName, 1, System.currentTimeMillis());
    }

    public static IndexMetadata.Builder createFirstFailureStore(String dataStreamName, long epochMillis) {
        return createFailureStore(dataStreamName, 1, epochMillis);
    }

    public static IndexMetadata.Builder createFailureStore(String dataStreamName, int generation) {
        return createFailureStore(dataStreamName, generation, System.currentTimeMillis());
    }

    public static IndexMetadata.Builder createFailureStore(String dataStreamName, int generation, long epochMillis) {
        return IndexMetadata.builder(DataStream.getDefaultFailureStoreName(dataStreamName, generation, epochMillis))
            .settings(SETTINGS)
            .numberOfShards(NUMBER_OF_SHARDS)
            .numberOfReplicas(NUMBER_OF_REPLICAS);
    }

    public static IndexMetadata.Builder getIndexMetadataBuilderForIndex(Index index) {
        return IndexMetadata.builder(index.getName())
            .settings(Settings.builder().put(SETTINGS.build()).put(SETTING_INDEX_UUID, index.getUUID()))
            .numberOfShards(NUMBER_OF_SHARDS)
            .numberOfReplicas(NUMBER_OF_REPLICAS);
    }

    public static String generateMapping(String timestampFieldName) {
        return String.format(Locale.ROOT, """
            {
              "_doc":{
                "properties": {
                  "%s": {
                    "type": "date"
                  }
                }
              }
            }""", timestampFieldName);
    }

    public static String generateTsdbMapping() {
        return """
            {
              "_doc":{
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "uid": {
                    "type": "keyword",
                    "time_series_dimension": true
                  }
                }
              }
            }""";
    }

    public static String generateMapping(String timestampFieldName, String type) {
        return "{\n"
            + "      \""
            + DataStreamTimestampFieldMapper.NAME
            + "\": {\n"
            + "        \"enabled\": true\n"
            + "      },"
            + "      \"properties\": {\n"
            + "        \""
            + timestampFieldName
            + "\": {\n"
            + "          \"type\": \""
            + type
            + "\"\n"
            + "        }\n"
            + "      }\n"
            + "    }";
    }

    /**
     * @return a list of random indices. NOTE: the list can be empty, if you do not want an empty list use
     * {@link DataStreamTestHelper#randomNonEmptyIndexInstances()}
     */
    public static List<Index> randomIndexInstances() {
        return randomIndexInstances(0, 128);
    }

    public static List<Index> randomNonEmptyIndexInstances() {
        return randomIndexInstances(1, 128);
    }

    public static List<Index> randomIndexInstances(int min, int max) {
        int numIndices = ESTestCase.randomIntBetween(min, max);
        List<Index> indices = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indices.add(new Index(randomAlphaOfLength(10).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(LuceneTestCase.random())));
        }
        return indices;
    }

    public static DataStream randomInstance() {
        return randomInstance(System::currentTimeMillis);
    }

    public static DataStream randomInstance(boolean failureStore) {
        return randomInstance(System::currentTimeMillis, failureStore);
    }

    public static DataStream randomInstance(String name) {
        return randomInstance(name, System::currentTimeMillis, randomBoolean());
    }

    public static DataStream randomInstance(LongSupplier timeProvider) {
        return randomInstance(timeProvider, randomBoolean());
    }

    public static DataStream randomInstance(LongSupplier timeProvider, boolean failureStore) {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        return randomInstance(dataStreamName, timeProvider, failureStore);
    }

    public static DataStream randomInstance(String dataStreamName, LongSupplier timeProvider, boolean failureStore) {
        List<Index> indices = randomIndexInstances();
        long generation = indices.size() + ESTestCase.randomLongBetween(1, 128);
        indices.add(new Index(getDefaultBackingIndexName(dataStreamName, generation), UUIDs.randomBase64UUID(LuceneTestCase.random())));
        Map<String, Object> metadata = null;
        if (randomBoolean()) {
            metadata = Map.of("key", "value");
        }
        List<Index> failureIndices = List.of();
        generation = generation + ESTestCase.randomLongBetween(1, 128);
        if (failureStore) {
            failureIndices = randomNonEmptyIndexInstances();
            failureIndices.add(
                new Index(
                    getDefaultFailureStoreName(dataStreamName, generation, System.currentTimeMillis()),
                    UUIDs.randomBase64UUID(LuceneTestCase.random())
                )
            );
        }

        return new DataStream(
            dataStreamName,
            indices,
            generation,
            metadata,
            randomBoolean(),
            randomBoolean(),
            false, // Some tests don't work well with system data streams, since these data streams require special handling
            timeProvider,
            randomBoolean(),
            randomBoolean() ? IndexMode.STANDARD : null, // IndexMode.TIME_SERIES triggers validation that many unit tests doesn't pass
            randomBoolean() ? DataStreamLifecycle.newBuilder().dataRetention(randomMillisUpToYear9999()).build() : null,
            failureStore,
            failureIndices,
            randomBoolean(),
            randomBoolean()
                ? new DataStreamAutoShardingEvent(
                    indices.get(indices.size() - 1).getName(),
                    randomIntBetween(1, 10),
                    randomMillisUpToYear9999()
                )
                : null
        );
    }

    public static DataStreamAlias randomAliasInstance() {
        List<String> dataStreams = List.of(generateRandomStringArray(5, 5, false, false));
        return new DataStreamAlias(
            randomAlphaOfLength(5),
            dataStreams,
            randomBoolean() ? randomFrom(dataStreams) : null,
            randomBoolean() ? randomMap(1, 4, () -> new Tuple<>("term", Map.of("year", "2022"))) : null
        );
    }

    @Nullable
    public static DataStreamGlobalRetention randomGlobalRetention() {
        if (randomBoolean()) {
            return null;
        }
        boolean withDefault = randomBoolean();
        return new DataStreamGlobalRetention(
            withDefault ? TimeValue.timeValueDays(randomIntBetween(1, 30)) : null,
            withDefault == false || randomBoolean() ? TimeValue.timeValueDays(randomIntBetween(31, 100)) : null
        );
    }

    /**
     * Constructs {@code ClusterState} with the specified data streams and indices.
     *
     * @param dataStreams The names of the data streams to create with their respective number of backing indices
     * @param indexNames  The names of indices to create that do not back any data streams
     */
    public static ClusterState getClusterStateWithDataStreams(List<Tuple<String, Integer>> dataStreams, List<String> indexNames) {
        return getClusterStateWithDataStreams(dataStreams, indexNames, 1);
    }

    /**
     * Constructs {@code ClusterState} with the specified data streams and indices.
     *
     * @param dataStreams The names of the data streams to create with their respective number of backing indices
     * @param indexNames  The names of indices to create that do not back any data streams
     * @param replicas number of replicas
     */
    public static ClusterState getClusterStateWithDataStreams(
        List<Tuple<String, Integer>> dataStreams,
        List<String> indexNames,
        int replicas
    ) {
        return getClusterStateWithDataStreams(dataStreams, indexNames, System.currentTimeMillis(), Settings.EMPTY, replicas);
    }

    public static ClusterState getClusterStateWithDataStreams(
        List<Tuple<String, Integer>> dataStreams,
        List<String> indexNames,
        long currentTime,
        Settings settings,
        int replicas
    ) {
        return getClusterStateWithDataStreams(dataStreams, indexNames, currentTime, settings, replicas, false);
    }

    public static ClusterState getClusterStateWithDataStreams(
        List<Tuple<String, Integer>> dataStreams,
        List<String> indexNames,
        long currentTime,
        Settings settings,
        int replicas,
        boolean replicated
    ) {
        return getClusterStateWithDataStreams(dataStreams, indexNames, currentTime, settings, replicas, replicated, false);
    }

    public static ClusterState getClusterStateWithDataStreams(
        List<Tuple<String, Integer>> dataStreams,
        List<String> indexNames,
        long currentTime,
        Settings settings,
        int replicas,
        boolean replicated,
        boolean storeFailures
    ) {
        Metadata.Builder builder = Metadata.builder();
        getClusterStateWithDataStreams(builder, dataStreams, indexNames, currentTime, settings, replicas, replicated, storeFailures);
        return ClusterState.builder(new ClusterName("_name")).metadata(builder).build();
    }

    public static void getClusterStateWithDataStreams(
        Metadata.Builder builder,
        List<Tuple<String, Integer>> dataStreams,
        List<String> indexNames,
        long currentTime,
        Settings settings,
        int replicas,
        boolean replicated,
        boolean storeFailures
    ) {
        builder.put(
            "template_1",
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("*"))
                .dataStreamTemplate(
                    new ComposableIndexTemplate.DataStreamTemplate(false, false, DataStream.isFailureStoreEnabled() && storeFailures)
                )
                .build()
        );

        List<IndexMetadata> allIndices = new ArrayList<>();
        for (Tuple<String, Integer> dsTuple : dataStreams) {
            List<IndexMetadata> backingIndices = new ArrayList<>();
            for (int backingIndexNumber = 1; backingIndexNumber <= dsTuple.v2(); backingIndexNumber++) {
                backingIndices.add(
                    createIndexMetadata(getDefaultBackingIndexName(dsTuple.v1(), backingIndexNumber, currentTime), true, settings, replicas)
                );
            }
            allIndices.addAll(backingIndices);

            List<IndexMetadata> failureStores = new ArrayList<>();
            if (DataStream.isFailureStoreEnabled() && storeFailures) {
                for (int failureStoreNumber = 1; failureStoreNumber <= dsTuple.v2(); failureStoreNumber++) {
                    failureStores.add(
                        createIndexMetadata(
                            getDefaultFailureStoreName(dsTuple.v1(), failureStoreNumber, currentTime),
                            true,
                            settings,
                            replicas
                        )
                    );
                }
                allIndices.addAll(failureStores);
            }

            DataStream ds = DataStreamTestHelper.newInstance(
                dsTuple.v1(),
                backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()),
                dsTuple.v2(),
                null,
                replicated,
                null,
                failureStores.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
            );
            builder.put(ds);
        }

        for (String indexName : indexNames) {
            allIndices.add(createIndexMetadata(indexName, false, settings, replicas));
        }

        for (IndexMetadata index : allIndices) {
            builder.put(index, false);
        }
    }

    public static ClusterState getClusterStateWithDataStream(String dataStream, List<Tuple<Instant, Instant>> timeSlices) {
        Metadata.Builder builder = Metadata.builder();
        getClusterStateWithDataStream(builder, dataStream, timeSlices);
        return ClusterState.builder(new ClusterName("_name")).metadata(builder).build();
    }

    public static void getClusterStateWithDataStream(
        Metadata.Builder builder,
        String dataStreamName,
        List<Tuple<Instant, Instant>> timeSlices
    ) {
        List<IndexMetadata> backingIndices = new ArrayList<>();
        DataStream existing = builder.dataStream(dataStreamName);
        if (existing != null) {
            for (Index index : existing.getIndices()) {
                backingIndices.add(builder.getSafe(index));
            }
        }
        long generation = existing != null ? existing.getGeneration() + 1 : 1L;
        for (Tuple<Instant, Instant> tuple : timeSlices) {
            Instant start = tuple.v1();
            Instant end = tuple.v2();
            Settings settings = Settings.builder()
                .put("index.mode", "time_series")
                .put("index.routing_path", "uid")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(start))
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(end))
                .build();
            var im = createIndexMetadata(getDefaultBackingIndexName(dataStreamName, generation, start.toEpochMilli()), true, settings, 0);
            builder.put(im, true);
            backingIndices.add(im);
            generation++;
        }
        DataStream ds = new DataStream(
            dataStreamName,
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()),
            generation,
            existing != null ? existing.getMetadata() : null,
            existing != null && existing.isHidden(),
            existing != null && existing.isReplicated(),
            existing != null && existing.isSystem(),
            existing != null && existing.isAllowCustomRouting(),
            IndexMode.TIME_SERIES
        );
        builder.put(ds);
    }

    private static IndexMetadata createIndexMetadata(String name, boolean hidden, Settings settings, int replicas) {
        Settings.Builder b = Settings.builder()
            .put(settings)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.hidden", hidden);

        return IndexMetadata.builder(name).settings(b).numberOfShards(1).numberOfReplicas(replicas).build();
    }

    public static String backingIndexPattern(String dataStreamName, long generation) {
        return String.format(Locale.ROOT, "\\.ds-%s-(\\d{4}\\.\\d{2}\\.\\d{2}-)?%06d", dataStreamName, generation);
    }

    public static Matcher<String> backingIndexEqualTo(String dataStreamName, int generation) {
        return new TypeSafeMatcher<>() {

            @Override
            protected boolean matchesSafely(String backingIndexName) {
                if (backingIndexName == null) {
                    return false;
                }

                int indexOfLastDash = backingIndexName.lastIndexOf('-');
                String actualDataStreamName = parseDataStreamName(backingIndexName, indexOfLastDash);
                int actualGeneration = parseGeneration(backingIndexName, indexOfLastDash);
                return actualDataStreamName.equals(dataStreamName) && actualGeneration == generation;
            }

            @Override
            protected void describeMismatchSafely(String backingIndexName, Description mismatchDescription) {
                int indexOfLastDash = backingIndexName.lastIndexOf('-');
                String dataStreamName = parseDataStreamName(backingIndexName, indexOfLastDash);
                int generation = parseGeneration(backingIndexName, indexOfLastDash);
                mismatchDescription.appendText(" was data stream name ")
                    .appendValue(dataStreamName)
                    .appendText(" and generation ")
                    .appendValue(generation);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("expected data stream name ")
                    .appendValue(dataStreamName)
                    .appendText(" and expected generation ")
                    .appendValue(generation);
            }

            private static String parseDataStreamName(String backingIndexName, int indexOfLastDash) {
                return backingIndexName.substring(4, backingIndexName.lastIndexOf('-', indexOfLastDash - 1));
            }

            private static int parseGeneration(String backingIndexName, int indexOfLastDash) {
                return Integer.parseInt(backingIndexName.substring(indexOfLastDash + 1));
            }
        };
    }

    public static MetadataRolloverService getMetadataRolloverService(
        DataStream dataStream,
        ThreadPool testThreadPool,
        Set<IndexSettingProvider> providers,
        NamedXContentRegistry registry
    ) throws Exception {
        DateFieldMapper dateFieldMapper = new DateFieldMapper.Builder(
            "@timestamp",
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            ScriptCompiler.NONE,
            false,
            IndexVersion.current()
        ).build(MapperBuilderContext.root(false, true));
        ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool);
        Environment env = mock(Environment.class);
        when(env.sharedDataFile()).thenReturn(null);
        AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(ClusterState.class), any(String.class), any())).then(i -> i.getArguments()[0]);
        when(allocationService.getShardRoutingRoleStrategy()).thenReturn(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        MappingLookup mappingLookup = null;
        if (dataStream != null) {
            RootObjectMapper.Builder root = new RootObjectMapper.Builder("_doc", ObjectMapper.Defaults.SUBOBJECTS);
            root.add(
                new DateFieldMapper.Builder(
                    DataStream.TIMESTAMP_FIELD_NAME,
                    DateFieldMapper.Resolution.MILLISECONDS,
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                    ScriptCompiler.NONE,
                    true,
                    IndexVersion.current()
                )
            );
            MetadataFieldMapper dtfm = getDataStreamTimestampFieldMapper();
            Mapping mapping = new Mapping(
                root.build(MapperBuilderContext.root(false, true)),
                new MetadataFieldMapper[] { dtfm },
                Collections.emptyMap()
            );
            mappingLookup = MappingLookup.fromMappers(mapping, List.of(dtfm, dateFieldMapper), List.of(), List.of());
        }
        IndicesService indicesService = mockIndicesServices(mappingLookup);

        ShardLimitValidator shardLimitValidator = new ShardLimitValidator(Settings.EMPTY, clusterService);
        MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            indicesService,
            allocationService,
            shardLimitValidator,
            env,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            testThreadPool,
            null,
            EmptySystemIndices.INSTANCE,
            false,
            new IndexSettingProviders(providers)
        );
        MetadataIndexAliasesService indexAliasesService = new MetadataIndexAliasesService(clusterService, indicesService, registry);
        return new MetadataRolloverService(
            testThreadPool,
            createIndexService,
            indexAliasesService,
            EmptySystemIndices.INSTANCE,
            WriteLoadForecaster.DEFAULT,
            clusterService
        );
    }

    public static MetadataFieldMapper getDataStreamTimestampFieldMapper() {
        Map<String, Object> fieldsMapping = new HashMap<>();
        fieldsMapping.put("enabled", true);
        MappingParserContext mockedParserContext = mock(MappingParserContext.class);
        return DataStreamTimestampFieldMapper.PARSER.parse("field", fieldsMapping, mockedParserContext).build();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static IndicesService mockIndicesServices(MappingLookup mappingLookup) throws Exception {
        /*
         * Throws Exception because Eclipse uses the lower bound for
         * CheckedFunction's exception type so it thinks the "when" call
         * can throw Exception. javac seems to be ok inferring something
         * else.
         */
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.withTempIndexService(any(IndexMetadata.class), any(CheckedFunction.class))).then(invocationOnMock -> {
            IndexService indexService = mock(IndexService.class);
            IndexMetadata indexMetadata = (IndexMetadata) invocationOnMock.getArguments()[0];
            when(indexService.index()).thenReturn(indexMetadata.getIndex());
            MapperService mapperService = mock(MapperService.class);

            RootObjectMapper root = new RootObjectMapper.Builder(MapperService.SINGLE_MAPPING_NAME, ObjectMapper.Defaults.SUBOBJECTS).build(
                MapperBuilderContext.root(false, false)
            );
            Mapping mapping = new Mapping(root, new MetadataFieldMapper[0], null);
            DocumentMapper documentMapper = mock(DocumentMapper.class);
            when(documentMapper.mapping()).thenReturn(mapping);
            when(documentMapper.mappingSource()).thenReturn(mapping.toCompressedXContent());
            RoutingFieldMapper routingFieldMapper = mock(RoutingFieldMapper.class);
            when(routingFieldMapper.required()).thenReturn(false);
            when(documentMapper.routingFieldMapper()).thenReturn(routingFieldMapper);

            when(mapperService.documentMapper()).thenReturn(documentMapper);
            when(indexService.mapperService()).thenReturn(mapperService);
            when(mapperService.mappingLookup()).thenReturn(mappingLookup);
            when(indexService.getIndexEventListener()).thenReturn(new IndexEventListener() {
            });
            when(indexService.getIndexSortSupplier()).thenReturn(() -> null);
            return ((CheckedFunction<IndexService, ?, ?>) invocationOnMock.getArguments()[1]).apply(indexService);
        });
        return indicesService;
    }

}
