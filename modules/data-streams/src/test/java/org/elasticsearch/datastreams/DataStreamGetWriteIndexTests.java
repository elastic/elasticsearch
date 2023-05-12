/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.datastreams.MetadataDataStreamRolloverServiceTests.createSettingsProvider;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataStreamGetWriteIndexTests extends ESTestCase {

    public static final DateFormatter MILLIS_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");
    public static final DateFormatter NANOS_FORMATTER = DateFormatter.forPattern("strict_date_optional_time_nanos");

    private ThreadPool testThreadPool;
    private MetadataRolloverService rolloverService;
    private MetadataCreateDataStreamService createDataStreamService;

    public void testPickingBackingIndicesPredefinedDates() throws Exception {
        Instant time = DateFormatters.from(MILLIS_FORMATTER.parse("2022-03-15T08:29:36.547Z")).toInstant();

        var state = createInitialState();
        state = createDataStream(state, "logs-myapp", time);
        IndexMetadata backingIndex = state.getMetadata().index(".ds-logs-myapp-2022.03.15-000001");
        assertThat(backingIndex, notNullValue());
        // Ensure truncate to seconds:
        assertThat(backingIndex.getSettings().get("index.time_series.start_time"), equalTo("2022-03-15T06:29:36.000Z"));
        assertThat(backingIndex.getSettings().get("index.time_series.end_time"), equalTo("2022-03-15T10:29:36.000Z"));

        // advance time and rollover:
        time = time.plusSeconds(80 * 60);
        var result = rolloverOver(state, "logs-myapp", time);
        state = result.clusterState();

        DataStream dataStream = state.getMetadata().dataStreams().get("logs-myapp");
        backingIndex = state.getMetadata().index(dataStream.getIndices().get(1));
        assertThat(backingIndex, notNullValue());
        assertThat(backingIndex.getSettings().get("index.time_series.start_time"), equalTo("2022-03-15T10:29:36.000Z"));
        assertThat(backingIndex.getSettings().get("index.time_series.end_time"), equalTo("2022-03-15T12:29:36.000Z"));
        String secondBackingIndex = backingIndex.getIndex().getName();

        // first backing index:
        {
            long start = MILLIS_FORMATTER.parseMillis("2022-03-15T06:29:36.000Z");
            long end = MILLIS_FORMATTER.parseMillis("2022-03-15T10:29:36.000Z") - 1;
            for (int i = 0; i < 256; i++) {
                String timestamp = MILLIS_FORMATTER.formatMillis(randomLongBetween(start, end));
                var writeIndex = getWriteIndex(state, "logs-myapp", timestamp);
                assertThat(writeIndex.getName(), equalTo(".ds-logs-myapp-2022.03.15-000001"));
            }
        }

        // Borderline:
        {
            var writeIndex = getWriteIndex(state, "logs-myapp", "2022-03-15T10:29:35.999Z");
            assertThat(writeIndex.getName(), equalTo(".ds-logs-myapp-2022.03.15-000001"));
        }

        // Second backing index:
        {
            long start = MILLIS_FORMATTER.parseMillis("2022-03-15T10:29:36.000Z");
            long end = MILLIS_FORMATTER.parseMillis("2022-03-15T12:29:36.000Z") - 1;
            for (int i = 0; i < 256; i++) {
                String timestamp = MILLIS_FORMATTER.formatMillis(randomLongBetween(start, end));
                var writeIndex = getWriteIndex(state, "logs-myapp", timestamp);
                assertThat(writeIndex.getName(), equalTo(secondBackingIndex));
            }
        }

        // Borderline (again):
        {
            var writeIndex = getWriteIndex(state, "logs-myapp", "2022-03-15T12:29:35.999Z");
            assertThat(writeIndex.getName(), equalTo(secondBackingIndex));
        }

        // Outside the valid temporal ranges:
        {
            var finalState = state;
            var e = expectThrows(IllegalArgumentException.class, () -> getWriteIndex(finalState, "logs-myapp", "2022-03-15T12:29:36.000Z"));
            assertThat(
                e.getMessage(),
                equalTo(
                    "the document timestamp [2022-03-15T12:29:36.000Z] is outside of ranges of currently writable indices ["
                        + "[2022-03-15T06:29:36.000Z,2022-03-15T10:29:36.000Z][2022-03-15T10:29:36.000Z,2022-03-15T12:29:36.000Z]]"
                )
            );
        }
    }

    public void testPickingBackingIndicesNanoTimestamp() throws Exception {
        Instant time = DateFormatters.from(NANOS_FORMATTER.parse("2022-03-15T08:29:36.123456789Z")).toInstant();

        var state = createInitialState();
        state = createDataStream(state, "logs-myapp", time);
        IndexMetadata backingIndex = state.getMetadata().index(".ds-logs-myapp-2022.03.15-000001");
        assertThat(backingIndex, notNullValue());
        // Ensure truncate to seconds and millis format:
        assertThat(backingIndex.getSettings().get("index.time_series.start_time"), equalTo("2022-03-15T06:29:36.000Z"));
        assertThat(backingIndex.getSettings().get("index.time_series.end_time"), equalTo("2022-03-15T10:29:36.000Z"));

        // advance time and rollover:
        time = time.plusSeconds(80 * 60);
        var result = rolloverOver(state, "logs-myapp", time);
        state = result.clusterState();

        DataStream dataStream = state.getMetadata().dataStreams().get("logs-myapp");
        backingIndex = state.getMetadata().index(dataStream.getIndices().get(1));
        assertThat(backingIndex, notNullValue());
        assertThat(backingIndex.getSettings().get("index.time_series.start_time"), equalTo("2022-03-15T10:29:36.000Z"));
        assertThat(backingIndex.getSettings().get("index.time_series.end_time"), equalTo("2022-03-15T12:29:36.000Z"));
        String secondBackingIndex = backingIndex.getIndex().getName();

        // first backing index:
        {
            long start = NANOS_FORMATTER.parseMillis("2022-03-15T06:29:36.000000000Z");
            long end = NANOS_FORMATTER.parseMillis("2022-03-15T10:29:36.000000000Z") - 1;
            for (int i = 0; i < 256; i++) {
                String timestamp = NANOS_FORMATTER.formatMillis(randomLongBetween(start, end));
                var writeIndex = getWriteIndex(state, "logs-myapp", timestamp);
                assertThat(writeIndex.getName(), equalTo(".ds-logs-myapp-2022.03.15-000001"));
            }
        }

        // Borderline:
        {
            var writeIndex = getWriteIndex(state, "logs-myapp", "2022-03-15T10:29:35.999999999Z");
            assertThat(writeIndex.getName(), equalTo(".ds-logs-myapp-2022.03.15-000001"));
        }

        // Second backing index:
        {
            long start = NANOS_FORMATTER.parseMillis("2022-03-15T10:29:36.000000000Z");
            long end = NANOS_FORMATTER.parseMillis("2022-03-15T12:29:36.000000000Z") - 1;
            for (int i = 0; i < 256; i++) {
                String timestamp = NANOS_FORMATTER.formatMillis(randomLongBetween(start, end));
                var writeIndex = getWriteIndex(state, "logs-myapp", timestamp);
                assertThat(writeIndex.getName(), equalTo(secondBackingIndex));
            }
        }

        // Borderline (again):
        {
            var writeIndex = getWriteIndex(state, "logs-myapp", "2022-03-15T12:29:35.999999999Z");
            assertThat(writeIndex.getName(), equalTo(secondBackingIndex));
        }
    }

    @Before
    public void setup() throws Exception {
        testThreadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool);

        IndicesService indicesService;
        {
            DateFieldMapper dateFieldMapper = new DateFieldMapper.Builder(
                "@timestamp",
                DateFieldMapper.Resolution.MILLISECONDS,
                null,
                ScriptCompiler.NONE,
                false,
                IndexVersion.CURRENT
            ).build(MapperBuilderContext.root(false));
            RootObjectMapper.Builder root = new RootObjectMapper.Builder("_doc", ObjectMapper.Defaults.SUBOBJECTS);
            root.add(
                new DateFieldMapper.Builder(
                    "@timestamp",
                    DateFieldMapper.Resolution.MILLISECONDS,
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                    ScriptCompiler.NONE,
                    true,
                    IndexVersion.CURRENT
                )
            );
            MetadataFieldMapper dtfm = DataStreamTestHelper.getDataStreamTimestampFieldMapper();
            Mapping mapping = new Mapping(
                root.build(MapperBuilderContext.root(false)),
                new MetadataFieldMapper[] { dtfm },
                Collections.emptyMap()
            );
            MappingLookup mappingLookup = MappingLookup.fromMappers(mapping, List.of(dtfm, dateFieldMapper), List.of(), List.of());
            indicesService = DataStreamTestHelper.mockIndicesServices(mappingLookup);
        }

        MetadataCreateIndexService createIndexService;
        {
            Environment env = mock(Environment.class);
            when(env.sharedDataFile()).thenReturn(null);
            AllocationService allocationService = mock(AllocationService.class);
            when(allocationService.reroute(any(ClusterState.class), any(String.class), any())).then(i -> i.getArguments()[0]);
            when(allocationService.getShardRoutingRoleStrategy()).thenReturn(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            ShardLimitValidator shardLimitValidator = new ShardLimitValidator(Settings.EMPTY, clusterService);
            createIndexService = new MetadataCreateIndexService(
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
                new IndexSettingProviders(Set.of(createSettingsProvider(xContentRegistry())))
            );
        }
        {
            MetadataIndexAliasesService indexAliasesService = new MetadataIndexAliasesService(
                clusterService,
                indicesService,
                null,
                xContentRegistry()
            );
            rolloverService = new MetadataRolloverService(
                testThreadPool,
                createIndexService,
                indexAliasesService,
                EmptySystemIndices.INSTANCE,
                WriteLoadForecaster.DEFAULT
            );
        }

        createDataStreamService = new MetadataCreateDataStreamService(testThreadPool, clusterService, createIndexService);
    }

    @After
    public void cleanup() {
        testThreadPool.shutdownNow();
    }

    private ClusterState createInitialState() {
        ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(List.of("logs-*"))
            .template(
                new Template(Settings.builder().put("index.mode", "time_series").put("index.routing_path", "uid").build(), null, null)
            )
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
            .build();
        Metadata.Builder builder = Metadata.builder();
        builder.put("template", template);
        return ClusterState.builder(ClusterState.EMPTY_STATE).metadata(builder).build();
    }

    private ClusterState createDataStream(ClusterState state, String name, Instant time) throws Exception {
        var request = new MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest(
            name,
            time.toEpochMilli(),
            null,
            TimeValue.ZERO,
            TimeValue.ZERO,
            false
        );
        return createDataStreamService.createDataStream(request, state, ActionListener.noop());
    }

    private MetadataRolloverService.RolloverResult rolloverOver(ClusterState state, String name, Instant time) throws Exception {
        MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
        List<Condition<?>> metConditions = Collections.singletonList(condition);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");
        return rolloverService.rolloverClusterState(state, name, null, createIndexRequest, metConditions, time, false, false, null);
    }

    private Index getWriteIndex(ClusterState state, String name, String timestamp) {
        var ia = state.getMetadata().getIndicesLookup().get(name);
        assertThat(ia, notNullValue());
        IndexRequest indexRequest = new IndexRequest(name);
        indexRequest.opType(DocWriteRequest.OpType.CREATE);
        if (randomBoolean()) {
            indexRequest.source(Map.of("@timestamp", timestamp));
        } else {
            indexRequest.setRawTimestamp(timestamp);
        }
        return ia.getWriteIndex(indexRequest, state.getMetadata());
    }

}
