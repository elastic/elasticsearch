/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createIndexMetadata;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.getClusterStateWithDataStreams;
import static org.elasticsearch.test.LambdaMatchers.transformedItemsMatch;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TransportGetDataStreamsActionTests extends ESTestCase {

    private final IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
    private final SystemIndices systemIndices = new SystemIndices(List.of());
    private final DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings = DataStreamGlobalRetentionSettings.create(
        ClusterSettings.createBuiltInClusterSettings()
    );
    private final DataStreamFailureStoreSettings emptyDataStreamFailureStoreSettings = DataStreamFailureStoreSettings.create(
        ClusterSettings.createBuiltInClusterSettings()
    );

    public void testGetDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(dataStreamName, 1)), List.of());
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName });
        List<DataStream> dataStreams = TransportGetDataStreamsAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams, transformedItemsMatch(DataStream::getName, contains(dataStreamName)));
    }

    public void testGetDataStreamsWithWildcards() {
        final String[] dataStreamNames = { "my-data-stream", "another-data-stream" };
        ClusterState cs = getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamNames[0], 1), new Tuple<>(dataStreamNames[1], 1)),
            List.of()
        );

        GetDataStreamAction.Request req = new GetDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamNames[1].substring(0, 5) + "*" }
        );
        List<DataStream> dataStreams = TransportGetDataStreamsAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams, transformedItemsMatch(DataStream::getName, contains(dataStreamNames[1])));

        req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "*" });
        dataStreams = TransportGetDataStreamsAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams, transformedItemsMatch(DataStream::getName, contains(dataStreamNames[1], dataStreamNames[0])));

        req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, (String[]) null);
        dataStreams = TransportGetDataStreamsAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams, transformedItemsMatch(DataStream::getName, contains(dataStreamNames[1], dataStreamNames[0])));

        req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "matches-none*" });
        dataStreams = TransportGetDataStreamsAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams, empty());
    }

    public void testGetDataStreamsWithoutWildcards() {
        final String[] dataStreamNames = { "my-data-stream", "another-data-stream" };
        ClusterState cs = getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamNames[0], 1), new Tuple<>(dataStreamNames[1], 1)),
            List.of()
        );

        GetDataStreamAction.Request req = new GetDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamNames[0], dataStreamNames[1] }
        );
        List<DataStream> dataStreams = TransportGetDataStreamsAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams, transformedItemsMatch(DataStream::getName, contains(dataStreamNames[1], dataStreamNames[0])));

        req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamNames[1] });
        dataStreams = TransportGetDataStreamsAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams, transformedItemsMatch(DataStream::getName, contains(dataStreamNames[1])));

        req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamNames[0] });
        dataStreams = TransportGetDataStreamsAction.getDataStreams(cs, resolver, req);
        assertThat(dataStreams, transformedItemsMatch(DataStream::getName, contains(dataStreamNames[0])));

        GetDataStreamAction.Request req2 = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "foo" });
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> TransportGetDataStreamsAction.getDataStreams(cs, resolver, req2)
        );
        assertThat(e.getMessage(), containsString("no such index [foo]"));
    }

    public void testGetNonexistentDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName });
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> TransportGetDataStreamsAction.getDataStreams(cs, resolver, req)
        );
        assertThat(e.getMessage(), containsString("no such index [" + dataStreamName + "]"));
    }

    public void testGetTimeSeriesDataStream() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String dataStream1 = "ds-1";
        String dataStream2 = "ds-2";
        Instant sixHoursAgo = now.minus(6, ChronoUnit.HOURS);
        Instant fourHoursAgo = now.minus(4, ChronoUnit.HOURS);
        Instant twoHoursAgo = now.minus(2, ChronoUnit.HOURS);
        Instant twoHoursAhead = now.plus(2, ChronoUnit.HOURS);

        ClusterState state;
        {
            var mBuilder = new Metadata.Builder();
            DataStreamTestHelper.getClusterStateWithDataStream(
                mBuilder,
                dataStream1,
                List.of(
                    new Tuple<>(sixHoursAgo, fourHoursAgo),
                    new Tuple<>(fourHoursAgo, twoHoursAgo),
                    new Tuple<>(twoHoursAgo, twoHoursAhead)
                )
            );
            DataStreamTestHelper.getClusterStateWithDataStream(
                mBuilder,
                dataStream2,
                List.of(
                    new Tuple<>(sixHoursAgo, fourHoursAgo),
                    new Tuple<>(fourHoursAgo, twoHoursAgo),
                    new Tuple<>(twoHoursAgo, twoHoursAhead)
                )
            );
            state = ClusterState.builder(new ClusterName("_name")).metadata(mBuilder).build();
        }

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertThat(
            response.getDataStreams(),
            contains(
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream1)),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)))
                ),
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream2)),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)))
                )
            )
        );

        // Remove the middle backing index first data stream, so that there is time gap in the data stream:
        {
            Metadata.Builder mBuilder = Metadata.builder(state.getMetadata());
            DataStream dataStream = state.getMetadata().dataStreams().get(dataStream1);
            mBuilder.put(dataStream.removeBackingIndex(dataStream.getIndices().get(1)));
            mBuilder.remove(dataStream.getIndices().get(1).getName());
            state = ClusterState.builder(state).metadata(mBuilder).build();
        }
        response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertThat(
            response.getDataStreams(),
            contains(
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream1)),
                    transformedMatch(
                        d -> d.getTimeSeries().temporalRanges(),
                        contains(new Tuple<>(sixHoursAgo, fourHoursAgo), new Tuple<>(twoHoursAgo, twoHoursAhead))
                    )
                ),
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream2)),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)))
                )
            )
        );
    }

    public void testGetTimeSeriesDataStreamWithOutOfOrderIndices() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String dataStream = "ds-1";
        Instant sixHoursAgo = now.minus(6, ChronoUnit.HOURS);
        Instant fourHoursAgo = now.minus(4, ChronoUnit.HOURS);
        Instant twoHoursAgo = now.minus(2, ChronoUnit.HOURS);
        Instant twoHoursAhead = now.plus(2, ChronoUnit.HOURS);

        ClusterState state;
        {
            var mBuilder = new Metadata.Builder();
            DataStreamTestHelper.getClusterStateWithDataStream(
                mBuilder,
                dataStream,
                List.of(
                    new Tuple<>(fourHoursAgo, twoHoursAgo),
                    new Tuple<>(sixHoursAgo, fourHoursAgo),
                    new Tuple<>(twoHoursAgo, twoHoursAhead)
                )
            );
            state = ClusterState.builder(new ClusterName("_name")).metadata(mBuilder).build();
        }

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertThat(
            response.getDataStreams(),
            contains(
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream)),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(sixHoursAgo, twoHoursAhead)))
                )
            )
        );
    }

    public void testGetTimeSeriesMixedDataStream() {
        Instant instant = Instant.parse("2023-06-06T14:00:00.000Z").truncatedTo(ChronoUnit.SECONDS);
        String dataStream1 = "ds-1";
        Instant twoHoursAgo = instant.minus(2, ChronoUnit.HOURS);
        Instant twoHoursAhead = instant.plus(2, ChronoUnit.HOURS);

        ClusterState state;
        {
            var mBuilder = new Metadata.Builder();
            DataStreamTestHelper.getClusterStateWithDataStreams(
                mBuilder,
                List.of(Tuple.tuple(dataStream1, 2)),
                List.of(),
                instant.toEpochMilli(),
                Settings.EMPTY,
                0,
                false,
                false
            );
            DataStreamTestHelper.getClusterStateWithDataStream(mBuilder, dataStream1, List.of(new Tuple<>(twoHoursAgo, twoHoursAhead)));
            state = ClusterState.builder(new ClusterName("_name")).metadata(mBuilder).build();
        }

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );

        var name1 = getDefaultBackingIndexName("ds-1", 1, instant.toEpochMilli());
        var name2 = getDefaultBackingIndexName("ds-1", 2, instant.toEpochMilli());
        var name3 = getDefaultBackingIndexName("ds-1", 3, twoHoursAgo.toEpochMilli());
        assertThat(
            response.getDataStreams(),
            contains(
                allOf(
                    transformedMatch(d -> d.getDataStream().getName(), equalTo(dataStream1)),
                    transformedMatch(
                        d -> d.getDataStream().getIndices().stream().map(Index::getName).toList(),
                        contains(name1, name2, name3)
                    ),
                    transformedMatch(d -> d.getTimeSeries().temporalRanges(), contains(new Tuple<>(twoHoursAgo, twoHoursAhead)))
                )
            )
        );
    }

    public void testPassingGlobalRetention() {
        ClusterState state;
        {
            var mBuilder = new Metadata.Builder();
            DataStreamTestHelper.getClusterStateWithDataStreams(
                mBuilder,
                List.of(Tuple.tuple("data-stream-1", 2)),
                List.of(),
                System.currentTimeMillis(),
                Settings.EMPTY,
                0,
                false,
                false
            );
            state = ClusterState.builder(new ClusterName("_name")).metadata(mBuilder).build();
        }

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertThat(response.getDataGlobalRetention(), nullValue());
        DataStreamGlobalRetention dataGlobalRetention = new DataStreamGlobalRetention(
            TimeValue.timeValueDays(randomIntBetween(1, 5)),
            TimeValue.timeValueDays(randomIntBetween(5, 10))
        );
        DataStreamGlobalRetentionSettings withGlobalRetentionSettings = DataStreamGlobalRetentionSettings.create(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder()
                    .put(
                        DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(),
                        dataGlobalRetention.defaultRetention()
                    )
                    .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), dataGlobalRetention.maxRetention())
                    .build()
            )
        );
        response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            withGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertThat(response.getDataGlobalRetention(), equalTo(dataGlobalRetention));
        // We used the default failures retention here which is greater than the max
        assertThat(response.getFailuresGlobalRetention(), equalTo(new DataStreamGlobalRetention(null, dataGlobalRetention.maxRetention())));
    }

    public void testDataStreamIsFailureStoreEffectivelyEnabled_disabled() {
        var metadata = new Metadata.Builder();
        DataStreamTestHelper.getClusterStateWithDataStreams(
            metadata,
            List.of(Tuple.tuple("data-stream-1", 2)),
            List.of(),
            System.currentTimeMillis(),
            Settings.EMPTY,
            0,
            false,
            false
        );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertThat(response.getDataStreams(), hasSize(1));
        assertThat(response.getDataStreams().get(0).isFailureStoreEffectivelyEnabled(), is(false));
    }

    public void testDataStreamIsFailureStoreEffectivelyEnabled_enabledExplicitly() {
        var metadata = new Metadata.Builder();
        DataStreamTestHelper.getClusterStateWithDataStreams(
            metadata,
            List.of(Tuple.tuple("data-stream-1", 2)),
            List.of(),
            System.currentTimeMillis(),
            Settings.EMPTY,
            0,
            false,
            true
        );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertThat(response.getDataStreams(), hasSize(1));
        assertThat(response.getDataStreams().get(0).isFailureStoreEffectivelyEnabled(), is(true));
    }

    public void testDataStreamIsFailureStoreEffectivelyEnabled_enabledByClusterSetting() {
        var metadata = new Metadata.Builder();
        DataStreamTestHelper.getClusterStateWithDataStreams(
            metadata,
            List.of(Tuple.tuple("data-stream-1", 2)),
            List.of(),
            System.currentTimeMillis(),
            Settings.EMPTY,
            0,
            false,
            false
        );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            DataStreamFailureStoreSettings.create(
                ClusterSettings.createBuiltInClusterSettings(
                    Settings.builder()
                        .put(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), "data-stream-*")
                        .build()
                )
            ),
            new IndexSettingProviders(Set.of()),
            null
        );
        assertThat(response.getDataStreams(), hasSize(1));
        assertThat(response.getDataStreams().get(0).isFailureStoreEffectivelyEnabled(), is(true));
    }

    public void testProvidersAffectMode() {
        ClusterState state = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(Tuple.tuple("data-stream-1", 2)),
            List.of(),
            System.currentTimeMillis(),
            Settings.EMPTY,
            0,
            false,
            false
        );

        var req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        var response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(
                Set.of(
                    (
                        indexName,
                        dataStreamName,
                        templateIndexMode,
                        metadata,
                        resolvedAt,
                        indexTemplateAndCreateRequestSettings,
                        combinedTemplateMappings) -> Settings.builder().put("index.mode", IndexMode.LOOKUP).build()
                )
            ),
            null
        );
        assertThat(response.getDataStreams().get(0).getIndexModeName(), equalTo("lookup"));
        assertThat(
            response.getDataStreams()
                .get(0)
                .getIndexSettingsValues()
                .values()
                .stream()
                .findFirst()
                .map(GetDataStreamAction.Response.IndexProperties::indexMode)
                .orElse("bad"),
            equalTo("standard")
        );
    }

    public void testGetEffectiveSettingsTemplateOnlySettings() {
        // Set a lifecycle only in the template, and make sure that is in the response:
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        final String templatePolicy = "templatePolicy";
        final String templateIndexMode = IndexMode.LOOKUP.getName();

        ClusterState state = getClusterStateWithDataStreamWithSettings(
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, templatePolicy)
                .put(IndexSettings.MODE.getKey(), templateIndexMode)
                .build(),
            Settings.EMPTY,
            Settings.EMPTY
        );

        GetDataStreamAction.Response response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertNotNull(response.getDataStreams());
        assertThat(response.getDataStreams().size(), equalTo(1));
        assertThat(response.getDataStreams().get(0).getIlmPolicy(), equalTo(templatePolicy));
        assertThat(response.getDataStreams().get(0).getIndexModeName(), equalTo(templateIndexMode));
    }

    public void testGetEffectiveSettingsComponentTemplateOnlySettings() {
        // Set a lifecycle only in the template, and make sure that is in the response:
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        final String templatePolicy = "templatePolicy";
        final String templateIndexMode = IndexMode.LOOKUP.getName();

        ClusterState state = getClusterStateWithDataStreamWithSettings(
            Settings.EMPTY,
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, templatePolicy)
                .put(IndexSettings.MODE.getKey(), templateIndexMode)
                .build(),
            Settings.EMPTY
        );

        GetDataStreamAction.Response response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertNotNull(response.getDataStreams());
        assertThat(response.getDataStreams().size(), equalTo(1));
        assertThat(response.getDataStreams().get(0).getIlmPolicy(), equalTo(templatePolicy));
        assertThat(response.getDataStreams().get(0).getIndexModeName(), equalTo(templateIndexMode));
    }

    public void testGetEffectiveSettings() {
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] {});
        final String templatePolicy = "templatePolicy";
        final String templateIndexMode = IndexMode.LOOKUP.getName();
        final String dataStreamPolicy = "dataStreamPolicy";
        final String dataStreamIndexMode = IndexMode.LOGSDB.getName();
        // Now set a lifecycle in both the template and the data stream, and make sure the response has the data stream one:
        ClusterState state = getClusterStateWithDataStreamWithSettings(
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, templatePolicy)
                .put(IndexSettings.MODE.getKey(), templateIndexMode)
                .build(),
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, templatePolicy)
                .put(IndexSettings.MODE.getKey(), templateIndexMode)
                .build(),
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, dataStreamPolicy)
                .put(IndexSettings.MODE.getKey(), dataStreamIndexMode)
                .build()
        );
        GetDataStreamAction.Response response = TransportGetDataStreamsAction.innerOperation(
            state,
            req,
            resolver,
            systemIndices,
            ClusterSettings.createBuiltInClusterSettings(),
            dataStreamGlobalRetentionSettings,
            emptyDataStreamFailureStoreSettings,
            new IndexSettingProviders(Set.of()),
            null
        );
        assertNotNull(response.getDataStreams());
        assertThat(response.getDataStreams().size(), equalTo(1));
        assertThat(response.getDataStreams().get(0).getIlmPolicy(), equalTo(dataStreamPolicy));
        assertThat(response.getDataStreams().get(0).getIndexModeName(), equalTo(dataStreamIndexMode));
    }

    private static ClusterState getClusterStateWithDataStreamWithSettings(
        Settings templateSettings,
        Settings componentTemplateSettings,
        Settings dataStreamSettings
    ) {
        String dataStreamName = "data-stream-1";
        int numberOfBackingIndices = randomIntBetween(1, 5);
        long currentTime = System.currentTimeMillis();
        int replicas = 0;
        boolean replicated = false;
        Metadata.Builder builder = Metadata.builder();
        builder.put(
            "template_1",
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("*"))
                .template(Template.builder().settings(templateSettings))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .componentTemplates(List.of("component_template_1"))
                .build()
        );
        ComponentTemplate componentTemplate = new ComponentTemplate(
            Template.builder().settings(componentTemplateSettings).build(),
            null,
            null,
            null
        );
        builder.componentTemplates(Map.of("component_template_1", componentTemplate));

        List<IndexMetadata> backingIndices = new ArrayList<>();
        for (int backingIndexNumber = 1; backingIndexNumber <= numberOfBackingIndices; backingIndexNumber++) {
            backingIndices.add(
                createIndexMetadata(
                    getDefaultBackingIndexName(dataStreamName, backingIndexNumber, currentTime),
                    true,
                    templateSettings,
                    replicas
                )
            );
        }
        List<IndexMetadata> allIndices = new ArrayList<>(backingIndices);

        DataStream ds = DataStream.builder(
            dataStreamName,
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
        ).setGeneration(numberOfBackingIndices).setSettings(dataStreamSettings).setReplicated(replicated).build();
        builder.put(ds);

        for (IndexMetadata index : allIndices) {
            builder.put(index, false);
        }
        return ClusterState.builder(new ClusterName("_name")).metadata(builder.build()).build();
    }
}
