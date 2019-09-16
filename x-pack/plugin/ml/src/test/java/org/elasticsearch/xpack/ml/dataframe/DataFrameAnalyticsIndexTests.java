/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFrameAnalyticsIndexTests extends ESTestCase {

    private static final String ANALYTICS_ID = "some-analytics-id";
    private static final String[] SOURCE_INDEX = new String[] {"source-index"};
    private static final String DEST_INDEX = "dest-index";
    private static final DataFrameAnalyticsConfig ANALYTICS_CONFIG =
        new DataFrameAnalyticsConfig.Builder()
            .setId(ANALYTICS_ID)
            .setSource(new DataFrameAnalyticsSource(SOURCE_INDEX, null))
            .setDest(new DataFrameAnalyticsDest(DEST_INDEX, null))
            .setAnalysis(new OutlierDetection())
            .build();
    private static final int CURRENT_TIME_MILLIS = 123456789;
    private static final String CREATED_BY = "data-frame-analytics";

    private ThreadPool threadPool = mock(ThreadPool.class);
    private Client client = mock(Client.class);
    private Clock clock = Clock.fixed(Instant.ofEpochMilli(123456789L), ZoneId.systemDefault());

    public void testCreateDestinationIndex() throws IOException {
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        ArgumentCaptor<CreateIndexRequest> createIndexRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        doAnswer(
            invocationOnMock -> {
                @SuppressWarnings("unchecked")
                ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[2];
                listener.onResponse(null);
                return null;
            })
            .when(client).execute(eq(CreateIndexAction.INSTANCE), createIndexRequestCaptor.capture(), any());

        Settings index1Settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        Settings index2Settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        ArgumentCaptor<GetSettingsRequest> getSettingsRequestCaptor = ArgumentCaptor.forClass(GetSettingsRequest.class);
        ArgumentCaptor<GetMappingsRequest> getMappingsRequestCaptor = ArgumentCaptor.forClass(GetMappingsRequest.class);

        ImmutableOpenMap.Builder<String, Settings> indexToSettings = ImmutableOpenMap.builder();
        indexToSettings.put("index_1", index1Settings);
        indexToSettings.put("index_2", index2Settings);

        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(indexToSettings.build(), ImmutableOpenMap.of());

        doAnswer(
            invocationOnMock -> {
                @SuppressWarnings("unchecked")
                ActionListener<GetSettingsResponse> listener = (ActionListener<GetSettingsResponse>) invocationOnMock.getArguments()[2];
                listener.onResponse(getSettingsResponse);
                return null;
            }
        ).when(client).execute(eq(GetSettingsAction.INSTANCE), getSettingsRequestCaptor.capture(), any());

        Map<String, Object> index1Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetaData);
        ImmutableOpenMap.Builder<String, MappingMetaData> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetaData);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        doAnswer(
            invocationOnMock -> {
                @SuppressWarnings("unchecked")
                ActionListener<GetMappingsResponse> listener = (ActionListener<GetMappingsResponse>) invocationOnMock.getArguments()[2];
                listener.onResponse(getMappingsResponse);
                return null;
            }
        ).when(client).execute(eq(GetMappingsAction.INSTANCE), getMappingsRequestCaptor.capture(), any());

        DataFrameAnalyticsIndex.createDestinationIndex(
            client,
            clock,
            ANALYTICS_CONFIG,
            ActionListener.wrap(
                response -> {},
                e -> fail(e.getMessage())));

        GetSettingsRequest capturedGetSettingsRequest = getSettingsRequestCaptor.getValue();
        assertThat(capturedGetSettingsRequest.indices(), equalTo(SOURCE_INDEX));
        assertThat(capturedGetSettingsRequest.indicesOptions(), equalTo(IndicesOptions.lenientExpandOpen()));
        assertThat(Arrays.asList(capturedGetSettingsRequest.names()), contains("index.number_of_shards", "index.number_of_replicas"));

        assertThat(getMappingsRequestCaptor.getValue().indices(), equalTo(SOURCE_INDEX));

        CreateIndexRequest createIndexRequest = createIndexRequestCaptor.getValue();

        assertThat(createIndexRequest.settings().keySet(),
            containsInAnyOrder("index.number_of_shards", "index.number_of_replicas", "index.sort.field", "index.sort.order"));
        assertThat(createIndexRequest.settings().getAsInt("index.number_of_shards", -1), equalTo(5));
        assertThat(createIndexRequest.settings().getAsInt("index.number_of_replicas", -1), equalTo(1));
        assertThat(createIndexRequest.settings().get("index.sort.field"), equalTo("ml__id_copy"));
        assertThat(createIndexRequest.settings().get("index.sort.order"), equalTo("asc"));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, createIndexRequest.mappings().get("_doc"))) {
            Map<String, Object> map = parser.map();
            assertThat(extractValue("_doc.properties.ml__id_copy.type", map), equalTo("keyword"));
            assertThat(extractValue("_doc.properties.field_1", map), equalTo("field_1_mappings"));
            assertThat(extractValue("_doc.properties.field_2", map), equalTo("field_2_mappings"));
            assertThat(extractValue("_doc._meta.analytics", map), equalTo(ANALYTICS_ID));
            assertThat(extractValue("_doc._meta.creation_date_in_millis", map), equalTo(CURRENT_TIME_MILLIS));
            assertThat(extractValue("_doc._meta.created_by", map), equalTo(CREATED_BY));
        }
    }
}
