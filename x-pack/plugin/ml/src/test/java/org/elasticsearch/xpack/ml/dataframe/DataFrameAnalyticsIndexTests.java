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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DataFrameAnalyticsIndexTests extends ESTestCase {

    private static final String CLUSTER_NAME = "some-cluster-name";

    private static final String ANALYTICS_ID = "some-analytics-id";
    private static final String SOURCE_INDEX = "source-index";
    private static final String DEST_INDEX = "dest-index";
    private static final DataFrameAnalyticsConfig ANALYTICS_CONFIG =
        new DataFrameAnalyticsConfig.Builder(ANALYTICS_ID)
            .setSource(new DataFrameAnalyticsSource(SOURCE_INDEX, null))
            .setDest(new DataFrameAnalyticsDest(DEST_INDEX, null))
            .setAnalysis(new OutlierDetection())
            .build();
    private static final int CURRENT_TIME_MILLIS = 123456789;
    private static final String CREATED_BY = "data-frame-analytics";

    private ThreadPool threadPool = mock(ThreadPool.class);
    private Client client = mock(Client.class);
    private Clock clock = ClockMock.fixed(Instant.ofEpochMilli(123456789L), ZoneId.systemDefault());

    public void testCreateDestinationIndex() throws IOException {
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        doAnswer(
            invocationOnMock -> {
                @SuppressWarnings("unchecked")
                ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[2];
                listener.onResponse(null);
                return null;
            })
            .when(client).execute(any(), any(), any());

        ClusterState clusterState =
            ClusterState.builder(new ClusterName(CLUSTER_NAME))
                .metaData(MetaData.builder()
                    .put(IndexMetaData.builder(SOURCE_INDEX)
                        .settings(Settings.builder()
                            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                        .putMapping(new MappingMetaData("_doc", Map.of("properties", Map.of())))))
                .build();
        DataFrameAnalyticsIndex.createDestinationIndex(
            client,
            clock,
            clusterState,
            ANALYTICS_CONFIG,
            ActionListener.wrap(
                response -> {},
                e -> fail(e.getMessage())));

        ArgumentCaptor<CreateIndexRequest> createIndexRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        verify(client, atLeastOnce()).threadPool();
        verify(client).execute(eq(CreateIndexAction.INSTANCE), createIndexRequestCaptor.capture(), any());
        verifyNoMoreInteractions(client);

        CreateIndexRequest createIndexRequest = createIndexRequestCaptor.getValue();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, createIndexRequest.mappings().get("_doc"))) {
            Map<String, Object> map = parser.map();
            assertThat(extractValue("_doc.properties._id_copy.type", map), equalTo("keyword"));
            assertThat(extractValue("_doc._meta.analytics", map), equalTo(ANALYTICS_ID));
            assertThat(extractValue("_doc._meta.creation_date_in_millis", map), equalTo(CURRENT_TIME_MILLIS));
            assertThat(extractValue("_doc._meta.created_by", map), equalTo(CREATED_BY));
        }
    }

    public void testCreateDestinationIndex_IndexNotFound() {
        ClusterState clusterState =
            ClusterState.builder(new ClusterName(CLUSTER_NAME))
                .metaData(MetaData.builder())
                .build();
        DataFrameAnalyticsIndex.createDestinationIndex(
            client,
            clock,
            clusterState,
            ANALYTICS_CONFIG,
            ActionListener.wrap(
                response -> fail("IndexNotFoundException should be thrown"),
                e -> {
                    assertThat(e, instanceOf(IndexNotFoundException.class));
                    IndexNotFoundException infe = (IndexNotFoundException) e;
                    assertThat(infe.getIndex().getName(), equalTo(SOURCE_INDEX));
                }));
    }
}
