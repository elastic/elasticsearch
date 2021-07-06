/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.vectors;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.vectors.VectorsFeatureSetUsage;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class VectorsInfoTransportActionTests extends ESTestCase {

    public void testAvailable() throws IOException {
        VectorsInfoTransportAction featureSet = new VectorsInfoTransportAction(mock(TransportService.class), mock(ActionFilters.class));
        assertThat(featureSet.available(), is(true));

        var usageAction = new VectorsUsageTransportAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            null,
            mock(ActionFilters.class),
            null);
        XPackFeatureSet.Usage usage = executeUsageAction(usageAction, null);
        assertThat(usage.available(), is(true));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new VectorsFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(true));
    }

    public void testAlwaysEnabled() throws IOException {
        VectorsInfoTransportAction featureSet = new VectorsInfoTransportAction(mock(TransportService.class), mock(ActionFilters.class));
        assertThat(featureSet.enabled(), is(true));

        VectorsUsageTransportAction usageAction = new VectorsUsageTransportAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            null,
            mock(ActionFilters.class),
            null);
        XPackFeatureSet.Usage usage = executeUsageAction(usageAction, null);
        assertThat(usage.enabled(), is(true));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new VectorsFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.enabled(), is(true));
    }

    private VectorsFeatureSetUsage executeUsageAction(VectorsUsageTransportAction usageAction, ClusterState clusterState) {
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, clusterState, future);
        assert future.isDone();
        return (VectorsFeatureSetUsage) future.actionGet(0, TimeUnit.SECONDS).getUsage();
    }

    public void testCaching() {
        final ClusterService clusterService = mock(ClusterService.class);
        final AtomicReference<ClusterStateListener> clusterStateListenerRef = new AtomicReference<>();
        doAnswer(invocation -> {
            clusterStateListenerRef.set((ClusterStateListener) invocation.getArguments()[0]);
            return null;
        }).when(clusterService).addListener(any());

        final VectorsUsageTransportAction usageAction = new VectorsUsageTransportAction(
            mock(TransportService.class),
            clusterService,
            null,
            mock(ActionFilters.class),
            null);

        assertThat(clusterStateListenerRef, not(nullValue()));

        final ClusterState emptyClusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        final VectorsFeatureSetUsage emptyUsage = executeUsageAction(usageAction, emptyClusterState);
        assertTrue(emptyUsage.available());
        assertTrue(emptyUsage.enabled());
        assertThat(emptyUsage.numDenseVectorFields(), equalTo(0));
        assertThat(emptyUsage.avgDenseVectorDims(), equalTo(0));
        assertThat(usageAction.cacheSize(), equalTo(0));

        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .build();

        final IndexMetadata indexMetadata1 = IndexMetadata.builder(indexMetadata)
            .putMapping("{\"properties\":{\"my_dense_vector1\":{\"type\":\"dense_vector\",\"dims\":1}}}")
            .mappingVersion(1)
            .build();

        final IndexMetadata indexMetadata2 = IndexMetadata.builder(indexMetadata)
            .putMapping("{\"properties\":{\"my_dense_vector1\":{\"type\":\"dense_vector\",\"dims\":2}}}")
            .mappingVersion(2)
            .build();

        final IndexMetadata indexMetadata2Cached = spy(indexMetadata2);
        when(indexMetadata2Cached.mapping()).thenThrow(new AssertionError("should not get mapping"));

        final IndexMetadata indexMetadata3 = IndexMetadata.builder(indexMetadata)
            .putMapping("{\"properties\":{\"my_dense_vector1\":{\"type\":\"dense_vector\",\"dims\":3}}}")
            .mappingVersion(3)
            .build();

        clusterStateListenerRef.get().clusterChanged(new ClusterChangedEvent("test", emptyClusterState, emptyClusterState));

        final VectorsFeatureSetUsage usage2 = executeUsageAction(usageAction, ClusterState.builder(emptyClusterState)
            .metadata(Metadata.builder(emptyClusterState.metadata()).put(indexMetadata2, true).build()).build());
        assertTrue(usage2.available());
        assertTrue(usage2.enabled());
        assertThat(usage2.numDenseVectorFields(), equalTo(1));
        assertThat(usage2.avgDenseVectorDims(), equalTo(2));
        assertThat(usageAction.cacheSize(), equalTo(1));

        final VectorsFeatureSetUsage usage2Cached = executeUsageAction(usageAction, ClusterState.builder(emptyClusterState)
            .metadata(Metadata.builder(emptyClusterState.metadata()).put(indexMetadata2Cached, false).build()).build());
        assertTrue(usage2Cached.available());
        assertTrue(usage2Cached.enabled());
        assertThat(usage2Cached.numDenseVectorFields(), equalTo(1));
        assertThat(usage2Cached.avgDenseVectorDims(), equalTo(2));
        assertThat(usageAction.cacheSize(), equalTo(1));

        final VectorsFeatureSetUsage usage1 = executeUsageAction(usageAction, ClusterState.builder(emptyClusterState)
            .metadata(Metadata.builder(emptyClusterState.metadata()).put(indexMetadata1, true).build()).build());
        assertTrue(usage1.available());
        assertTrue(usage1.enabled());
        assertThat(usage1.numDenseVectorFields(), equalTo(1));
        assertThat(usage1.avgDenseVectorDims(), equalTo(1));
        assertThat(usageAction.cacheSize(), equalTo(1));

        final VectorsFeatureSetUsage usage3 = executeUsageAction(usageAction, ClusterState.builder(emptyClusterState)
            .metadata(Metadata.builder(emptyClusterState.metadata()).put(indexMetadata3, true).build()).build());
        assertTrue(usage3.available());
        assertTrue(usage3.enabled());
        assertThat(usage3.numDenseVectorFields(), equalTo(1));
        assertThat(usage3.avgDenseVectorDims(), equalTo(3));
        assertThat(usageAction.cacheSize(), equalTo(1));

        executeUsageAction(usageAction, emptyClusterState);
        assertThat(usageAction.cacheSize(), equalTo(1)); // stale entries not cleared on execution

        clusterStateListenerRef.get().clusterChanged(new ClusterChangedEvent("test", emptyClusterState, emptyClusterState));
        assertThat(usageAction.cacheSize(), equalTo(1)); // stale entries not cleared if metadata version unchanged

        clusterStateListenerRef.get().clusterChanged(new ClusterChangedEvent(
            "test",
            ClusterState.builder(emptyClusterState).metadata(Metadata.builder(emptyClusterState.metadata()).version(1)).build(),
            emptyClusterState));
        assertThat(usageAction.cacheSize(), equalTo(0));
    }

}
