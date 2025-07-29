/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.createDataStream;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetDataStreamLifecycleStatsActionTests extends ESTestCase {

    private final DataStreamLifecycleService dataStreamLifecycleService = mock(DataStreamLifecycleService.class);
    private final DataStreamLifecycleErrorStore errorStore = mock(DataStreamLifecycleErrorStore.class);
    private final TransportGetDataStreamLifecycleStatsAction action = new TransportGetDataStreamLifecycleStatsAction(
        mock(TransportService.class),
        mock(ClusterService.class),
        mock(ThreadPool.class),
        mock(ActionFilters.class),
        dataStreamLifecycleService
    );
    private Long lastRunDuration;
    private Long timeBetweenStarts;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        lastRunDuration = randomBoolean() ? randomLongBetween(0, 100000) : null;
        timeBetweenStarts = randomBoolean() ? randomLongBetween(0, 100000) : null;
        when(dataStreamLifecycleService.getLastRunDuration()).thenReturn(lastRunDuration);
        when(dataStreamLifecycleService.getTimeBetweenStarts()).thenReturn(timeBetweenStarts);
        when(dataStreamLifecycleService.getErrorStore()).thenReturn(errorStore);
        when(errorStore.getAllIndices()).thenReturn(Set.of());
    }

    public void testEmptyClusterState() {
        GetDataStreamLifecycleStatsAction.Response response = action.collectStats(ClusterState.EMPTY_STATE);
        assertThat(response.getRunDuration(), is(lastRunDuration));
        assertThat(response.getTimeBetweenStarts(), is(timeBetweenStarts));
        assertThat(response.getDataStreamStats().isEmpty(), is(true));
    }

    public void testMixedDataStreams() {
        Set<String> indicesInError = new HashSet<>();
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream ilmDataStream = createDataStream(
            builder,
            "ilm-managed-index",
            numBackingIndices,
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()),
            null,
            Clock.systemUTC().millis()
        );
        builder.put(ilmDataStream);
        DataStream dslDataStream = createDataStream(
            builder,
            "dsl-managed-index",
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(10)).build(),
            Clock.systemUTC().millis()
        );
        indicesInError.add(dslDataStream.getIndices().get(randomInt(numBackingIndices - 1)).getName());
        builder.put(dslDataStream);
        {
            String dataStreamName = "mixed";
            final List<Index> backingIndices = new ArrayList<>();
            for (int k = 1; k <= 2; k++) {
                IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k))
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .creationDate(Clock.systemUTC().millis());

                IndexMetadata indexMetadata = indexMetaBuilder.build();
                builder.put(indexMetadata, false);
                backingIndices.add(indexMetadata.getIndex());
            }
            // DSL managed write index
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 3))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(Clock.systemUTC().millis());
            MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(Clock.systemUTC().millis() - 2000L));
            indexMetaBuilder.putRolloverInfo(
                new RolloverInfo(dataStreamName, List.of(rolloverCondition), Clock.systemUTC().millis() - 2000L)
            );
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            builder.put(indexMetadata, false);
            backingIndices.add(indexMetadata.getIndex());
            builder.put(newInstance(dataStreamName, backingIndices, 3, null, false, DataStreamLifecycle.dataLifecycleBuilder().build()));
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        when(errorStore.getAllIndices()).thenReturn(indicesInError);
        GetDataStreamLifecycleStatsAction.Response response = action.collectStats(state);
        assertThat(response.getRunDuration(), is(lastRunDuration));
        assertThat(response.getTimeBetweenStarts(), is(timeBetweenStarts));
        assertThat(response.getDataStreamStats().size(), is(2));
        for (GetDataStreamLifecycleStatsAction.Response.DataStreamStats stats : response.getDataStreamStats()) {
            if (stats.dataStreamName().equals("dsl-managed-index")) {
                assertThat(stats.backingIndicesInTotal(), is(3));
                assertThat(stats.backingIndicesInError(), is(1));
            }
            if (stats.dataStreamName().equals("mixed")) {
                assertThat(stats.backingIndicesInTotal(), is(1));
                assertThat(stats.backingIndicesInError(), is(0));
            }
        }
    }
}
