/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFactoryRetention;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionResolver;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.empty;

public class UpdateDataStreamGlobalRetentionServiceTests extends ESTestCase {
    private static TestThreadPool threadPool;
    private ClusterService clusterService;
    private UpdateDataStreamGlobalRetentionService service;

    @BeforeClass
    public static void setupThreadPool() {
        threadPool = new TestThreadPool(getTestClass().getName());
    }

    @Before
    public void setupServices() {
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        service = new UpdateDataStreamGlobalRetentionService(
            clusterService,
            new DataStreamGlobalRetentionResolver(DataStreamFactoryRetention.emptyFactoryRetention())
        );
    }

    @After
    public void closeClusterService() {
        clusterService.close();
    }

    @AfterClass
    public static void tearDownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testUpdateClusterState() {
        // Removing from a cluster state without global retention
        {
            assertThat(service.updateGlobalRetention(ClusterState.EMPTY_STATE, null), equalTo(ClusterState.EMPTY_STATE));
            assertThat(
                service.updateGlobalRetention(ClusterState.EMPTY_STATE, DataStreamGlobalRetention.EMPTY),
                equalTo(ClusterState.EMPTY_STATE)
            );
        }

        // Removing from a cluster state with global retention
        {
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .putCustom(DataStreamGlobalRetention.TYPE, randomNonEmptyGlobalRetention())
                .build();
            DataStreamGlobalRetention updatedRetention = DataStreamGlobalRetention.getFromClusterState(
                service.updateGlobalRetention(clusterState, null)
            );
            assertThat(updatedRetention, nullValue());
            updatedRetention = DataStreamGlobalRetention.getFromClusterState(
                service.updateGlobalRetention(clusterState, DataStreamGlobalRetention.EMPTY)
            );
            assertThat(updatedRetention, nullValue());
        }

        // Updating retention
        {
            var initialRetention = randomNonEmptyGlobalRetention();
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .putCustom(DataStreamGlobalRetention.TYPE, initialRetention)
                .build();
            var expectedRetention = randomValueOtherThan(
                initialRetention,
                UpdateDataStreamGlobalRetentionServiceTests::randomNonEmptyGlobalRetention
            );
            var updatedRetention = DataStreamGlobalRetention.getFromClusterState(
                service.updateGlobalRetention(clusterState, expectedRetention)
            );
            assertThat(updatedRetention, equalTo(expectedRetention));
        }

        // No change means no new cluster state
        {
            UpdateDataStreamGlobalRetentionService serviceWithRandomFactoryRetention = new UpdateDataStreamGlobalRetentionService(
                clusterService,
                new DataStreamGlobalRetentionResolver(
                    randomBoolean() ? DataStreamFactoryRetention.emptyFactoryRetention() : randomNonEmptyFactoryRetention()
                )
            );
            var retention = randomBoolean() ? null : randomNonEmptyGlobalRetention();
            ClusterState clusterState = retention == null
                ? ClusterState.EMPTY_STATE
                : ClusterState.builder(ClusterName.DEFAULT).putCustom(DataStreamGlobalRetention.TYPE, retention).build();
            var updatedClusterState = serviceWithRandomFactoryRetention.updateGlobalRetention(clusterState, retention);
            assertThat(updatedClusterState == clusterState, is(true));
        }
    }

    public void testDetermineAffectedDataStreams() {
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStreamWithoutLifecycle = newDataStreamInstance(
            "ds-no-lifecycle",
            List.of(new Index(randomAlphaOfLength(10), randomAlphaOfLength(10))),
            1,
            null,
            false,
            null,
            List.of()
        );
        builder.put(dataStreamWithoutLifecycle);
        String dataStreamNoRetention = "ds-no-retention";
        DataStream dataStreamWithLifecycleNoRetention = newDataStreamInstance(
            dataStreamNoRetention,
            List.of(new Index(randomAlphaOfLength(10), randomAlphaOfLength(10))),
            1,
            null,
            false,
            DataStreamLifecycle.DEFAULT,
            List.of()
        );

        builder.put(dataStreamWithLifecycleNoRetention);
        DataStream dataStreamWithLifecycleShortRetention = newDataStreamInstance(
            "ds-no-short-retention",
            List.of(new Index(randomAlphaOfLength(10), randomAlphaOfLength(10))),
            1,
            null,
            false,
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueDays(7)).build(),
            List.of()
        );
        builder.put(dataStreamWithLifecycleShortRetention);
        String dataStreamLongRetention = "ds-long-retention";
        DataStream dataStreamWithLifecycleLongRetention = newDataStreamInstance(
            dataStreamLongRetention,
            List.of(new Index(randomAlphaOfLength(10), randomAlphaOfLength(10))),
            1,
            null,
            false,
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueDays(365)).build(),
            List.of()
        );
        builder.put(dataStreamWithLifecycleLongRetention);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        // No global retention
        {
            var affectedDataStreams = service.determineAffectedDataStreams(null, clusterState);
            assertThat(affectedDataStreams.isEmpty(), is(true));
        }
        // No difference in global retention
        {
            var globalRetention = randomNonEmptyGlobalRetention();
            var clusterStateWithRetention = ClusterState.builder(clusterState)
                .putCustom(DataStreamGlobalRetention.TYPE, globalRetention)
                .build();
            var affectedDataStreams = service.determineAffectedDataStreams(globalRetention, clusterStateWithRetention);
            assertThat(affectedDataStreams.isEmpty(), is(true));
        }
        // Default retention in effect
        {
            var globalRetention = new DataStreamGlobalRetention(TimeValue.timeValueDays(randomIntBetween(1, 10)), null);
            var affectedDataStreams = service.determineAffectedDataStreams(globalRetention, clusterState);
            if (dataStreamWithLifecycleNoRetention.isSystem()) {
                assertThat(affectedDataStreams.size(), is(0));
            } else {
                assertThat(affectedDataStreams.size(), is(1));
                var dataStream = affectedDataStreams.get(0);
                assertThat(dataStream.dataStreamName(), equalTo(dataStreamNoRetention));
                assertThat(dataStream.previousEffectiveRetention(), nullValue());
                assertThat(dataStream.newEffectiveRetention(), equalTo(globalRetention.getDefaultRetention()));
            }
        }
        // Max retention in effect
        {
            var globalRetention = new DataStreamGlobalRetention(null, TimeValue.timeValueDays(randomIntBetween(10, 90)));
            var affectedDataStreams = service.determineAffectedDataStreams(globalRetention, clusterState);
            if (dataStreamWithLifecycleLongRetention.isSystem() && dataStreamWithLifecycleNoRetention.isSystem()) {
                assertThat(affectedDataStreams.size(), is(0));
            } else if (dataStreamWithLifecycleLongRetention.isSystem() == false && dataStreamWithLifecycleNoRetention.isSystem() == false) {
                assertThat(affectedDataStreams.size(), is(2));
                var dataStream = affectedDataStreams.get(0);
                assertThat(dataStream.dataStreamName(), equalTo(dataStreamLongRetention));
                assertThat(dataStream.previousEffectiveRetention(), notNullValue());
                assertThat(dataStream.newEffectiveRetention(), equalTo(globalRetention.getMaxRetention()));
                dataStream = affectedDataStreams.get(1);
                assertThat(dataStream.dataStreamName(), equalTo(dataStreamNoRetention));
                assertThat(dataStream.previousEffectiveRetention(), nullValue());
                assertThat(dataStream.newEffectiveRetention(), equalTo(globalRetention.getMaxRetention()));
            } else if (dataStreamWithLifecycleLongRetention.isSystem() == false) {
                assertThat(affectedDataStreams.size(), is(1));
                var dataStream = affectedDataStreams.get(0);
                assertThat(dataStream.dataStreamName(), equalTo(dataStreamLongRetention));
                assertThat(dataStream.previousEffectiveRetention(), notNullValue());
                assertThat(dataStream.newEffectiveRetention(), equalTo(globalRetention.getMaxRetention()));
            } else {
                assertThat(affectedDataStreams.size(), is(1));
                var dataStream = affectedDataStreams.get(0);
                assertThat(dataStream.dataStreamName(), equalTo(dataStreamNoRetention));
                assertThat(dataStream.previousEffectiveRetention(), nullValue());
                assertThat(dataStream.newEffectiveRetention(), equalTo(globalRetention.getMaxRetention()));
            }
        }

        // Requested global retention match the factory retention, so no affected data streams
        {
            DataStreamFactoryRetention factoryRetention = randomNonEmptyFactoryRetention();
            UpdateDataStreamGlobalRetentionService serviceWithRandomFactoryRetention = new UpdateDataStreamGlobalRetentionService(
                clusterService,
                new DataStreamGlobalRetentionResolver(factoryRetention)
            );
            var globalRetention = new DataStreamGlobalRetention(factoryRetention.getDefaultRetention(), factoryRetention.getMaxRetention());
            var affectedDataStreams = serviceWithRandomFactoryRetention.determineAffectedDataStreams(globalRetention, clusterState);
            assertThat(affectedDataStreams, is(empty()));
        }
    }

    private static DataStream newDataStreamInstance(
        String name,
        List<Index> indices,
        long generation,
        Map<String, Object> metadata,
        boolean replicated,
        @Nullable DataStreamLifecycle lifecycle,
        List<Index> failureStores
    ) {
        DataStream.Builder builder = DataStream.builder(name, indices)
            .setGeneration(generation)
            .setMetadata(metadata)
            .setReplicated(replicated)
            .setLifecycle(lifecycle)
            .setFailureStoreEnabled(failureStores.isEmpty() == false)
            .setFailureIndices(failureStores);
        if (randomBoolean()) {
            builder.setSystem(true);
            builder.setHidden(true);
        }
        return builder.build();
    }

    private static DataStreamGlobalRetention randomNonEmptyGlobalRetention() {
        boolean withDefault = randomBoolean();
        return new DataStreamGlobalRetention(
            withDefault ? TimeValue.timeValueDays(randomIntBetween(1, 1000)) : null,
            withDefault == false || randomBoolean() ? TimeValue.timeValueDays(randomIntBetween(1000, 2000)) : null
        );
    }

    private static DataStreamFactoryRetention randomNonEmptyFactoryRetention() {
        boolean withDefault = randomBoolean();
        TimeValue defaultRetention = withDefault ? TimeValue.timeValueDays(randomIntBetween(10, 20)) : null;
        TimeValue maxRetention = withDefault && randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(50, 200));
        return new DataStreamFactoryRetention() {
            @Override
            public TimeValue getMaxRetention() {
                return maxRetention;
            }

            @Override
            public TimeValue getDefaultRetention() {
                return defaultRetention;
            }

            @Override
            public void init(ClusterSettings clusterSettings) {

            }
        };
    }
}
