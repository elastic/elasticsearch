/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.writeloadforecaster.LicensedWriteLoadForecaster.forecastIndexWriteLoad;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notANumber;

public class ClusterInfoWriteLoadForecasterTests extends ESTestCase {
    public void testClusterInfoProcessing() {
        var state = ClusterStateCreationUtils.state(1, new String[] {"index1", "index2", "index3", "index4"}, 1);
        var indexIterator = state.metadata().indicesAllProjects().iterator();

        IndexMetadata indexMetadata1 = indexIterator.next();
        IndexMetadata indexMetadata2 = indexIterator.next();
        IndexMetadata indexMetadata3 = indexIterator.next();
        IndexMetadata indexMetadata4 = indexIterator.next();

        Index index1 = indexMetadata1.getIndex();
        Index index2 = indexMetadata2.getIndex();
        Index index3 = indexMetadata3.getIndex();
        Index index4 = indexMetadata4.getIndex();

        final var clusterInfo = ClusterInfo.builder()
            .shardWriteLoads(Map.of(
                new ShardId(index1, 1), 0.0,
                new ShardId(index2, 1), 0.2,
                new ShardId(index2, 2), 0.5,
                new ShardId(index3, 1), 0.6
            ))
            .build();

        ClusterInfoWriteLoadForecaster writeLoadForecaster = new ClusterInfoWriteLoadForecaster();
        AtomicReference<Consumer<ClusterInfo>> onNewClusterInfoCallback = new AtomicReference<>();
        writeLoadForecaster.setClusterInfoService(new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                return clusterInfo;
            }

            @Override
            public void addListener(Consumer<ClusterInfo> clusterInfoConsumer) {
                onNewClusterInfoCallback.set(clusterInfoConsumer);
            }
        });

        assert onNewClusterInfoCallback.get() != null;

        onNewClusterInfoCallback.get().accept(clusterInfo);

        assertEquals(writeLoadForecaster.getForecastedWriteLoad(indexMetadata1).getAsDouble(), 0.0, 0.00001);
        assertEquals(writeLoadForecaster.getForecastedWriteLoad(indexMetadata2).getAsDouble(), 0.5, 0.00001);
        assertEquals(writeLoadForecaster.getForecastedWriteLoad(indexMetadata3).getAsDouble(), 0.6, 0.00001);
        assertEquals(writeLoadForecaster.getForecastedWriteLoad(indexMetadata4).isPresent(), false);
    }
}
