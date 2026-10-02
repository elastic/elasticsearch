/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class ClusterInfoWriteLoadForecasterTests extends ESTestCase {
    public void testClusterInfoProcessing() {
        var state = ClusterStateCreationUtils.state(1, new String[] { "index1", "index2", "index3", "index4" }, 1);
        var indexIterator = state.metadata().indicesAllProjects().iterator();

        IndexMetadata indexMetadata1 = indexIterator.next();
        IndexMetadata indexMetadata2 = indexIterator.next();
        IndexMetadata indexMetadata3 = indexIterator.next();
        IndexMetadata indexMetadata4 = indexIterator.next();

        Index index1 = indexMetadata1.getIndex();
        Index index2 = indexMetadata2.getIndex();
        Index index3 = indexMetadata3.getIndex();
        Index index4 = indexMetadata4.getIndex();

        double index1WriteLoad = 0.0;
        // test max calculation for representing one index, as index2WriteLoad2 is bigger
        double index2WriteLoad1 = randomDoubleBetween(0.0, 20.0, true);
        double index2WriteLoad2 = randomDoubleBetween(index2WriteLoad1, index2WriteLoad1 + 20.0, true);
        double index3WriteLoad = randomDouble();

        final var clusterInfo = ClusterInfo.builder()
            .shardWriteLoads(
                Map.of(
                    new ShardId(index1, 1),
                    index1WriteLoad,
                    new ShardId(index2, 1),
                    index2WriteLoad1,
                    new ShardId(index2, 2),
                    index2WriteLoad2,
                    new ShardId(index3, 1),
                    index3WriteLoad
                )
            )
            .build();

        ClusterInfoWriteLoadForecaster writeLoadForecaster = new ClusterInfoWriteLoadForecaster(() -> true);
        writeLoadForecaster.onNewClusterInfo(clusterInfo);

        // there is no value before the license refresh
        assertEquals(writeLoadForecaster.getForecastedWriteLoad(indexMetadata1).isPresent(), false);

        writeLoadForecaster.refreshLicense();
        assertEquals(writeLoadForecaster.getForecastedWriteLoad(indexMetadata1).getAsDouble(), index1WriteLoad, 0.00001);
        assertEquals(writeLoadForecaster.getForecastedWriteLoad(indexMetadata2).getAsDouble(), index2WriteLoad2, 0.00001);
        assertEquals(writeLoadForecaster.getForecastedWriteLoad(indexMetadata3).getAsDouble(), index3WriteLoad, 0.00001);
        assertEquals(writeLoadForecaster.getForecastedWriteLoad(indexMetadata4).isPresent(), false);
    }
}
