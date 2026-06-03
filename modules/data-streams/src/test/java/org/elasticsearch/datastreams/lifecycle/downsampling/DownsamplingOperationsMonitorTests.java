/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.downsampling;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.downsample.DownsampleShardTaskParams;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class DownsamplingOperationsMonitorTests extends ESTestCase {

    public void testActivelyDownsampledIndexNames() {
        ProjectId projectId = randomProjectIdOrDefault();

        // no persistent tasks means no indices are being downsampled
        ProjectMetadata emptyProject = ProjectMetadata.builder(projectId).build();
        assertThat(DownsamplingOperationsMonitor.getActivelyDownsampledIndexNames(emptyProject), empty());

        // two shards of index-one and one shard of index-two are being downsampled
        Index indexOne = new Index("index-one", UUIDs.randomBase64UUID());
        Index indexTwo = new Index("index-two", UUIDs.randomBase64UUID());
        PersistentTasksCustomMetadata persistentTasks = PersistentTasksCustomMetadata.builder()
            .addTask(
                "task-1",
                DownsampleShardTaskParams.NAME,
                new DownsampleShardTaskParams(
                    new DownsampleConfig(new DateHistogramInterval("1h"), null),
                    "downsample-1h-index-one",
                    0L,
                    1000L,
                    new ShardId(indexOne, 0),
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    Map.of()
                ),
                new PersistentTasksCustomMetadata.Assignment("node-1", "test")
            )
            .addTask(
                "task-2",
                DownsampleShardTaskParams.NAME,
                new DownsampleShardTaskParams(
                    new DownsampleConfig(new DateHistogramInterval("1h"), null),
                    "downsample-1h-index-one",
                    0L,
                    1000L,
                    new ShardId(indexOne, 1),
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    Map.of()
                ),
                new PersistentTasksCustomMetadata.Assignment("node-1", "test")
            )
            .addTask(
                "task-3",
                DownsampleShardTaskParams.NAME,
                new DownsampleShardTaskParams(
                    new DownsampleConfig(new DateHistogramInterval("1h"), null),
                    "downsample-1h-index-two",
                    0L,
                    1000L,
                    new ShardId(indexTwo, 0),
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    Map.of()
                ),
                new PersistentTasksCustomMetadata.Assignment("node-2", "test")
            )
            .build();

        ProjectMetadata projectWithTasks = ProjectMetadata.builder(projectId)
            .putCustom(PersistentTasksCustomMetadata.TYPE, persistentTasks)
            .build();

        // the two shard tasks for index-one are deduplicated; both index names are returned
        assertThat(
            DownsamplingOperationsMonitor.getActivelyDownsampledIndexNames(projectWithTasks),
            containsInAnyOrder("index-one", "index-two")
        );
    }
}
