/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.downsample.DownsampleShardIndexerStatus;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;

import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class DownsamplingOperationsMonitorTests extends ESTestCase {

    private final DownsamplingOperationsMonitor monitor = new DownsamplingOperationsMonitor();

    public void testActivelyDownsampledIndexNames() {
        ProjectId projectId = randomProjectIdOrDefault();

        // no persistent tasks means no indices are being downsampled
        ProjectMetadata emptyProject = ProjectMetadata.builder(projectId).build();
        assertThat(monitor.getActivelyDownsampledIndexNames(emptyProject), empty());

        // two shards of index-one and one shard of index-two are being downsampled
        Index indexOne = new Index("index-one", UUIDs.randomBase64UUID());
        Index indexTwo = new Index("index-two", UUIDs.randomBase64UUID());
        Index indexFailed = new Index("index-failed", UUIDs.randomBase64UUID());
        Index indexSuccess = new Index("index-success", UUIDs.randomBase64UUID());
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
            .addTask(
                "task-failed",
                DownsampleShardTaskParams.NAME,
                new DownsampleShardTaskParams(
                    new DownsampleConfig(new DateHistogramInterval("1h"), null),
                    "downsample-1h-index-one",
                    0L,
                    1000L,
                    new ShardId(indexFailed, 1),
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    Map.of()
                ),
                new PersistentTasksCustomMetadata.Assignment("node-1", "test")
            )
            .updateTaskState("task-failed", new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.FAILED, null))
            .addTask(
                "task-success",
                DownsampleShardTaskParams.NAME,
                new DownsampleShardTaskParams(
                    new DownsampleConfig(new DateHistogramInterval("1h"), null),
                    "downsample-1h-index-one",
                    0L,
                    1000L,
                    new ShardId(indexSuccess, 1),
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    org.elasticsearch.common.Strings.EMPTY_ARRAY,
                    Map.of()
                ),
                new PersistentTasksCustomMetadata.Assignment("node-1", "test")
            )
            .updateTaskState("task-success", new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.COMPLETED, null))
            .build();

        ProjectMetadata projectWithTasks = ProjectMetadata.builder(projectId)
            .putCustom(PersistentTasksCustomMetadata.TYPE, persistentTasks)
            .build();

        // the two shard tasks for index-one are deduplicated; both indices are returned
        assertThat(monitor.getActivelyDownsampledIndexNames(projectWithTasks), containsInAnyOrder(indexOne, indexTwo));
    }
}
