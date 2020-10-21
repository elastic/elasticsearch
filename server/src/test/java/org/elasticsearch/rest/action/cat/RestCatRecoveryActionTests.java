/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestCatRecoveryActionTests extends ESTestCase {

    public void testRestRecoveryAction() {
        final RestCatRecoveryAction action = new RestCatRecoveryAction();
        final int totalShards = randomIntBetween(1, 32);
        final int successfulShards = Math.max(0, totalShards - randomIntBetween(1, 2));
        final int failedShards = totalShards - successfulShards;
        final Map<String, List<RecoveryState>> shardRecoveryStates = new HashMap<>();
        final List<RecoveryState> recoveryStates = new ArrayList<>();

        for (int i = 0; i < successfulShards; i++) {
            final RecoveryState state = mock(RecoveryState.class);
            when(state.getShardId()).thenReturn(new ShardId(new Index("index", "_na_"), i));
            final RecoveryState.Timer timer = mock(RecoveryState.Timer.class);
            final long startTime = randomLongBetween(0, new Date().getTime());
            when(timer.startTime()).thenReturn(startTime);
            final long time = randomLongBetween(1000000, 10 * 1000000);
            when(timer.time()).thenReturn(time);
            when(timer.stopTime()).thenReturn(startTime + time);
            when(state.getTimer()).thenReturn(timer);
            when(state.getRecoverySource()).thenReturn(TestShardRouting.randomRecoverySource());
            when(state.getStage()).thenReturn(randomFrom(RecoveryState.Stage.values()));
            final DiscoveryNode sourceNode = randomBoolean() ? mock(DiscoveryNode.class) : null;
            if (sourceNode != null) {
                when(sourceNode.getHostName()).thenReturn(randomAlphaOfLength(8));
            }
            when(state.getSourceNode()).thenReturn(sourceNode);
            final DiscoveryNode targetNode = mock(DiscoveryNode.class);
            when(targetNode.getHostName()).thenReturn(randomAlphaOfLength(8));
            when(state.getTargetNode()).thenReturn(targetNode);

            RecoveryState.Index index = mock(RecoveryState.Index.class);

            final int totalRecoveredFiles = randomIntBetween(1, 64);
            when(index.totalRecoverFiles()).thenReturn(totalRecoveredFiles);
            final int recoveredFileCount = randomIntBetween(0, totalRecoveredFiles);
            when(index.recoveredFileCount()).thenReturn(recoveredFileCount);
            when(index.recoveredFilesPercent()).thenReturn((100f * recoveredFileCount) / totalRecoveredFiles);
            when(index.totalFileCount()).thenReturn(randomIntBetween(totalRecoveredFiles, 2 * totalRecoveredFiles));

            final int totalRecoveredBytes = randomIntBetween(1, 1 << 24);
            when(index.totalRecoverBytes()).thenReturn((long)totalRecoveredBytes);
            final int recoveredBytes = randomIntBetween(0, totalRecoveredBytes);
            when(index.recoveredBytes()).thenReturn((long)recoveredBytes);
            when(index.recoveredBytesPercent()).thenReturn((100f * recoveredBytes) / totalRecoveredBytes);
            when(index.totalRecoverBytes()).thenReturn((long)randomIntBetween(totalRecoveredBytes, 2 * totalRecoveredBytes));
            when(state.getIndex()).thenReturn(index);

            final RecoveryState.Translog translog = mock(RecoveryState.Translog.class);
            final int translogOps = randomIntBetween(0, 1 << 18);
            when(translog.totalOperations()).thenReturn(translogOps);
            final int translogOpsRecovered = randomIntBetween(0, translogOps);
            when(translog.recoveredOperations()).thenReturn(translogOpsRecovered);
            when(translog.recoveredPercent()).thenReturn(translogOps == 0 ? 100f : (100f * translogOpsRecovered / translogOps));
            when(state.getTranslog()).thenReturn(translog);

            recoveryStates.add(state);
        }

        final List<RecoveryState> shuffle = new ArrayList<>(recoveryStates);
        Randomness.shuffle(shuffle);
        shardRecoveryStates.put("index", shuffle);

        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        final RecoveryResponse response = new RecoveryResponse(
                totalShards,
                successfulShards,
                failedShards,
                shardRecoveryStates,
                shardFailures);
        final Table table = action.buildRecoveryTable(null, response);

        assertNotNull(table);

        List<Table.Cell> headers = table.getHeaders();

        final List<String> expectedHeaders = Arrays.asList(
                "index",
                "shard",
                "start_time",
                "start_time_millis",
                "stop_time",
                "stop_time_millis",
                "time",
                "type",
                "stage",
                "source_host",
                "source_node",
                "target_host",
                "target_node",
                "repository",
                "snapshot",
                "files",
                "files_recovered",
                "files_percent",
                "files_total",
                "bytes",
                "bytes_recovered",
                "bytes_percent",
                "bytes_total",
                "translog_ops",
                "translog_ops_recovered",
                "translog_ops_percent");

        for (int i = 0; i < expectedHeaders.size(); i++) {
            assertThat(headers.get(i).value, equalTo(expectedHeaders.get(i)));
        }

        assertThat(table.getRows().size(), equalTo(successfulShards));

        for (int i = 0; i < successfulShards; i++) {
            final RecoveryState state = recoveryStates.get(i);
            final List<Object> expectedValues = Arrays.asList(
                    "index",
                    i,
                    XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(state.getTimer().startTime()),
                    state.getTimer().startTime(),
                    XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(state.getTimer().stopTime()),
                    state.getTimer().stopTime(),
                    new TimeValue(state.getTimer().time()),
                    state.getRecoverySource().getType().name().toLowerCase(Locale.ROOT),
                    state.getStage().name().toLowerCase(Locale.ROOT),
                    state.getSourceNode() == null ? "n/a" : state.getSourceNode().getHostName(),
                    state.getSourceNode() == null ? "n/a" : state.getSourceNode().getName(),
                    state.getTargetNode().getHostName(),
                    state.getTargetNode().getName(),
                    state.getRecoverySource() == null || state.getRecoverySource().getType() != RecoverySource.Type.SNAPSHOT ?
                            "n/a" :
                            ((SnapshotRecoverySource) state.getRecoverySource()).snapshot().getRepository(),
                    state.getRecoverySource() == null || state.getRecoverySource().getType() != RecoverySource.Type.SNAPSHOT ?
                            "n/a" :
                            ((SnapshotRecoverySource) state.getRecoverySource()).snapshot().getSnapshotId().getName(),
                    state.getIndex().totalRecoverFiles(),
                    state.getIndex().recoveredFileCount(),
                    percent(state.getIndex().recoveredFilesPercent()),
                    state.getIndex().totalFileCount(),
                    new ByteSizeValue(state.getIndex().totalRecoverBytes()),
                    new ByteSizeValue(state.getIndex().recoveredBytes()),
                    percent(state.getIndex().recoveredBytesPercent()),
                    new ByteSizeValue(state.getIndex().totalBytes()),
                    state.getTranslog().totalOperations(),
                    state.getTranslog().recoveredOperations(),
                    percent(state.getTranslog().recoveredPercent()));

            final List<Table.Cell> cells = table.getRows().get(i);
            for (int j = 0; j < expectedValues.size(); j++) {
                assertThat(cells.get(j).value, equalTo(expectedValues.get(j)));
            }
        }
    }

    private static String percent(float percent) {
        return String.format(Locale.ROOT, "%1.1f%%", percent);
    }

}
