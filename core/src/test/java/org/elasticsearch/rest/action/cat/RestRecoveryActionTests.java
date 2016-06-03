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

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestRecoveryActionTests extends ESTestCase {

    public void testRestRecoveryAction() {
        final Settings settings = Settings.EMPTY;
        final RestController restController = new RestController(settings);
        final RestRecoveryAction action = new RestRecoveryAction(settings, restController, restController, null);
        final int totalShards = randomIntBetween(1, 32);
        final int successfulShards = Math.max(0, totalShards - randomIntBetween(1, 2));
        final int failedShards = totalShards - successfulShards;
        final boolean detailed = randomBoolean();
        final Map<String, List<RecoveryState>> shardRecoveryStates = new HashMap<>();
        final List<RecoveryState> recoveryStates = new ArrayList<>();

        for (int i = 0; i < successfulShards; i++) {
            final RecoveryState state = mock(RecoveryState.class);
            when(state.getShardId()).thenReturn(new ShardId(new Index("index", "_na_"), i));
            final RecoveryState.Timer timer = mock(RecoveryState.Timer.class);
            when(timer.time()).thenReturn((long)randomIntBetween(1000000, 10 * 1000000));
            when(state.getTimer()).thenReturn(timer);
            when(state.getType()).thenReturn(randomFrom(RecoveryState.Type.values()));
            when(state.getStage()).thenReturn(randomFrom(RecoveryState.Stage.values()));
            final DiscoveryNode sourceNode = randomBoolean() ? mock(DiscoveryNode.class) : null;
            if (sourceNode != null) {
                when(sourceNode.getHostName()).thenReturn(randomAsciiOfLength(8));
            }
            when(state.getSourceNode()).thenReturn(sourceNode);
            final DiscoveryNode targetNode = mock(DiscoveryNode.class);
            when(targetNode.getHostName()).thenReturn(randomAsciiOfLength(8));
            when(state.getTargetNode()).thenReturn(targetNode);

            final RestoreSource restoreSource = randomBoolean() ? mock(RestoreSource.class) : null;
            if (restoreSource != null) {
                final Snapshot snapshot = new Snapshot(randomAsciiOfLength(8),
                                                       new SnapshotId(randomAsciiOfLength(8), UUIDs.randomBase64UUID()));
                when(restoreSource.snapshot()).thenReturn(snapshot);
            }

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

        final List<ShardOperationFailedException> shardFailures = new ArrayList<>();
        final RecoveryResponse response = new RecoveryResponse(
                totalShards,
                successfulShards,
                failedShards,
                detailed,
                shardRecoveryStates,
                shardFailures);
        final Table table = action.buildRecoveryTable(null, response);

        assertNotNull(table);

        List<Table.Cell> headers = table.getHeaders();

        assertThat(headers.get(0).value, equalTo("index"));
        assertThat(headers.get(1).value, equalTo("shard"));
        assertThat(headers.get(2).value, equalTo("time"));
        assertThat(headers.get(3).value, equalTo("type"));
        assertThat(headers.get(4).value, equalTo("stage"));
        assertThat(headers.get(5).value, equalTo("source_host"));
        assertThat(headers.get(6).value, equalTo("source_node"));
        assertThat(headers.get(7).value, equalTo("target_host"));
        assertThat(headers.get(8).value, equalTo("target_node"));
        assertThat(headers.get(9).value, equalTo("repository"));
        assertThat(headers.get(10).value, equalTo("snapshot"));
        assertThat(headers.get(11).value, equalTo("files"));
        assertThat(headers.get(12).value, equalTo("files_recovered"));
        assertThat(headers.get(13).value, equalTo("files_percent"));
        assertThat(headers.get(14).value, equalTo("files_total"));
        assertThat(headers.get(15).value, equalTo("bytes"));
        assertThat(headers.get(16).value, equalTo("bytes_recovered"));
        assertThat(headers.get(17).value, equalTo("bytes_percent"));
        assertThat(headers.get(18).value, equalTo("bytes_total"));
        assertThat(headers.get(19).value, equalTo("translog_ops"));
        assertThat(headers.get(20).value, equalTo("translog_ops_recovered"));
        assertThat(headers.get(21).value, equalTo("translog_ops_percent"));

        assertThat(table.getRows().size(), equalTo(successfulShards));
        for (int i = 0; i < successfulShards; i++) {
            final RecoveryState state = recoveryStates.get(i);
            List<Table.Cell> cells = table.getRows().get(i);
            assertThat(cells.get(0).value, equalTo("index"));
            assertThat(cells.get(1).value, equalTo(i));
            assertThat(cells.get(2).value, equalTo(new TimeValue(state.getTimer().time())));
            assertThat(cells.get(3).value, equalTo(state.getType().name().toLowerCase(Locale.ROOT)));
            assertThat(cells.get(4).value, equalTo(state.getStage().name().toLowerCase(Locale.ROOT)));
            assertThat(cells.get(5).value, equalTo(state.getSourceNode() == null ? "n/a" : state.getSourceNode().getHostName()));
            assertThat(cells.get(6).value, equalTo(state.getSourceNode() == null ? "n/a" : state.getSourceNode().getName()));
            assertThat(cells.get(7).value, equalTo(state.getTargetNode().getHostName()));
            assertThat(cells.get(8).value, equalTo(state.getTargetNode().getName()));
            assertThat(
                    cells.get(9).value,
                    equalTo(state.getRestoreSource() == null ? "n/a" : state.getRestoreSource().snapshot().getRepository()));
            assertThat(
                    cells.get(10).value,
                    equalTo(state.getRestoreSource() == null ? "n/a" : state.getRestoreSource().snapshot().getSnapshotId().getName()));
            assertThat(cells.get(11).value, equalTo(state.getIndex().totalRecoverFiles()));
            assertThat(cells.get(12).value, equalTo(state.getIndex().recoveredFileCount()));
            assertThat(cells.get(13).value, equalTo(percent(state.getIndex().recoveredFilesPercent())));
            assertThat(cells.get(14).value, equalTo(state.getIndex().totalFileCount()));
            assertThat(cells.get(15).value, equalTo(state.getIndex().totalRecoverBytes()));
            assertThat(cells.get(16).value, equalTo(state.getIndex().recoveredBytes()));
            assertThat(cells.get(17).value, equalTo(percent(state.getIndex().recoveredBytesPercent())));
            assertThat(cells.get(18).value, equalTo(state.getIndex().totalBytes()));
            assertThat(cells.get(19).value, equalTo(state.getTranslog().totalOperations()));
            assertThat(cells.get(20).value, equalTo(state.getTranslog().recoveredOperations()));
            assertThat(cells.get(21).value, equalTo(percent(state.getTranslog().recoveredPercent())));
        }
    }

    private static String percent(float percent) {
        return String.format(Locale.ROOT, "%1.1f%%", percent);
    }

}
