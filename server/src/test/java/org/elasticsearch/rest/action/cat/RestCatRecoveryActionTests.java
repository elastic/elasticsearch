/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class RestCatRecoveryActionTests extends ESTestCase {

    public void testRestRecoveryAction() {
        final RestCatRecoveryAction action = new RestCatRecoveryAction();
        final int totalShards = randomIntBetween(1, 32);
        final int successfulShards = Math.max(0, totalShards - randomIntBetween(1, 2));
        final int failedShards = totalShards - successfulShards;
        final Map<String, List<RecoveryState>> shardRecoveryStates = new HashMap<>();
        final List<RecoveryState> recoveryStates = new ArrayList<>();

        for (int i = 0; i < successfulShards; i++) {
            final RecoverySource recoverySource = TestShardRouting.buildRecoverySource();
            final boolean needsSourceNode = recoverySource.getType() == RecoverySource.Type.PEER
                || recoverySource.getType() == RecoverySource.Type.RESHARD_SPLIT;
            final DiscoveryNode sourceNode = (needsSourceNode || randomBoolean())
                ? DiscoveryNodeUtils.builder(randomIdentifier())
                    .name(randomAlphaOfLength(8))
                    .address(randomAlphaOfLength(8), randomAlphaOfLength(8), buildNewFakeTransportAddress())
                    .build()
                : null;
            final DiscoveryNode targetNode = DiscoveryNodeUtils.builder(randomIdentifier())
                .name(randomAlphaOfLength(8))
                .address(randomAlphaOfLength(8), randomAlphaOfLength(8), buildNewFakeTransportAddress())
                .build();
            final boolean primary = recoverySource.getType() == RecoverySource.Type.PEER ? randomBoolean() : true;

            final ShardRouting shardRouting = TestShardRouting.newShardRouting(
                new ShardId(new Index("index", "_na_"), i),
                targetNode.getId(),
                primary,
                ShardRoutingState.INITIALIZING,
                recoverySource
            );
            final RecoveryState state = new RecoveryState(shardRouting, targetNode, sourceNode);
            state.setLocalRetries(randomIntBetween(0, 10));

            // Walk the state machine to a randomly chosen target stage. Declaration order matches traversal order.
            final RecoveryState.Stage targetStage = randomFrom(RecoveryState.Stage.values());
            if (targetStage.ordinal() >= RecoveryState.Stage.INIT.ordinal()) {
                state.setStage(RecoveryState.Stage.INIT);
            }
            if (targetStage.ordinal() >= RecoveryState.Stage.INDEX.ordinal()) {
                state.setStage(RecoveryState.Stage.INDEX);
                final int reusedFiles = randomIntBetween(0, 5);
                for (int f = 0; f < reusedFiles; f++) {
                    state.getIndex().addFileDetail("reused-" + f, randomLongBetween(1, 1 << 20), true);
                }
                final int nonReusedFiles = randomIntBetween(1, 10);
                for (int f = 0; f < nonReusedFiles; f++) {
                    final long length = randomLongBetween(1, 1 << 20);
                    state.getIndex().addFileDetail("file-" + f, length, false);
                    state.getIndex().addRecoveredBytesToFile("file-" + f, randomLongBetween(0, length));
                }
                state.getIndex().setFileDetailsComplete();
            }
            if (targetStage.ordinal() >= RecoveryState.Stage.VERIFY_INDEX.ordinal()) {
                state.setStage(RecoveryState.Stage.VERIFY_INDEX);
            }
            if (targetStage.ordinal() >= RecoveryState.Stage.TRANSLOG.ordinal()) {
                state.setStage(RecoveryState.Stage.TRANSLOG);
                final int translogOps = randomIntBetween(0, 1 << 18);
                state.getTranslog().totalOperations(translogOps);
                state.getTranslog().incrementRecoveredOperations(randomIntBetween(0, translogOps));
            }
            if (targetStage.ordinal() >= RecoveryState.Stage.FINALIZE.ordinal()) {
                state.setStage(RecoveryState.Stage.FINALIZE);
            }
            if (targetStage.ordinal() >= RecoveryState.Stage.DONE.ordinal()) {
                state.setStage(RecoveryState.Stage.DONE);
            }
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
            shardFailures
        );
        // Stop any timers that are running, so the time() captured when building the table matches that asserted below:
        recoveryStates.forEach(state -> {
            if (state.getTimer().startTime() > 0 && state.getTimer().stopTime() == 0) {
                state.getTimer().stop();
            }
        });
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
            "local_retries",
            "priority",
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
            "translog_ops_percent"
        );

        for (int i = 0; i < expectedHeaders.size(); i++) {
            assertThat(headers.get(i).value, equalTo(expectedHeaders.get(i)));
        }

        assertThat(table.getRows().size(), equalTo(successfulShards));

        for (int i = 0; i < successfulShards; i++) {
            final RecoveryState state = recoveryStates.get(i);
            final List<Object> expectedValues = Arrays.asList(
                "index",
                i,
                XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(state.getTimer().startTime())),
                state.getTimer().startTime(),
                XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(state.getTimer().stopTime())),
                state.getTimer().stopTime(),
                new TimeValue(state.getTimer().time()),
                state.getRecoverySource().getType().name().toLowerCase(Locale.ROOT),
                state.getStage().name().toLowerCase(Locale.ROOT),
                state.getLocalRetries(),
                state.getRecoveryPriority().name().toLowerCase(Locale.ROOT),
                state.getSourceNode() == null ? "n/a" : state.getSourceNode().getHostName(),
                state.getSourceNode() == null ? "n/a" : state.getSourceNode().getName(),
                state.getTargetNode().getHostName(),
                state.getTargetNode().getName(),
                state.getRecoverySource() == null || state.getRecoverySource().getType() != RecoverySource.Type.SNAPSHOT
                    ? "n/a"
                    : ((SnapshotRecoverySource) state.getRecoverySource()).snapshot().getRepository(),
                state.getRecoverySource() == null || state.getRecoverySource().getType() != RecoverySource.Type.SNAPSHOT
                    ? "n/a"
                    : ((SnapshotRecoverySource) state.getRecoverySource()).snapshot().getSnapshotId().getName(),
                state.getIndex().totalRecoverFiles(),
                state.getIndex().recoveredFileCount(),
                percent(state.getIndex().recoveredFilesPercent()),
                state.getIndex().totalFileCount(),
                ByteSizeValue.ofBytes(state.getIndex().totalRecoverBytes()),
                ByteSizeValue.ofBytes(state.getIndex().recoveredBytes()),
                percent(state.getIndex().recoveredBytesPercent()),
                ByteSizeValue.ofBytes(state.getIndex().totalBytes()),
                state.getTranslog().totalOperations(),
                state.getTranslog().recoveredOperations(),
                percent(state.getTranslog().recoveredPercent())
            );

            final List<Table.Cell> cells = table.getRows().get(i);
            for (int j = 0; j < expectedValues.size(); j++) {
                assertThat(cells.get(j).value, equalTo(expectedValues.get(j)));
            }
        }
    }

    private static String percent(float percent) {
        return Strings.format("%1.1f%%", percent);
    }

}
