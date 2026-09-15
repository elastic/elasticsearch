/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.indices.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState.Stage;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class RecoveryStateTests extends ESTestCase {

    public void testCannotTransitionToCreatedStage() {
        final var state = createRecoveryState();
        state.setStage(Stage.INIT);

        final AssertionError error = expectThrows(AssertionError.class, () -> state.setStage(Stage.CREATED));
        assertThat(error.getMessage(), containsString("can't move recovery to stage [CREATED]"));
        assertThat(state.getStage(), equalTo(Stage.INIT));
    }

    public void testTimerOnlyStartsWhenRecoveryStarts() {
        final var state = createRecoveryState();
        assertThat("a queued recovery has no start time", state.getTimer().startTime(), equalTo(0L));
        assertThat("a queued recovery has not accrued any time", state.getTimer().time(), equalTo(0L));

        state.setStage(Stage.INIT);

        assertThat(state.getStage(), equalTo(Stage.INIT));
        assertThat(state.getTimer().startTime(), greaterThan(0L));
    }

    public void testReinitializingDoesNotRestartTimer() {
        final var state = createRecoveryState();
        state.setStage(Stage.INIT);
        final var startTime = state.getTimer().startTime();

        state.setStage(Stage.INIT);
        assertThat(state.getTimer().startTime(), equalTo(startTime));

        assertThat(state.reset().getTimer().startTime(), equalTo(startTime));
    }

    public void testQueuedRecoveryReportsZeroTimingsInXContent() throws IOException {
        final var state = createRecoveryState();
        assertThat(longField(state, "start_time_in_millis"), equalTo(0L));
        assertThat(longField(state, "total_time_in_millis"), equalTo(0L));

        state.setStage(Stage.INIT);
        assertThat(longField(state, "start_time_in_millis"), greaterThan(0L));
    }

    public void testCreatedStageIsDowngradedToInitForOldNodes() throws IOException {
        final var createdStageVersion = TransportVersion.fromName("recovery_stage_created");
        final var state = createRecoveryState();

        assertThat(
            serializeDeserialize(state, TransportVersionUtils.getPreviousVersion(createdStageVersion)).getStage(),
            equalTo(Stage.INIT)
        );
        assertThat(
            serializeDeserialize(state, TransportVersionUtils.randomVersionSupporting(createdStageVersion)).getStage(),
            equalTo(Stage.CREATED)
        );
    }

    public void testLocalRetryCountOmittedForOldNodes() throws IOException {
        final var localRetryCountVersion = TransportVersion.fromName("recovery_local_retry_count_in_recovery_state");
        int localRetries = randomIntBetween(1, 10);
        final var state = createRecoveryState().setLocalRetries(localRetries);

        assertThat(
            serializeDeserialize(state, TransportVersionUtils.getPreviousVersion(localRetryCountVersion)).getLocalRetries(),
            equalTo(0) // should be omitted when sent to old node, and defaulted to 0 when received from old node
        );
        assertThat(
            serializeDeserialize(state, TransportVersionUtils.randomVersionSupporting(localRetryCountVersion)).getLocalRetries(),
            equalTo(localRetries)
        );
    }

    private static RecoveryState createRecoveryState() {
        final var discoveryNode = DiscoveryNodeUtils.builder(randomUUID()).roles(emptySet()).build();
        final var shardRouting = TestShardRouting.newShardRouting(
            new ShardId(randomIndexName(), randomUUID(), 0),
            discoveryNode.getId(),
            randomBoolean(),
            ShardRoutingState.INITIALIZING
        );
        final var state = new RecoveryState(
            shardRouting,
            discoveryNode,
            shardRouting.recoverySource().getType() == RecoverySource.Type.PEER ? discoveryNode : null
        );
        assertThat(state.getStage(), equalTo(Stage.CREATED));
        return state;
    }

    private RecoveryState serializeDeserialize(RecoveryState state, TransportVersion version) throws IOException {
        return copyWriteable(state, writableRegistry(), RecoveryState::readRecoveryState, version);
    }

    private static long longField(RecoveryState state, String field) throws IOException {
        final var value = toXContentMap(state).get(field);
        assertThat(field + " must be present in the response", value, instanceOf(Number.class));
        return ((Number) value).longValue();
    }

    private static Map<String, Object> toXContentMap(RecoveryState state) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            state.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }
}
