/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.metadata.ShutdownPersistentTasksStatus;
import org.elasticsearch.cluster.metadata.ShutdownPluginsStatus;
import org.elasticsearch.cluster.metadata.ShutdownShardMigrationStatus;
import org.elasticsearch.cluster.metadata.ShutdownShardSnapshotsStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.RESTART;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.common.xcontent.ChunkedToXContent.wrapAsToXContent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class GetShutdownStatusResponseTests extends AbstractWireSerializingTestCase<GetShutdownStatusAction.Response> {
    @Override
    protected Writeable.Reader<GetShutdownStatusAction.Response> instanceReader() {
        return GetShutdownStatusAction.Response::new;
    }

    @Override
    protected GetShutdownStatusAction.Response createTestInstance() {
        List<SingleNodeShutdownStatus> shutdownStatuses = randomList(0, 20, GetShutdownStatusResponseTests::randomNodeShutdownStatus);
        return new GetShutdownStatusAction.Response(shutdownStatuses);
    }

    @Override
    protected GetShutdownStatusAction.Response mutateInstance(GetShutdownStatusAction.Response instance) {
        Set<SingleNodeShutdownStatus> oldNodes = new HashSet<>(instance.getShutdownStatuses());
        List<SingleNodeShutdownStatus> newNodes = randomList(
            1,
            20,
            () -> randomValueOtherThanMany(oldNodes::contains, GetShutdownStatusResponseTests::randomNodeShutdownStatus)
        );

        return new GetShutdownStatusAction.Response(newNodes);
    }

    public static SingleNodeShutdownMetadata randomNodeShutdownMetadata() {
        final SingleNodeShutdownMetadata.Type type = randomFrom(EnumSet.allOf(SingleNodeShutdownMetadata.Type.class));
        final String targetNodeName = type == SingleNodeShutdownMetadata.Type.REPLACE ? randomAlphaOfLengthBetween(10, 20) : null;
        final TimeValue allocationDelay = type == RESTART && randomBoolean() ? randomPositiveTimeValue() : null;
        final TimeValue gracefulShutdown = type == SIGTERM ? randomPositiveTimeValue() : null;
        return SingleNodeShutdownMetadata.builder()
            .setNodeId(randomAlphaOfLength(5))
            .setNodeEphemeralId(randomAlphaOfLength(5))
            .setType(type)
            .setReason(randomAlphaOfLength(5))
            .setStartedAtMillis(randomNonNegativeLong())
            .setTargetNodeName(targetNodeName)
            .setAllocationDelay(allocationDelay)
            .setGracePeriod(gracefulShutdown)
            .build();
    }

    public static SingleNodeShutdownStatus randomNodeShutdownStatus() {
        final var status = randomStatus();
        final int persistentTasksRemaining = randomIntBetween(0, 10);
        final int autoReassignRemaining = randomIntBetween(0, persistentTasksRemaining);
        final var nodeShutdownMetadata = randomNodeShutdownMetadata();
        return new SingleNodeShutdownStatus(
            nodeShutdownMetadata,
            new ShutdownShardMigrationStatus(status, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            status == SingleNodeShutdownMetadata.Status.NOT_STARTED
                ? ShutdownPersistentTasksStatus.notStarted()
                : ShutdownPersistentTasksStatus.fromRemainingTasks(persistentTasksRemaining, autoReassignRemaining),
            new ShutdownPluginsStatus(randomBoolean()),
            status == SingleNodeShutdownMetadata.Status.NOT_STARTED
                ? ShutdownShardSnapshotsStatus.NOT_STARTED
                : ShutdownShardSnapshotsStatus.fromShardCounts(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            () -> nodeShutdownMetadata.getStartedAtMillis() + 1_000L
        );
    }

    public void testOverallStatusIncludeShardSnapshotStatus() {
        final var metadata = randomNodeShutdownMetadata();
        final var shardMigrationStatus = new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.COMPLETE, 0, 0, 0);
        final var persistentTasksStatus = ShutdownPersistentTasksStatus.fromRemainingTasks(0, 0);
        final var pluginsStatus = new ShutdownPluginsStatus(true);

        assertThat(
            new SingleNodeShutdownStatus(
                metadata,
                shardMigrationStatus,
                persistentTasksStatus,
                pluginsStatus,
                ShutdownShardSnapshotsStatus.fromShardCounts(between(0, 100), between(0, 100), between(1, 100))
            ).overallStatus(),
            equalTo(SingleNodeShutdownMetadata.Status.IN_PROGRESS)
        );

        assertThat(
            new SingleNodeShutdownStatus(
                metadata,
                shardMigrationStatus,
                persistentTasksStatus,
                pluginsStatus,
                ShutdownShardSnapshotsStatus.fromShardCounts(between(0, 100), between(0, 100), 0)
            ).overallStatus(),
            equalTo(SingleNodeShutdownMetadata.Status.COMPLETE)
        );
    }

    public void testRunningTimeEmittedWhileInProgress() throws IOException {
        final var metadata = randomNodeShutdownMetadata();
        final var shardMigrationStatus = new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.IN_PROGRESS, 0, 0, 0);
        final var persistentTasksStatus = ShutdownPersistentTasksStatus.fromRemainingTasks(1, 1);
        final var pluginsStatus = new ShutdownPluginsStatus(false);

        var singleNodeStatus = new SingleNodeShutdownStatus(
            metadata,
            shardMigrationStatus,
            persistentTasksStatus,
            pluginsStatus,
            ShutdownShardSnapshotsStatus.fromShardCounts(between(0, 100), between(0, 100), between(1, 100)),
            () -> metadata.getStartedAtMillis() + 2500L
        );

        assertThat(toJsonMap(singleNodeStatus), hasEntry("shutdown_running_time", "2.5s"));
    }

    public void testRunningTimeOmittedWhenComplete() throws IOException {
        final var metadata = randomNodeShutdownMetadata();
        final var shardMigrationStatus = new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.COMPLETE, 0, 0, 0);
        final var persistentTasksStatus = ShutdownPersistentTasksStatus.fromRemainingTasks(0, 0);
        final var pluginsStatus = new ShutdownPluginsStatus(true);

        var singleNodeStatus = new SingleNodeShutdownStatus(
            metadata,
            shardMigrationStatus,
            persistentTasksStatus,
            pluginsStatus,
            ShutdownShardSnapshotsStatus.fromShardCounts(between(0, 100), between(0, 100), 0),
            () -> metadata.getStartedAtMillis() + 2500L
        );

        assertThat(toJsonMap(singleNodeStatus), not(hasKey("shutdown_running_time")));
    }

    public void testRunningTimeIsClampedWhenClockRegresses() throws IOException {
        final var metadata = randomNodeShutdownMetadata();
        final var shardMigrationStatus = new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.IN_PROGRESS, 0, 0, 0);
        final var persistentTasksStatus = ShutdownPersistentTasksStatus.fromRemainingTasks(1, 1);
        final var pluginsStatus = new ShutdownPluginsStatus(false);

        var singleNodeStatus = new SingleNodeShutdownStatus(
            metadata,
            shardMigrationStatus,
            persistentTasksStatus,
            pluginsStatus,
            ShutdownShardSnapshotsStatus.fromShardCounts(between(0, 100), between(0, 100), between(1, 100)),
            () -> metadata.getStartedAtMillis() - 1L
        );

        assertThat(toJsonMap(singleNodeStatus), hasEntry("shutdown_running_time", "0s"));
    }

    public void testSerializationBwc() throws IOException {
        final var oldVersion = TransportVersionUtils.getPreviousVersion(ShutdownShardSnapshotsStatus.SHUTDOWN_SHARD_SNAPSHOTS_STATUS);
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        final var original = createTestInstance();
        original.writeTo(out);

        final var in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        final var deserialized = new GetShutdownStatusAction.Response(in);
        assertEquals(original.getShutdownStatuses().size(), deserialized.getShutdownStatuses().size());
        for (int i = 0; i < original.getShutdownStatuses().size(); i++) {
            final var originalNodeStatus = original.getShutdownStatuses().get(i);
            final var deserializedNodeStatus = deserialized.getShutdownStatuses().get(i);
            assertThat(deserializedNodeStatus.migrationStatus(), equalTo(originalNodeStatus.migrationStatus()));
            assertThat(deserializedNodeStatus.pluginsStatus(), equalTo(originalNodeStatus.pluginsStatus()));
            assertThat(deserializedNodeStatus.persistentTasksStatus(), equalTo(originalNodeStatus.persistentTasksStatus()));
            assertThat(deserializedNodeStatus.shardSnapshotsStatus(), equalTo(ShutdownShardSnapshotsStatus.fromShardCounts(0, 0, 0)));
        }
    }

    public static SingleNodeShutdownMetadata.Status randomStatus() {
        return randomFrom(new ArrayList<>(EnumSet.allOf(SingleNodeShutdownMetadata.Status.class)));
    }

    private Map<String, Object> toJsonMap(ChunkedToXContent chunkedToXContent) throws IOException {
        var xContent = wrapAsToXContent(chunkedToXContent);
        try (var parser = createParser(xContent.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))) {
            return parser.map();
        }
    }
}
