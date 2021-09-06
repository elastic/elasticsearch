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
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    protected GetShutdownStatusAction.Response mutateInstance(GetShutdownStatusAction.Response instance) throws IOException {
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
        final TimeValue allocationDelay = type == SingleNodeShutdownMetadata.Type.RESTART && randomBoolean()
            ? TimeValue.parseTimeValue(randomPositiveTimeValue(), GetShutdownStatusResponseTests.class.getSimpleName())
            : null;
        return SingleNodeShutdownMetadata.builder()
            .setNodeId(randomAlphaOfLength(5))
            .setType(type)
            .setReason(randomAlphaOfLength(5))
            .setStartedAtMillis(randomNonNegativeLong())
            .setTargetNodeName(targetNodeName)
            .setAllocationDelay(allocationDelay)
            .build();
    }

    public static SingleNodeShutdownStatus randomNodeShutdownStatus() {
        return new SingleNodeShutdownStatus(
            randomNodeShutdownMetadata(),
            new ShutdownShardMigrationStatus(randomStatus(), randomNonNegativeLong()),
            new ShutdownPersistentTasksStatus(),
            new ShutdownPluginsStatus(randomBoolean())
        );
    }

    public static SingleNodeShutdownMetadata.Status randomStatus() {
        return randomFrom(new ArrayList<>(EnumSet.allOf(SingleNodeShutdownMetadata.Status.class)));
    }
}
