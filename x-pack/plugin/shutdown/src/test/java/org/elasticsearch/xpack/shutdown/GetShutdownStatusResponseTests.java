/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.metadata.NodeShutdownComponentStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.io.stream.Writeable;
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
        List<SingleNodeShutdownMetadata> nodeMetadatas = randomList(0, 20, GetShutdownStatusResponseTests::randomNodeShutdownInfo);
        return new GetShutdownStatusAction.Response(nodeMetadatas);
    }

    @Override
    protected GetShutdownStatusAction.Response mutateInstance(GetShutdownStatusAction.Response instance) throws IOException {
        Set<SingleNodeShutdownMetadata> oldNodes = new HashSet<>(instance.getShutdownStatuses());
        List<SingleNodeShutdownMetadata> newNodes = randomList(
            1,
            20,
            () -> randomValueOtherThanMany(oldNodes::contains, GetShutdownStatusResponseTests::randomNodeShutdownInfo)
        );

        return new GetShutdownStatusAction.Response(newNodes);
    }

    public static SingleNodeShutdownMetadata randomNodeShutdownInfo() {
        return SingleNodeShutdownMetadata.builder()
            .setNodeId(randomAlphaOfLength(5))
            .setType(randomBoolean() ? SingleNodeShutdownMetadata.Type.REMOVE : SingleNodeShutdownMetadata.Type.RESTART)
            .setReason(randomAlphaOfLength(5))
            .setStatus(randomStatus())
            .setStartedAtMillis(randomNonNegativeLong())
            .setShardMigrationStatus(randomComponentStatus())
            .setPersistentTasksStatus(randomComponentStatus())
            .setPluginsStatus(randomComponentStatus())
            .build();
    }

    public static SingleNodeShutdownMetadata.Status randomStatus() {
        return randomFrom(new ArrayList<>(EnumSet.allOf(SingleNodeShutdownMetadata.Status.class)));
    }

    public static NodeShutdownComponentStatus randomComponentStatus() {
        return new NodeShutdownComponentStatus(
            randomStatus(),
            randomBoolean() ? null : randomNonNegativeLong(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(4, 10)
        );
    }
}
