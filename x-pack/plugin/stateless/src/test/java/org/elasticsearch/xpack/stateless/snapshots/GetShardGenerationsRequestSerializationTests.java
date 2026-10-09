/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetShardGenerationsRequestSerializationTests extends AbstractWireSerializingTestCase<GetShardGenerationsRequest> {

    static ShardId randomShardId() {
        return new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 10));
    }

    @Override
    protected Writeable.Reader<GetShardGenerationsRequest> instanceReader() {
        return GetShardGenerationsRequest::new;
    }

    @Override
    protected GetShardGenerationsRequest createTestInstance() {
        final List<ShardId> shardIds = new ArrayList<>();
        for (int i = between(1, 5); i > 0; i--) {
            shardIds.add(randomShardId());
        }
        return new GetShardGenerationsRequest(
            TimeValue.timeValueSeconds(between(1, 60)),
            new ProjectRepo(ProjectId.DEFAULT, randomIdentifier()),
            randomLongBetween(-1, 100),
            shardIds
        );
    }

    @Override
    protected GetShardGenerationsRequest mutateInstance(GetShardGenerationsRequest instance) throws IOException {
        final var shardIds = new ArrayList<>(instance.getShardIds());
        return switch (between(0, 2)) {
            case 0 -> new GetShardGenerationsRequest(
                instance.masterNodeTimeout(),
                new ProjectRepo(instance.getProjectRepo().projectId(), instance.getProjectRepo().name() + "x"),
                instance.getRepositoryGeneration(),
                shardIds
            );
            case 1 -> new GetShardGenerationsRequest(
                instance.masterNodeTimeout(),
                instance.getProjectRepo(),
                instance.getRepositoryGeneration() + 1,
                shardIds
            );
            default -> {
                shardIds.add(randomValueOtherThanMany(shardIds::contains, GetShardGenerationsRequestSerializationTests::randomShardId));
                yield new GetShardGenerationsRequest(
                    instance.masterNodeTimeout(),
                    instance.getProjectRepo(),
                    instance.getRepositoryGeneration(),
                    shardIds
                );
            }
        };
    }
}
