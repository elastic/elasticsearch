/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.stateless.snapshots.GetShardGenerationsRequestSerializationTests.randomShardId;

public class GetShardGenerationsResponseSerializationTests extends AbstractWireSerializingTestCase<GetShardGenerationsResponse> {

    private static RepositoryShardGeneration randomShardGeneration() {
        return new RepositoryShardGeneration(new IndexId(randomIdentifier(), randomUUID()), new ShardGeneration(randomIdentifier()));
    }

    @Override
    protected Writeable.Reader<GetShardGenerationsResponse> instanceReader() {
        return GetShardGenerationsResponse::new;
    }

    @Override
    protected GetShardGenerationsResponse createTestInstance() {
        final Map<ShardId, RepositoryShardGeneration> generations = new HashMap<>();
        for (int i = between(0, 5); i > 0; i--) {
            // some shards have no generation
            generations.put(randomShardId(), randomBoolean() ? randomShardGeneration() : null);
        }
        return new GetShardGenerationsResponse(randomLongBetween(-1, 100), generations);
    }

    @Override
    protected GetShardGenerationsResponse mutateInstance(GetShardGenerationsResponse instance) throws IOException {
        final Map<ShardId, RepositoryShardGeneration> generations = new HashMap<>(instance.getShardGenerations());
        if (randomBoolean()) {
            return new GetShardGenerationsResponse(instance.getRepositoryGeneration() + 1, generations);
        }
        generations.put(randomValueOtherThanMany(generations::containsKey, () -> randomShardId()), randomShardGeneration());
        return new GetShardGenerationsResponse(instance.getRepositoryGeneration(), generations);
    }
}
