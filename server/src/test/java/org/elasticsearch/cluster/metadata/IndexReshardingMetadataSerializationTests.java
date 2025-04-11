/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class IndexReshardingMetadataSerializationTests extends AbstractXContentSerializingTestCase<IndexReshardingMetadata> {
    @Override
    protected IndexReshardingMetadata doParseInstance(XContentParser parser) throws IOException {
        return IndexReshardingMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<IndexReshardingMetadata> instanceReader() {
        return IndexReshardingMetadata::new;
    }

    @Override
    protected IndexReshardingMetadata createTestInstance() {
        if (randomBoolean()) {
            return new IndexReshardingMetadata(createTestSplit());
        } else {
            return new IndexReshardingMetadata(new IndexReshardingState.Noop());
        }
    }

    private IndexReshardingState.Split createTestSplit() {
        final int oldShards = randomIntBetween(1, 100);
        final int newShards = randomIntBetween(2, 5) * oldShards;
        final var sourceShardStates = new IndexReshardingState.Split.SourceShardState[oldShards];
        final var targetShardStates = new IndexReshardingState.Split.TargetShardState[newShards - oldShards];
        // Semantically it is illegal for SourceShardState to be DONE before all corresponding target shards are
        // DONE but these tests are exercising equals/hashcode not semantic integrity. Letting the source shard
        // state vary randomly gives better coverage in fewer instances even though it is wrong semantically.
        for (int i = 0; i < oldShards; i++) {
            sourceShardStates[i] = randomFrom(IndexReshardingState.Split.SourceShardState.values());
        }
        for (int i = 0; i < targetShardStates.length; i++) {
            targetShardStates[i] = randomFrom(IndexReshardingState.Split.TargetShardState.values());
        }

        return new IndexReshardingState.Split(sourceShardStates, targetShardStates);
    }

    @Override
    protected IndexReshardingMetadata mutateInstance(IndexReshardingMetadata instance) throws IOException {
        var state = switch (instance.getState()) {
            case IndexReshardingState.Noop ignored -> createTestSplit();
            case IndexReshardingState.Split split -> mutateSplit(split);
        };
        return new IndexReshardingMetadata(state);
    }

    // To exercise equals() we want to mutate exactly one thing randomly so we know that each component
    // is contributing to equality testing. Some of these mutations are semantically illegal but we ignore
    // that here.
    private IndexReshardingState.Split mutateSplit(IndexReshardingState.Split split) {
        enum Mutation {
            SOURCE_SHARD_STATES,
            TARGET_SHARD_STATES
        }

        var sourceShardStates = split.sourceShards().clone();
        var targetShardStates = split.targetShards().clone();

        switch (randomFrom(Mutation.values())) {
            case SOURCE_SHARD_STATES:
                var is = randomInt(sourceShardStates.length - 1);
                sourceShardStates[is] = IndexReshardingState.Split.SourceShardState.values()[(sourceShardStates[is].ordinal() + 1)
                    % IndexReshardingState.Split.SourceShardState.values().length];
                break;
            case TARGET_SHARD_STATES:
                var it = randomInt(targetShardStates.length - 1);
                targetShardStates[it] = IndexReshardingState.Split.TargetShardState.values()[(targetShardStates[it].ordinal() + 1)
                    % IndexReshardingState.Split.TargetShardState.values().length];
        }

        return new IndexReshardingState.Split(sourceShardStates, targetShardStates);
    }
}
