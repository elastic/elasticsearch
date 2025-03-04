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
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

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

        return new IndexReshardingMetadata(
            IndexReshardingMetadata.Operation.SPLIT,
            new IndexReshardingState.Split(sourceShardStates, targetShardStates)
        );
    }

    // To exercise equals() we want to mutate exactly one thing randomly so we know that each component
    // is contributing to equality testing. Some of these mutations are semantically illegal but we ignore
    // that here.
    @Override
    protected IndexReshardingMetadata mutateInstance(IndexReshardingMetadata instance) throws IOException {
        enum Mutation {
            SOURCE_SHARD_STATES,
            TARGET_SHARD_STATES
        }

        var split = instance.getSplit();
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

        return new IndexReshardingMetadata(instance.operation, new IndexReshardingState.Split(sourceShardStates, targetShardStates));
    }

    public void testInvalidSerializedState() throws IOException {
        // the parser does an unchecked cast on shards so it gets a little extra sanity check
        String json = """
            {
                "operation": "SPLIT",
                "source_shards": [1],
                "target_shards": [2]
            }
            """;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            assertThrows(IllegalArgumentException.class, () -> IndexReshardingMetadata.fromXContent(parser));
        }
    }
}
