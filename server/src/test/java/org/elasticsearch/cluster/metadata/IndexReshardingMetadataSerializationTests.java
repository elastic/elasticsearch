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
import java.util.Arrays;

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
        final var targetReshardingStates = new IndexReshardingMetadata.ShardReshardingState[newShards - oldShards];
        for (int i = 0; i < targetReshardingStates.length; i++) {
            targetReshardingStates[i] = randomFrom(IndexReshardingMetadata.ShardReshardingState.values());
        }

        return new IndexReshardingMetadata(oldShards, newShards, targetReshardingStates);
    }

    @Override
    protected IndexReshardingMetadata mutateInstance(IndexReshardingMetadata instance) throws IOException {
        // this is fairly redundant, since this function exists in the base class for testing that instances
        // that have been mutated are different according to equals. The test doesn't seem super valuable to me tbh.
        IndexReshardingMetadata newInstance;
        do {
            newInstance = createTestInstance();
        } while (newInstance.oldShardCount() == instance.oldShardCount()
            && newInstance.newShardCount() == instance.newShardCount()
            && Arrays.equals(newInstance.targetShardStates(), instance.targetShardStates()));

        return newInstance;
    }
}
