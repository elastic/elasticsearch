/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.SimpleDiffableWireSerializationTestCase;

import java.util.List;

public class DataStreamGlobalRetentionSerializationTests extends SimpleDiffableWireSerializationTestCase<ClusterState.Custom> {

    @Override
    protected ClusterState.Custom makeTestChanges(ClusterState.Custom testInstance) {
        if (randomBoolean()) {
            return testInstance;
        }
        return mutateInstance(testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<ClusterState.Custom>> diffReader() {
        return DataStreamGlobalRetention::readDiffFrom;
    }

    @Override
    protected Writeable.Reader<ClusterState.Custom> instanceReader() {
        return DataStreamGlobalRetention::read;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(ClusterState.Custom.class, DataStreamGlobalRetention.TYPE, DataStreamGlobalRetention::read)
            )
        );
    }

    @Override
    protected ClusterState.Custom createTestInstance() {
        return randomGlobalRetention();
    }

    @Override
    protected ClusterState.Custom mutateInstance(ClusterState.Custom instance) {
        DataStreamGlobalRetention metadata = (DataStreamGlobalRetention) instance;
        var defaultRetention = metadata.getDefaultRetention();
        var maxRetention = metadata.getMaxRetention();
        switch (randomInt(1)) {
            case 0 -> {
                defaultRetention = randomValueOtherThan(
                    defaultRetention,
                    () -> randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(1, 1000))
                );
            }
            case 1 -> {
                maxRetention = randomValueOtherThan(
                    maxRetention,
                    () -> randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(1001, 2000))
                );
            }
        }
        return new DataStreamGlobalRetention(defaultRetention, maxRetention);
    }

    public static DataStreamGlobalRetention randomGlobalRetention() {
        return new DataStreamGlobalRetention(
            randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(1, 1000)),
            randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(1000, 2000))
        );
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(createTestInstance(), ignored -> 1);
    }

    public void testValidation() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new DataStreamGlobalRetention(
                TimeValue.timeValueDays(randomIntBetween(1001, 2000)),
                TimeValue.timeValueDays(randomIntBetween(1, 1000))
            )
        );
    }
}
