/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SimpleDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DataStreamAutoShardingEventTests extends SimpleDiffableSerializationTestCase<DataStreamAutoShardingEvent> {

    @Override
    protected DataStreamAutoShardingEvent doParseInstance(XContentParser parser) throws IOException {
        return DataStreamAutoShardingEvent.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DataStreamAutoShardingEvent> instanceReader() {
        return DataStreamAutoShardingEvent::new;
    }

    @Override
    protected DataStreamAutoShardingEvent createTestInstance() {
        return DataStreamAutoShardingEventTests.randomInstance();
    }

    @Override
    protected DataStreamAutoShardingEvent mutateInstance(DataStreamAutoShardingEvent instance) {
        String triggerIndex = instance.triggerIndexName();
        long timestamp = instance.timestamp();
        int targetNumberOfShards = instance.targetNumberOfShards();
        switch (randomInt(2)) {
            case 0 -> triggerIndex = randomValueOtherThan(triggerIndex, () -> randomAlphaOfLengthBetween(10, 50));
            case 1 -> timestamp = randomValueOtherThan(timestamp, ESTestCase::randomNonNegativeLong);
            case 2 -> targetNumberOfShards = randomValueOtherThan(targetNumberOfShards, ESTestCase::randomNonNegativeInt);
        }
        return new DataStreamAutoShardingEvent(triggerIndex, targetNumberOfShards, timestamp);
    }

    static DataStreamAutoShardingEvent randomInstance() {
        return new DataStreamAutoShardingEvent(randomAlphaOfLengthBetween(10, 40), randomNonNegativeInt(), randomNonNegativeLong());
    }

    @Override
    protected DataStreamAutoShardingEvent makeTestChanges(DataStreamAutoShardingEvent testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<DataStreamAutoShardingEvent>> diffReader() {
        return DataStreamAutoShardingEvent::readDiffFrom;
    }
}
