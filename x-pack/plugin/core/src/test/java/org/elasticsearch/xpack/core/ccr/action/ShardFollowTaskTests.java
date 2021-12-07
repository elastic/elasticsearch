/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;

public class ShardFollowTaskTests extends AbstractSerializingTestCase<ShardFollowTask> {

    @Override
    protected ShardFollowTask doParseInstance(XContentParser parser) throws IOException {
        return ShardFollowTask.fromXContent(parser);
    }

    @Override
    protected ShardFollowTask createTestInstance() {
        return new ShardFollowTask(
            randomAlphaOfLength(4),
            new ShardId(randomAlphaOfLength(4), randomAlphaOfLength(4), randomInt(5)),
            new ShardId(randomAlphaOfLength(4), randomAlphaOfLength(4), randomInt(5)),
            randomIntBetween(1, Integer.MAX_VALUE),
            randomIntBetween(1, Integer.MAX_VALUE),
            randomIntBetween(1, Integer.MAX_VALUE),
            randomIntBetween(1, Integer.MAX_VALUE),
            new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES),
            new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES),
            randomIntBetween(1, Integer.MAX_VALUE),
            new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES),
            TimeValue.parseTimeValue(randomTimeValue(), ""),
            TimeValue.parseTimeValue(randomTimeValue(), ""),
            randomBoolean() ? null : Collections.singletonMap("key", "value")
        );
    }

    @Override
    protected Writeable.Reader<ShardFollowTask> instanceReader() {
        return ShardFollowTask::readFrom;
    }
}
