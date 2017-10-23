/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class ShardFollowTaskTests extends AbstractSerializingTestCase<ShardFollowTask> {

    @Override
    protected ShardFollowTask doParseInstance(XContentParser parser) throws IOException {
        return ShardFollowTask.fromXContent(parser);
    }

    @Override
    protected ShardFollowTask createTestInstance() {
        return new ShardFollowTask(
                new ShardId(randomAlphaOfLength(4), randomAlphaOfLength(4), randomInt(5)),
                new ShardId(randomAlphaOfLength(4), randomAlphaOfLength(4), randomInt(5))
        );
    }

    @Override
    protected Writeable.Reader<ShardFollowTask> instanceReader() {
        return ShardFollowTask::new;
    }
}
