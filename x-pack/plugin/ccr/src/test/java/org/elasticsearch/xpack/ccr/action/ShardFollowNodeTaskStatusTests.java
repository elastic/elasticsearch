/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class ShardFollowNodeTaskStatusTests extends AbstractSerializingTestCase<ShardFollowNodeTask.Status> {

    @Override
    protected ShardFollowNodeTask.Status doParseInstance(XContentParser parser) throws IOException {
        return ShardFollowNodeTask.Status.fromXContent(parser);
    }

    @Override
    protected ShardFollowNodeTask.Status createTestInstance() {
        return new ShardFollowNodeTask.Status(
                randomInt(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong());
    }

    @Override
    protected Writeable.Reader<ShardFollowNodeTask.Status> instanceReader() {
        return ShardFollowNodeTask.Status::new;
    }

}
