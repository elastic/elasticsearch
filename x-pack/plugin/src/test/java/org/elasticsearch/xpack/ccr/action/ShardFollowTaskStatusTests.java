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

public class ShardFollowTaskStatusTests extends AbstractSerializingTestCase<ShardFollowTask.Status> {

    @Override
    protected ShardFollowTask.Status doParseInstance(XContentParser parser) throws IOException {
        return ShardFollowTask.Status.fromXContent(parser);
    }

    @Override
    protected ShardFollowTask.Status createTestInstance() {
        ShardFollowTask.Status status = new ShardFollowTask.Status();
        status.setProcessedGlobalCheckpoint(randomNonNegativeLong());
        return status;
    }

    @Override
    protected Writeable.Reader<ShardFollowTask.Status> instanceReader() {
        return ShardFollowTask.Status::new;
    }
}
