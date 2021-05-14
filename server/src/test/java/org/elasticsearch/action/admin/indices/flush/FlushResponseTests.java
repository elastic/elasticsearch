/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractBroadcastResponseTestCase;

import java.util.List;

public class FlushResponseTests extends AbstractBroadcastResponseTestCase<FlushResponse> {

    @Override
    protected FlushResponse createTestInstance(int totalShards, int successfulShards, int failedShards,
                                               List<DefaultShardOperationFailedException> failures) {
        return new FlushResponse(totalShards, successfulShards, failedShards, failures);
    }

    @Override
    protected FlushResponse doParseInstance(XContentParser parser) {
        return FlushResponse.fromXContent(parser);
    }
}
