/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.test.AbstractBroadcastResponseTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.util.List;

public class RefreshResponseTests extends AbstractBroadcastResponseTestCase<RefreshResponse> {

    @Override
    protected RefreshResponse createTestInstance(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> failures
    ) {
        return new RefreshResponse(totalShards, successfulShards, failedShards, failures);
    }

    @Override
    protected RefreshResponse doParseInstance(XContentParser parser) {
        return RefreshResponse.fromXContent(parser);
    }
}
