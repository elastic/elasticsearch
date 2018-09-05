/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.AbstractStreamableTestCase;

public class ShardChangesResponseTests extends AbstractStreamableTestCase<ShardChangesAction.Response> {

    @Override
    protected ShardChangesAction.Response createTestInstance() {
        final long mappingVersion = randomNonNegativeLong();
        final long leaderGlobalCheckpoint = randomNonNegativeLong();
        final long leaderMaxSeqNo = randomLongBetween(leaderGlobalCheckpoint, Long.MAX_VALUE);
        final int numOps = randomInt(8);
        final Translog.Operation[] operations = new Translog.Operation[numOps];
        for (int i = 0; i < numOps; i++) {
            operations[i] = new Translog.NoOp(i, 0, "test");
        }
        return new ShardChangesAction.Response(mappingVersion, leaderGlobalCheckpoint, leaderMaxSeqNo, operations);
    }

    @Override
    protected ShardChangesAction.Response createBlankInstance() {
        return new ShardChangesAction.Response();
    }

}
