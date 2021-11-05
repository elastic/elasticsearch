/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ShardChangesResponseTests extends AbstractWireSerializingTestCase<ShardChangesAction.Response> {

    @Override
    protected ShardChangesAction.Response createTestInstance() {
        final long mappingVersion = randomNonNegativeLong();
        final long settingsVersion = randomNonNegativeLong();
        final long aliasesVersion = randomNonNegativeLong();
        final long leaderGlobalCheckpoint = randomNonNegativeLong();
        final long leaderMaxSeqNo = randomLongBetween(leaderGlobalCheckpoint, Long.MAX_VALUE);
        final long maxSeqNoOfUpdatesOrDeletes = randomLongBetween(-1, Long.MAX_VALUE);
        final int numOps = randomInt(8);
        final Translog.Operation[] operations = new Translog.Operation[numOps];
        for (int i = 0; i < numOps; i++) {
            operations[i] = new Translog.NoOp(i, 0, "test");
        }
        return new ShardChangesAction.Response(
            mappingVersion,
            settingsVersion,
            aliasesVersion,
            leaderGlobalCheckpoint,
            leaderMaxSeqNo,
            maxSeqNoOfUpdatesOrDeletes,
            operations,
            randomNonNegativeLong()
        );
    }

    @Override
    protected Writeable.Reader<ShardChangesAction.Response> instanceReader() {
        return ShardChangesAction.Response::new;
    }
}
