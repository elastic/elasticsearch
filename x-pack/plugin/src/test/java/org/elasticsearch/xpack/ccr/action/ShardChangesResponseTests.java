/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class ShardChangesResponseTests extends AbstractStreamableTestCase<ShardChangesAction.Response> {

    @Override
    protected ShardChangesAction.Response createTestInstance() {
        int numOps = randomInt(8);
        List<Translog.Operation> operations = new ArrayList<>(numOps);
        for (int i = 0; i < numOps; i++) {
            operations.add(new Translog.NoOp(i, 0, "test"));
        }
        return new ShardChangesAction.Response(operations);
    }

    @Override
    protected ShardChangesAction.Response createBlankInstance() {
        return new ShardChangesAction.Response();
    }

}
