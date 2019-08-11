/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class ShardChangesRequestTests extends AbstractWireSerializingTestCase<ShardChangesAction.Request> {

    @Override
    protected ShardChangesAction.Request createTestInstance() {
        ShardChangesAction.Request request =
            new ShardChangesAction.Request(new ShardId("_index", "_indexUUID", 0), randomAlphaOfLength(4));
        request.setMaxOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        request.setFromSeqNo(randomNonNegativeLong());
        return request;
    }

    @Override
    protected Writeable.Reader<ShardChangesAction.Request> instanceReader() {
        return ShardChangesAction.Request::new;
    }

    public void testValidate() {
        ShardChangesAction.Request request = new ShardChangesAction.Request(new ShardId("_index", "_indexUUID", 0), "uuid");
        request.setFromSeqNo(-1);
        assertThat(request.validate().getMessage(), containsString("fromSeqNo [-1] cannot be lower than 0"));

        request.setFromSeqNo(0);
        request.setMaxOperationCount(-1);
        assertThat(request.validate().getMessage(), containsString("maxOperationCount [-1] cannot be lower than 0"));

        request.setMaxOperationCount(8);
        assertThat(request.validate(), nullValue());
    }
}
