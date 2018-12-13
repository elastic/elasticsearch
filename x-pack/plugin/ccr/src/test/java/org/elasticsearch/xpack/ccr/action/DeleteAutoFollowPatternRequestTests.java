/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;

public class DeleteAutoFollowPatternRequestTests extends AbstractWireSerializingTestCase<DeleteAutoFollowPatternAction.Request> {

    @Override
    protected Writeable.Reader<DeleteAutoFollowPatternAction.Request> instanceReader() {
        return DeleteAutoFollowPatternAction.Request::new;
    }

    @Override
    protected DeleteAutoFollowPatternAction.Request createTestInstance() {
        return new DeleteAutoFollowPatternAction.Request(randomAlphaOfLength(4));
    }
}
