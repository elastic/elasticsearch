/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class DeleteAutoFollowPatternRequestTests extends AbstractStreamableTestCase<DeleteAutoFollowPatternAction.Request> {

    @Override
    protected DeleteAutoFollowPatternAction.Request createBlankInstance() {
        return new DeleteAutoFollowPatternAction.Request();
    }

    @Override
    protected DeleteAutoFollowPatternAction.Request createTestInstance() {
        DeleteAutoFollowPatternAction.Request request = new DeleteAutoFollowPatternAction.Request();
        request.setLeaderClusterAlias(randomAlphaOfLength(4));
        return request;
    }
}
