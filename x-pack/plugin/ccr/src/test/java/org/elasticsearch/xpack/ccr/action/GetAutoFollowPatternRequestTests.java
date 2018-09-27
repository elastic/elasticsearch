/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;

public class GetAutoFollowPatternRequestTests extends AbstractStreamableTestCase<GetAutoFollowPatternAction.Request> {

    @Override
    protected GetAutoFollowPatternAction.Request createBlankInstance() {
        return new GetAutoFollowPatternAction.Request();
    }

    @Override
    protected GetAutoFollowPatternAction.Request createTestInstance() {
        GetAutoFollowPatternAction.Request request = new GetAutoFollowPatternAction.Request();
        if (randomBoolean()) {
            request.setLeaderClusterAlias(randomAlphaOfLength(4));
        }
        return request;
    }
}
