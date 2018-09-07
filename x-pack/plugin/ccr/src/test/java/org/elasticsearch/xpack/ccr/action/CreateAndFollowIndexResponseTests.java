/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class CreateAndFollowIndexResponseTests extends AbstractStreamableTestCase<CreateAndFollowIndexAction.Response> {

    @Override
    protected CreateAndFollowIndexAction.Response createBlankInstance() {
        return new CreateAndFollowIndexAction.Response();
    }

    @Override
    protected CreateAndFollowIndexAction.Response createTestInstance() {
        return new CreateAndFollowIndexAction.Response(randomBoolean(), randomBoolean(), randomBoolean());
    }
}
