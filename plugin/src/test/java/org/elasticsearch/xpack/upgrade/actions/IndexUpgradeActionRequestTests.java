/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.actions;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeAction.Request;

import java.util.Collections;

public class IndexUpgradeActionRequestTests extends AbstractStreamableTestCase<Request> {
    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLength(10));
        if (randomBoolean()) {
            request.extraParams(Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(20)));
        }
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }
}
