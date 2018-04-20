/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class DeprecationInfoActionRequestTests extends AbstractStreamableTestCase<DeprecationInfoAction.Request> {

    @Override
    protected DeprecationInfoAction.Request createTestInstance() {
        return new DeprecationInfoAction.Request(randomAlphaOfLength(10));
    }

    @Override
    protected DeprecationInfoAction.Request createBlankInstance() {
        return new DeprecationInfoAction.Request();
    }
}
