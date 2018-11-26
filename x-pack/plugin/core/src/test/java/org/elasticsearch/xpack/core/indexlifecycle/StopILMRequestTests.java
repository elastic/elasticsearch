/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class StopILMRequestTests extends AbstractStreamableTestCase<StopILMRequest> {

    @Override
    protected StopILMRequest createBlankInstance() {
        return new StopILMRequest();
    }

    @Override
    protected StopILMRequest createTestInstance() {
        return new StopILMRequest();
    }

    public void testValidate() {
        StopILMRequest request = createTestInstance();
        assertNull(request.validate());
    }

}
