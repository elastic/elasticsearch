/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class StartILMRequestTests extends AbstractStreamableTestCase<StartILMRequest> {

    @Override
    protected StartILMRequest createBlankInstance() {
        return new StartILMRequest();
    }

    @Override
    protected StartILMRequest createTestInstance() {
        return new StartILMRequest();
    }

    public void testValidate() {
        StartILMRequest request = createTestInstance();
        assertNull(request.validate());
    }

}
