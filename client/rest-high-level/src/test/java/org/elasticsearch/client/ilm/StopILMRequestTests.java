/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class StopILMRequestTests extends ESTestCase {

    protected StopILMRequest createTestInstance() {
        return new StopILMRequest();
    }

    public void testValidate() {
        StopILMRequest request = createTestInstance();
        assertFalse(request.validate().isPresent());
    }

    public void testEqualsAndHashcode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), (original) -> createTestInstance());
    }

}
