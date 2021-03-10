/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class StopILMRequestTests extends AbstractWireSerializingTestCase<StopILMRequest> {

    @Override
    protected StopILMRequest createTestInstance() {
        return new StopILMRequest();
    }

    @Override
    protected Writeable.Reader<StopILMRequest> instanceReader() {
        return StopILMRequest::new;
    }

    public void testValidate() {
        StopILMRequest request = createTestInstance();
        assertNull(request.validate());
    }

}
