/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class StartILMRequestTests extends AbstractWireSerializingTestCase<StartILMRequest> {

    @Override
    protected StartILMRequest createTestInstance() {
        return new StartILMRequest();
    }

    @Override
    protected Writeable.Reader<StartILMRequest> instanceReader() {
        return StartILMRequest::new;
    }

    public void testValidate() {
        StartILMRequest request = createTestInstance();
        assertNull(request.validate());
    }

}
