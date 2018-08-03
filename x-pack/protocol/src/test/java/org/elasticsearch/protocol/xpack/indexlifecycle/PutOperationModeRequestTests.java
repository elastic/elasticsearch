/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.test.AbstractStreamableTestCase;

import java.io.IOException;

public class PutOperationModeRequestTests extends AbstractStreamableTestCase<PutOperationModeRequest> {

    @Override
    protected PutOperationModeRequest createBlankInstance() {
        return new PutOperationModeRequest();
    }

    @Override
    protected PutOperationModeRequest createTestInstance() {
        return new PutOperationModeRequest(randomFrom(OperationMode.STOPPING, OperationMode.RUNNING));
    }

    @Override
    protected PutOperationModeRequest mutateInstance(PutOperationModeRequest instance) throws IOException {
        return new PutOperationModeRequest(
                randomValueOtherThan(instance.getMode(), () -> randomFrom(OperationMode.STOPPING, OperationMode.RUNNING)));
    }

    public void testNullMode() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new PutOperationModeRequest(null));
        assertEquals("mode cannot be null", exception.getMessage());
    }

    public void testStoppedMode() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new PutOperationModeRequest(OperationMode.STOPPED));
        assertEquals("cannot directly stop index-lifecycle", exception.getMessage());
    }

    public void testValidate() {
        PutOperationModeRequest request = createTestInstance();
        assertNull(request.validate());
    }

}
