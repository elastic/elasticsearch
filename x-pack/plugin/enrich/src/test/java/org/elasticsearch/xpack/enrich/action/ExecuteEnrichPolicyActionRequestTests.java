/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;

public class ExecuteEnrichPolicyActionRequestTests extends AbstractWireSerializingTestCase<ExecuteEnrichPolicyAction.Request> {

    @Override
    protected ExecuteEnrichPolicyAction.Request createTestInstance() {
        return new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, randomAlphaOfLength(3));
    }

    @Override
    protected ExecuteEnrichPolicyAction.Request mutateInstance(ExecuteEnrichPolicyAction.Request instance) {
        String name = instance.getName();
        boolean waitForCompletion = instance.isWaitForCompletion();

        switch (between(0, 1)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomAlphaOfLength(3));
            case 1 -> waitForCompletion = waitForCompletion == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, name).setWaitForCompletion(waitForCompletion);
    }

    @Override
    protected Writeable.Reader<ExecuteEnrichPolicyAction.Request> instanceReader() {
        return ExecuteEnrichPolicyAction.Request::new;
    }
}
