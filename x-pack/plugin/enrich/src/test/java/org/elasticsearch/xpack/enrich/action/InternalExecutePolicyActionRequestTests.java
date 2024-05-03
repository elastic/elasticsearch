/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.enrich.action.InternalExecutePolicyAction.Request;

public class InternalExecutePolicyActionRequestTests extends AbstractWireSerializingTestCase<Request> {
    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLength(3), randomAlphaOfLength(5));
        if (randomBoolean()) {
            request.setWaitForCompletion(true);
        }
        return request;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String policyName = instance.getName();
        String enrichIndexName = instance.getEnrichIndexName();
        boolean waitForCompletion = instance.isWaitForCompletion();

        switch (between(0, 2)) {
            case 0 -> policyName = randomAlphaOfLength(4);
            case 1 -> enrichIndexName = randomAlphaOfLength(6);
            case 2 -> waitForCompletion = waitForCompletion == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        Request request = new Request(policyName, enrichIndexName);
        request.setWaitForCompletion(waitForCompletion);
        return request;
    }
}
