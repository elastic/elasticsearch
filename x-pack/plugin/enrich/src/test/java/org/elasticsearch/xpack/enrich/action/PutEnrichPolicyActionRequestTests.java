/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;

public class PutEnrichPolicyActionRequestTests extends AbstractWireSerializingTestCase<PutEnrichPolicyAction.Request> {

    @Override
    protected PutEnrichPolicyAction.Request createTestInstance() {
        final EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        return new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, randomAlphaOfLength(3), policy);
    }

    @Override
    protected PutEnrichPolicyAction.Request mutateInstance(PutEnrichPolicyAction.Request instance) {
        String name = instance.getName();
        EnrichPolicy policy = instance.getPolicy();

        switch (between(0, 1)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomAlphaOfLength(3));
            case 1 -> policy = randomValueOtherThan(policy, () -> randomEnrichPolicy(XContentType.JSON));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, name, policy);
    }

    @Override
    protected Writeable.Reader<PutEnrichPolicyAction.Request> instanceReader() {
        return PutEnrichPolicyAction.Request::new;
    }
}
