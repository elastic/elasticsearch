/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;

public class PutEnrichPolicyActionRequestTests extends AbstractWireSerializingTestCase<PutEnrichPolicyAction.Request> {

    @Override
    protected PutEnrichPolicyAction.Request createTestInstance() {
        final EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        return new PutEnrichPolicyAction.Request(randomAlphaOfLength(3), policy);
    }

    @Override
    protected Writeable.Reader<PutEnrichPolicyAction.Request> instanceReader() {
        return PutEnrichPolicyAction.Request::new;
    }
}
