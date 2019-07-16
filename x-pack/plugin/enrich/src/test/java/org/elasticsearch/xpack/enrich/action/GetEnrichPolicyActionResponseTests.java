/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicyDefinition;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;

import java.io.IOException;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.assertEqualPolicies;
import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;

public class GetEnrichPolicyActionResponseTests extends AbstractSerializingTestCase<GetEnrichPolicyAction.Response> {

    @Override
    protected GetEnrichPolicyAction.Response doParseInstance(XContentParser parser) throws IOException {
        EnrichPolicyDefinition policy = EnrichPolicyDefinition.fromXContent(parser);
        return new GetEnrichPolicyAction.Response(policy);
    }

    @Override
    protected GetEnrichPolicyAction.Response createTestInstance() {
        EnrichPolicyDefinition policy = randomEnrichPolicy(XContentType.JSON);
        return new GetEnrichPolicyAction.Response(policy);
    }

    @Override
    protected Writeable.Reader<GetEnrichPolicyAction.Response> instanceReader() {
        return GetEnrichPolicyAction.Response::new;
    }

    @Override
    protected void assertEqualInstances(GetEnrichPolicyAction.Response expectedInstance, GetEnrichPolicyAction.Response newInstance) {
        assertNotSame(expectedInstance, newInstance);
        // the tests shuffle around the policy query source xcontent type, so this is needed here
        assertEqualPolicies(expectedInstance.getPolicy(), newInstance.getPolicy());
    }
}
