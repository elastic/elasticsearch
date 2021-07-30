/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.assertEqualPolicies;
import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.core.IsEqual.equalTo;

public class GetEnrichPolicyActionResponseTests extends AbstractSerializingTestCase<GetEnrichPolicyAction.Response> {

    @Override
    protected GetEnrichPolicyAction.Response doParseInstance(XContentParser parser) throws IOException {
        Map<String, EnrichPolicy> policies = new HashMap<>();
        assert parser.nextToken() == XContentParser.Token.START_OBJECT;
        assert parser.nextToken() == XContentParser.Token.FIELD_NAME;
        assert parser.currentName().equals("policies");
        assert parser.nextToken() == XContentParser.Token.START_ARRAY;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            assert token == XContentParser.Token.START_OBJECT;
            assert parser.nextToken() == XContentParser.Token.FIELD_NAME;
            assert parser.currentName().equals("config");
            assert parser.nextToken() == XContentParser.Token.START_OBJECT;
            EnrichPolicy.NamedPolicy policy = EnrichPolicy.NamedPolicy.fromXContent(parser);
            policies.put(policy.getName(), policy.getPolicy());
            assert parser.nextToken() == XContentParser.Token.END_OBJECT;
        }

        return new GetEnrichPolicyAction.Response(policies);
    }

    @Override
    protected GetEnrichPolicyAction.Response createTestInstance() {
        Map<String, EnrichPolicy> items = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
            items.put(randomAlphaOfLength(3), policy);
        }
        return new GetEnrichPolicyAction.Response(items);
    }

    @Override
    protected Writeable.Reader<GetEnrichPolicyAction.Response> instanceReader() {
        return GetEnrichPolicyAction.Response::new;
    }

    @Override
    protected void assertEqualInstances(GetEnrichPolicyAction.Response expectedInstance, GetEnrichPolicyAction.Response newInstance) {
        assertNotSame(expectedInstance, newInstance);
        // the tests shuffle around the policy query source xcontent type, so this is needed here
        assertThat(expectedInstance.getPolicies().size(), equalTo(newInstance.getPolicies().size()));
        // since the backing store is a treemap the list will be sorted so we can just check each
        // instance is the same
        for (int i = 0; i < expectedInstance.getPolicies().size(); i++) {
            EnrichPolicy.NamedPolicy expected = expectedInstance.getPolicies().get(i);
            EnrichPolicy.NamedPolicy newed = newInstance.getPolicies().get(i);
            assertThat(expected.getName(), equalTo(newed.getName()));
            assertEqualPolicies(expected.getPolicy(), newed.getPolicy());
        }
    }
}
