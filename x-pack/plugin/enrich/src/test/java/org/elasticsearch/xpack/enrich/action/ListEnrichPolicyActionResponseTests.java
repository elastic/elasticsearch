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
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ListEnrichPolicyAction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.assertEqualPolicies;
import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.Matchers.equalTo;

public class ListEnrichPolicyActionResponseTests extends AbstractSerializingTestCase<ListEnrichPolicyAction.Response> {
    @Override
    protected ListEnrichPolicyAction.Response doParseInstance(XContentParser parser) throws IOException {
        Map<String, EnrichPolicy> policies = new HashMap<>();
        assert parser.nextToken() == XContentParser.Token.START_OBJECT;
        assert parser.nextToken() == XContentParser.Token.FIELD_NAME;
        assert parser.currentName().equals("policies");
        assert parser.nextToken() == XContentParser.Token.START_ARRAY;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            assert token == XContentParser.Token.START_OBJECT;
            EnrichPolicy policy = EnrichPolicy.fromXContent(parser);
            policies.put(policy.getName(), policy);
        }

        return new ListEnrichPolicyAction.Response(policies);
    }

    @Override
    protected ListEnrichPolicyAction.Response createTestInstance() {
        Map<String, EnrichPolicy> items = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
            items.put(policy.getName(), policy);
        }
        return new ListEnrichPolicyAction.Response(items);
    }

    @Override
    protected Writeable.Reader<ListEnrichPolicyAction.Response> instanceReader() {
        return ListEnrichPolicyAction.Response::new;
    }

    @Override
    protected void assertEqualInstances(ListEnrichPolicyAction.Response expectedInstance, ListEnrichPolicyAction.Response newInstance) {
        assertThat(expectedInstance.getPolicyMap().size(), equalTo(newInstance.getPolicyMap().size()));

        for (Map.Entry<String, EnrichPolicy> entry: expectedInstance.getPolicyMap().entrySet()) {
            EnrichPolicy newPolicy = newInstance.getPolicyMap().get(entry.getKey());
            assertEqualPolicies(entry.getValue(), newPolicy);
            assertThat(entry.getKey(), equalTo(newPolicy.getName()));
        }
    }
}
