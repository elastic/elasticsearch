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
import java.util.Optional;

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
        assertThat(expectedInstance.getPolicies().size(), equalTo(newInstance.getPolicies().size()));
        for (EnrichPolicy expectedPolicy: expectedInstance.getPolicies()) {
            // contains and indexOf cannot be used here as the query source may be represented differently, so we need to check
            // if the name is the same and if it is, use that to ensure the policies are the same
            Optional<EnrichPolicy> maybePolicy = newInstance.getPolicies().stream()
                .filter(p -> p.getName().equals(expectedPolicy.getName())).findFirst();
            assertTrue(maybePolicy.isPresent());
            EnrichPolicy newPolicy = maybePolicy.get();
            assertEqualPolicies(expectedPolicy, newPolicy);
            assertThat(expectedPolicy.getName(), equalTo(newPolicy.getName()));
        }
    }
}
