/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class GetPolicyResponseTests extends AbstractResponseTestCase<GetEnrichPolicyAction.Response, GetPolicyResponse> {

    @Override
    protected GetEnrichPolicyAction.Response createServerTestInstance(XContentType xContentType) {
        int numPolicies = randomIntBetween(0, 8);
        Map<String, EnrichPolicy> policies = new HashMap<>(numPolicies);
        for (int i = 0; i < numPolicies; i++) {
            policies.put(randomAlphaOfLength(4), createRandomEnrichPolicy(xContentType));
        }
        return new GetEnrichPolicyAction.Response(policies);
    }

    @Override
    protected GetPolicyResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetPolicyResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(GetEnrichPolicyAction.Response serverTestInstance, GetPolicyResponse clientInstance) {
        assertThat(clientInstance.getPolicies().size(), equalTo(serverTestInstance.getPolicies().size()));
        for (int i = 0; i < clientInstance.getPolicies().size(); i++) {
            assertThat(clientInstance.getPolicies().get(i).getType(),
                equalTo(serverTestInstance.getPolicies().get(i).getPolicy().getType()));
            assertThat(clientInstance.getPolicies().get(i).getName(),
                equalTo(serverTestInstance.getPolicies().get(i).getName()));
            assertThat(clientInstance.getPolicies().get(i).getIndices(),
                equalTo(serverTestInstance.getPolicies().get(i).getPolicy().getIndices()));
            if (clientInstance.getPolicies().get(i).getQuery() !=  null) {
                assertThat(clientInstance.getPolicies().get(i).getQuery(),
                    equalTo(serverTestInstance.getPolicies().get(i).getPolicy().getQuery().getQuery()));
            } else {
                assertThat(serverTestInstance.getPolicies().get(i).getPolicy().getQuery(), nullValue());
            }
            assertThat(clientInstance.getPolicies().get(i).getMatchField(),
                equalTo(serverTestInstance.getPolicies().get(i).getPolicy().getMatchField()));
            assertThat(clientInstance.getPolicies().get(i).getEnrichFields(),
                equalTo(serverTestInstance.getPolicies().get(i).getPolicy().getEnrichFields()));
        }
    }

    private static EnrichPolicy createRandomEnrichPolicy(XContentType xContentType){
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            builder.endObject();
            BytesReference querySource = BytesReference.bytes(builder);
            return new EnrichPolicy(
                randomAlphaOfLength(4),
                randomBoolean() ? new EnrichPolicy.QuerySource(querySource, xContentType) : null,
                Arrays.asList(generateRandomStringArray(8, 4, false, false)),
                randomAlphaOfLength(4),
                Arrays.asList(generateRandomStringArray(8, 4, false, false))
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
