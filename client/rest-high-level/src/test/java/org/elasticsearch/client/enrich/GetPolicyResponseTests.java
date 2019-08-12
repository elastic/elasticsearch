/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class GetPolicyResponseTests extends AbstractResponseTestCase<GetEnrichPolicyAction.Response, GetPolicyResponse> {

    @Override
    protected GetEnrichPolicyAction.Response createServerTestInstance(XContentType xContentType) throws IOException {
        return new GetEnrichPolicyAction.Response(createRandomEnrichPolicy(xContentType));
    }

    @Override
    protected GetPolicyResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return new GetPolicyResponse(parser);
    }

    @Override
    protected void assertInstances(GetEnrichPolicyAction.Response serverTestInstance, GetPolicyResponse clientInstance) {
        assertThat(clientInstance.getPolicy().getType(), equalTo(serverTestInstance.getPolicy().getType()));
        assertThat(clientInstance.getPolicy().getIndices(), equalTo(serverTestInstance.getPolicy().getIndices()));
        if (clientInstance.getPolicy().getQuery() !=  null) {
            assertThat(clientInstance.getPolicy().getQuery(), equalTo(serverTestInstance.getPolicy().getQuery().getQuery()));
        } else {
            assertThat(serverTestInstance.getPolicy().getQuery(), nullValue());
        }
        assertThat(clientInstance.getPolicy().getEnrichKey(), equalTo(serverTestInstance.getPolicy().getEnrichKey()));
        assertThat(clientInstance.getPolicy().getEnrichValues(), equalTo(serverTestInstance.getPolicy().getEnrichValues()));
    }

    private static EnrichPolicy createRandomEnrichPolicy(XContentType xContentType) throws IOException {
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        builder.startObject();
        builder.endObject();
        BytesReference querySource = BytesArray.bytes(builder);

        return new EnrichPolicy(
            randomAlphaOfLength(4),
            randomBoolean() ? new EnrichPolicy.QuerySource(querySource, xContentType) : null,
            Arrays.asList(generateRandomStringArray(8, 4, false, false)),
            randomAlphaOfLength(4),
            Arrays.asList(generateRandomStringArray(8, 4, false, false))
        );
    }
}
