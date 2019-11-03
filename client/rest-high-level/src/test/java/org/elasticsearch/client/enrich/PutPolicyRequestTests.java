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

import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class PutPolicyRequestTests extends AbstractRequestTestCase<PutPolicyRequest, PutEnrichPolicyAction.Request> {

    public void testValidate() {
        PutPolicyRequest request = createClientTestInstance();
        assertThat(request.validate().isPresent(), is(false));

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> new PutPolicyRequest(request.getName(), request.getType(), request.getIndices(), null, request.getEnrichFields()));
        assertThat(e.getMessage(), containsString("matchField must be a non-null and non-empty string"));
    }

    public void testEqualsAndHashcode() {
        PutPolicyRequest testInstance = createTestInstance();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(testInstance, (original) -> {
            PutPolicyRequest copy = new PutPolicyRequest(original.getName(), original.getType(), original.getIndices(),
                original.getMatchField(), original.getEnrichFields());
            copy.setQuery(original.getQuery());
            return copy;
        });
    }

    @Override
    protected PutPolicyRequest createClientTestInstance() {
        return createTestInstance("name");
    }

    public static PutPolicyRequest createTestInstance() {
        return createTestInstance(randomAlphaOfLength(4));
    }

    public static PutPolicyRequest createTestInstance(String name) {
        PutPolicyRequest testInstance = new PutPolicyRequest(
            name,
            randomAlphaOfLength(4),
            Arrays.asList(generateRandomStringArray(4, 4, false, false)),
            randomAlphaOfLength(4),
            Arrays.asList(generateRandomStringArray(4, 4, false, false))
        );
        if (randomBoolean()) {
            try {
                testInstance.setQuery(new MatchAllQueryBuilder());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return testInstance;
    }

    @Override
    protected PutEnrichPolicyAction.Request doParseToServerInstance(XContentParser parser) throws IOException {
        return PutEnrichPolicyAction.fromXContent(parser, "name");
    }

    @Override
    protected void assertInstances(PutEnrichPolicyAction.Request serverInstance, PutPolicyRequest clientTestInstance) {
        assertThat(clientTestInstance.getName(), equalTo(serverInstance.getName()));
        assertThat(clientTestInstance.getType(), equalTo(serverInstance.getPolicy().getType()));
        assertThat(clientTestInstance.getIndices(), equalTo(serverInstance.getPolicy().getIndices()));
        if (clientTestInstance.getQuery() != null) {
            XContentType type = serverInstance.getPolicy().getQuery().getContentType();
            assertThat(PutPolicyRequest.asMap(clientTestInstance.getQuery(), type),
                equalTo(PutPolicyRequest.asMap(serverInstance.getPolicy().getQuery().getQuery(), type)));
        } else {
            assertThat(serverInstance.getPolicy().getQuery(), nullValue());
        }
        assertThat(clientTestInstance.getMatchField(), equalTo(serverInstance.getPolicy().getMatchField()));
        assertThat(clientTestInstance.getEnrichFields(), equalTo(serverInstance.getPolicy().getEnrichFields()));
    }
}
