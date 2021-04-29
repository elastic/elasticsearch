/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
            assertThat(PutPolicyRequest.asMap(clientTestInstance.getQuery(), XContentType.JSON),
                equalTo(PutPolicyRequest.asMap(serverInstance.getPolicy().getQuery().getQuery(), type)));
        } else {
            assertThat(serverInstance.getPolicy().getQuery(), nullValue());
        }
        assertThat(clientTestInstance.getMatchField(), equalTo(serverInstance.getPolicy().getMatchField()));
        assertThat(clientTestInstance.getEnrichFields(), equalTo(serverInstance.getPolicy().getEnrichFields()));
    }
}
