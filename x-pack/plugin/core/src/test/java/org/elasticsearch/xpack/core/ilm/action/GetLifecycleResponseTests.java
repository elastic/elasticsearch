/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MockAction;
import org.elasticsearch.xpack.core.ilm.TestLifecycleType;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.LifecyclePolicyResponseItem;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests.randomTestLifecyclePolicy;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class GetLifecycleResponseTests extends ESTestCase {

    protected Response createTestInstance() {
        String randomPrefix = randomAlphaOfLength(5);
        List<LifecyclePolicyResponseItem> responseItems = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 2); i++) {
            responseItems.add(
                new LifecyclePolicyResponseItem(
                    randomTestLifecyclePolicy(randomPrefix + i),
                    randomNonNegativeLong(),
                    randomAlphaOfLength(8),
                    randomUsage()
                )
            );
        }
        return new Response(responseItems);
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        Response response = createTestInstance();
        XContentBuilder builder = jsonBuilder().prettyPrint();
        response.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xcontent -> {
            try {
                xcontent.toXContent(builder, EMPTY_PARAMS);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                fail(e.getMessage());
            }
        });
        Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(xContentMap.size(), is(response.getPolicies().size()));
        for (LifecyclePolicyResponseItem policy : response.getPolicies()) {
            Map<String, Object> policyAsXContent = (Map<String, Object>) xContentMap.get(policy.getLifecyclePolicy().getName());
            assertThat(policyAsXContent, notNullValue());
            assertThat(policyAsXContent.get("version"), is(policy.getVersion()));
            assertThat(policyAsXContent.get("modified_date"), is(policy.getModifiedDate()));
            assertThat(policyAsXContent.get("policy"), notNullValue());
        }
    }

    public void testChunkCount() {
        Response response = createTestInstance();
        // we have 2 chunks surrounding the policies - one for { and } respectively
        // we have one chunk / policy
        AbstractChunkedSerializingTestCase.assertChunkCount(response, ignored -> 2 + response.getPolicies().size());
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new),
                new NamedWriteableRegistry.Entry(LifecycleType.class, TestLifecycleType.TYPE, in -> TestLifecycleType.INSTANCE)
            )
        );
    }

    public static ItemUsage randomUsage() {
        return new ItemUsage(randomStringList(), randomStringList(), randomStringList());
    }

    @Nullable
    private static List<String> randomStringList() {
        if (randomBoolean()) {
            return null;
        } else {
            return randomList(0, 1, () -> randomAlphaOfLengthBetween(2, 10));
        }
    }
}
