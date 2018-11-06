/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.MockAction;
import org.elasticsearch.xpack.core.indexlifecycle.TestLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction.LifecyclePolicyResponseItem;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyTests.randomTestLifecyclePolicy;

public class GetLifecycleResponseTests extends AbstractStreamableTestCase<GetLifecycleAction.Response> {

    @Override
    protected Response createTestInstance() {
        String randomPrefix = randomAlphaOfLength(5);
        List<LifecyclePolicyResponseItem> responseItems = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 2); i++) {
            responseItems.add(new LifecyclePolicyResponseItem(randomTestLifecyclePolicy(randomPrefix + i),
                randomNonNegativeLong(), randomAlphaOfLength(8)));
        }
        return new Response(responseItems);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new),
                        new NamedWriteableRegistry.Entry(LifecycleType.class, TestLifecycleType.TYPE, in -> TestLifecycleType.INSTANCE)));
    }

    @Override
    protected Response mutateInstance(Response response) {
        List<LifecyclePolicyResponseItem> responseItems = new ArrayList<>(response.getPolicies());
        if (responseItems.size() > 0) {
            if (randomBoolean()) {
                responseItems.add(new LifecyclePolicyResponseItem(randomTestLifecyclePolicy(randomAlphaOfLength(5)),
                    randomNonNegativeLong(), randomAlphaOfLength(4)));
            } else {
                responseItems.remove(0);
            }
        } else {
            responseItems.add(new LifecyclePolicyResponseItem(randomTestLifecyclePolicy(randomAlphaOfLength(2)),
                randomNonNegativeLong(), randomAlphaOfLength(4)));
        }
        return new Response(responseItems);
    }
}
