/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.MockAction;
import org.elasticsearch.xpack.core.indexlifecycle.TestLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction.Response;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyTests.randomTestLifecyclePolicy;

public class GetLifecycleResponseTests extends AbstractStreamableTestCase<GetLifecycleAction.Response> {

    @Override
    protected Response createTestInstance() {
        String randomPrefix = randomAlphaOfLength(5);
        Map<LifecyclePolicy, Tuple<Long, String>> policyMap = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 2); i++) {
            policyMap.put(randomTestLifecyclePolicy(randomPrefix + i),
                new Tuple<>(randomNonNegativeLong(), randomAlphaOfLength(8)));
        }
        return new Response(policyMap);
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
        Map<LifecyclePolicy, Tuple<Long, String>> policyMap = new HashMap<>(response.getPoliciesToMetadata());
        if (policyMap.size() > 0) {
            if (randomBoolean()) {
                policyMap.put(randomTestLifecyclePolicy(randomAlphaOfLength(5)),
                    new Tuple<>(randomNonNegativeLong(), randomAlphaOfLength(4)));
            } else {
                policyMap.remove(randomFrom(response.getPoliciesToMetadata().keySet()));
            }
        } else {
            policyMap.put(randomTestLifecyclePolicy(randomAlphaOfLength(2)), new Tuple<>(randomNonNegativeLong(), randomAlphaOfLength(4)));
        }
        return new Response(policyMap);
    }
}
