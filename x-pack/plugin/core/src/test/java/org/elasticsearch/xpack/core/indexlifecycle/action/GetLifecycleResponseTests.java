/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.TestLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction.Response;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GetLifecycleResponseTests extends AbstractStreamableTestCase<GetLifecycleAction.Response> {
    
    private String lifecycleName;

    @Before
    public void setup() {
        lifecycleName = randomAlphaOfLength(20); // NORELEASE we need to randomise the lifecycle name rather 
                                                 // than use the same name for all instances
    }

    @Override
    protected Response createTestInstance() {
        return new Response(new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, Collections.emptyMap()));
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                        new NamedWriteableRegistry.Entry(LifecycleType.class, TestLifecycleType.TYPE, in -> TestLifecycleType.INSTANCE)));
    }

    @Override
    protected Response mutateInstance(Response response) {
        LifecyclePolicy policy = response.getPolicy();
        String name = policy.getName();
        Map<String, Phase> phases = policy.getPhases();
        switch (between(0, 1)) {
            case 0:
                name = name + randomAlphaOfLengthBetween(1, 5);
                break;
            case 1:
                phases = new HashMap<>(phases);
                String newPhaseName = randomAlphaOfLengthBetween(1, 10);
                phases.put(name, new Phase(newPhaseName, TimeValue.timeValueSeconds(randomIntBetween(1, 1000)),
                    Collections.emptyMap()));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new Response(new LifecyclePolicy(TestLifecycleType.INSTANCE, name, phases));
    }
}
