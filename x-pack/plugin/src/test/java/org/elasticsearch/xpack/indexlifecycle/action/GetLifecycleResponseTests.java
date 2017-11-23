/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.indexlifecycle.Phase;
import org.elasticsearch.xpack.indexlifecycle.action.GetLifecycleAction.Response;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetLifecycleResponseTests extends AbstractStreamableTestCase<GetLifecycleAction.Response> {
    
    private String lifecycleName;

    @Before
    public void setup() {
        lifecycleName = randomAlphaOfLength(20); // NOCOMMIT we need to randomise the lifecycle name rather 
                                                 // than use the same name for all instances
    }

    @Override
    protected Response createTestInstance() {
        int numberPhases = randomInt(5);
        List<Phase> phases = new ArrayList<>(numberPhases);
        for (int i = 0; i < numberPhases; i++) {
            TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
            List<LifecycleAction> actions = new ArrayList<>();
            if (randomBoolean()) {
                actions.add(new DeleteAction());
            }
            phases.add(new Phase(randomAlphaOfLength(10), after, actions));
        }
        return new Response(new LifecyclePolicy(lifecycleName, phases));
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new)));
    }

}
