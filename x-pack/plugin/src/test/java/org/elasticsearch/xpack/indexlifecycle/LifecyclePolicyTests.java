/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LifecyclePolicyTests extends AbstractSerializingTestCase<LifecyclePolicy> {
    
    private NamedXContentRegistry registry;
    private String lifecycleName;

    @Before
    public void setup() {
        List<NamedXContentRegistry.Entry> entries = Arrays
                .asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        registry = new NamedXContentRegistry(entries);
        lifecycleName = randomAlphaOfLength(20); // NOCOMMIT we need to randomise the lifecycle name rather 
                                                 // than use the same name for all instances
    }

    @Override
    protected LifecyclePolicy createTestInstance() {
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
        return new LifecyclePolicy(lifecycleName, phases);
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] { "phases" }; // NOCOMMIT this needs to be temporary since we should not rely on the order of the JSON map
    }

    @Override
    protected LifecyclePolicy doParseInstance(XContentParser parser) throws IOException {
        return LifecyclePolicy.parse(parser, new Tuple<>(lifecycleName, registry));
    }

    @Override
    protected Reader<LifecyclePolicy> instanceReader() {
        return LifecyclePolicy::new;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new)));
    }

}
