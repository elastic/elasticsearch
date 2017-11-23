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

public class PhaseTests extends AbstractSerializingTestCase<Phase> {
    
    private NamedXContentRegistry registry;
    private String phaseName;

    @Before
    public void setup() {
        List<NamedXContentRegistry.Entry> entries = Arrays
                .asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        registry = new NamedXContentRegistry(entries);
        phaseName = randomAlphaOfLength(20); // NOCOMMIT we need to randomise the phase name rather 
                                             // than use the same name for all instances
    }

    @Override
    protected Phase createTestInstance() {
        TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
        List<LifecycleAction> actions = new ArrayList<>();
        if (randomBoolean()) {
            actions.add(new DeleteAction());
        }
        return new Phase(phaseName, after, actions);
    }

    @Override
    protected Phase doParseInstance(XContentParser parser) throws IOException {
        
        return Phase.parse(parser, new Tuple<>(phaseName, registry));
    }

    @Override
    protected Reader<Phase> instanceReader() {
        return Phase::new;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays
                .asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new)));
    }

}
