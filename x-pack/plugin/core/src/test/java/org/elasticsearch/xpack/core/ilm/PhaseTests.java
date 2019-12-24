/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.ParseField;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PhaseTests extends AbstractSerializingTestCase<Phase> {
    private String phaseName;

    @Before
    public void setup() {
        phaseName = randomAlphaOfLength(20);
    }

    @Override
    protected Phase createTestInstance() {
        return randomTestPhase(phaseName);
    }

    static Phase randomTestPhase(String phaseName) {
        TimeValue after = null;
        if (randomBoolean()) {
            after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
        }
        Map<String, LifecycleAction> actions = Collections.emptyMap();
        if (randomBoolean()) {
            actions = Collections.singletonMap(MockAction.NAME, new MockAction());
        }
        return new Phase(phaseName, after, actions);
    }

    @Override
    protected Phase doParseInstance(XContentParser parser) throws IOException {
        return Phase.parse(parser, phaseName);
    }

    @Override
    protected Reader<Phase> instanceReader() {
        return Phase::new;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays
                .asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new)));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MockAction.NAME), MockAction::parse));
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected Phase mutateInstance(Phase instance) throws IOException {
        String name = instance.getName();
        TimeValue after = instance.getMinimumAge();
        Map<String, LifecycleAction> actions = instance.getActions();
        switch (between(0, 2)) {
        case 0:
            name = name + randomAlphaOfLengthBetween(1, 5);
            break;
        case 1:
            after = TimeValue.timeValueSeconds(after.getSeconds() + randomIntBetween(1, 1000));
            break;
        case 2:
            actions = new HashMap<>(actions);
            actions.put(MockAction.NAME + "another", new MockAction(Collections.emptyList()));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new Phase(name, after, actions);
    }

    public void testDefaultAfter() {
        Phase phase = new Phase(randomAlphaOfLength(20), null, Collections.emptyMap());
        assertEquals(TimeValue.ZERO, phase.getMinimumAge());
    }
}
