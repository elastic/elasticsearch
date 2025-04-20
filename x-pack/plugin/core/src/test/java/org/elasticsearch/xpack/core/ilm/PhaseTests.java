/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PhaseTests extends AbstractXContentSerializingTestCase<Phase> {
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
            after = randomTimeValue(0, 1_000_000_000, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS);
        }
        Map<String, LifecycleAction> actions = Map.of();
        if (randomBoolean()) {
            actions = Map.of(MockAction.NAME, new MockAction());
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
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new))
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            CollectionUtils.appendToCopy(
                ClusterModule.getNamedXWriteables(),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MockAction.NAME), MockAction::parse)
            )
        );
    }

    @Override
    protected Phase mutateInstance(Phase instance) {
        String name = instance.getName();
        TimeValue after = instance.getMinimumAge();
        Map<String, LifecycleAction> actions = instance.getActions();
        switch (between(0, 2)) {
            case 0 -> name = name + randomAlphaOfLengthBetween(1, 5);
            case 1 -> after = TimeValue.timeValueSeconds(after.getSeconds() + randomIntBetween(1, 1000));
            case 2 -> {
                actions = new HashMap<>(actions);
                actions.put(MockAction.NAME + "another", new MockAction(List.of()));
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new Phase(name, after, actions);
    }

    public void testDefaultAfter() {
        Phase phase = new Phase(randomAlphaOfLength(20), null, Map.of());
        assertEquals(TimeValue.ZERO, phase.getMinimumAge());
    }
}
