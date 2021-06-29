/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

public class PhaseTests extends AbstractXContentTestCase<Phase> {
    private String phaseName;

    @Before
    public void setup() {
        phaseName = randomAlphaOfLength(20);
    }

    @Override
    protected Phase createTestInstance() {
        return randomPhase(phaseName);
    }

    static Phase randomPhase(String phaseName) {
        TimeValue after = null;
        if (randomBoolean()) {
            after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
        }
        Map<String, LifecycleAction> actions = Collections.emptyMap();
        if (randomBoolean()) {
            actions = Collections.singletonMap(DeleteAction.NAME, new DeleteAction());
        }
        return new Phase(phaseName, after, actions);
    }

    @Override
    protected Phase doParseInstance(XContentParser parser) {
        return Phase.parse(parser, phaseName);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // actions are plucked from the named registry, and it fails if the action is not in the named registry
        return (field) -> field.equals("actions");
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(CollectionUtils.appendToCopy(ClusterModule.getNamedXWriteables(),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse)));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testDefaultAfter() {
        Phase phase = new Phase(randomAlphaOfLength(20), null, Collections.emptyMap());
        assertEquals(TimeValue.ZERO, phase.getMinimumAge());
    }
}
