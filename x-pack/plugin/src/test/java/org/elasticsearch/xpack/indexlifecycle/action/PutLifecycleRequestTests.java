/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;
import org.elasticsearch.xpack.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.indexlifecycle.Phase;
import org.elasticsearch.xpack.indexlifecycle.TestLifecycleType;
import org.elasticsearch.xpack.indexlifecycle.action.PutLifecycleAction.Request;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PutLifecycleRequestTests extends AbstractStreamableXContentTestCase<PutLifecycleAction.Request> {
    
    private String lifecycleName;

    @Before
    public void setup() {
        lifecycleName = randomAlphaOfLength(20); // NORELEASE we need to randomise the lifecycle name rather 
                                                 // than use the same name for all instances
    }

    @Override
    protected Request createTestInstance() {
        return new Request(new LifecyclePolicy(TestLifecycleType.INSTANCE, lifecycleName, Collections.emptyMap()));
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return PutLifecycleAction.Request.parseRequest(lifecycleName, parser);
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                        new NamedWriteableRegistry.Entry(LifecycleType.class, TestLifecycleType.TYPE, in -> TestLifecycleType.INSTANCE)));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        entries.add(new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TestLifecycleType.TYPE),
                (p) -> TestLifecycleType.INSTANCE));
        return new NamedXContentRegistry(entries);
    }

    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected MutateFunction<Request> getMutateFunction() {
        return resp -> {
            LifecyclePolicy policy = resp.getPolicy();
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
            return new Request(new LifecyclePolicy(TestLifecycleType.INSTANCE, name, phases));
        };
    }

}
