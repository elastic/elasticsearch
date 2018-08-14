/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LifecyclePolicyMetadataTests extends AbstractSerializingTestCase<LifecyclePolicyMetadata> {

    private String lifecycleName;

    @Before
    public void setup() {
        lifecycleName = randomAlphaOfLength(20);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new),
                        new NamedWriteableRegistry.Entry(LifecycleType.class, TestLifecycleType.TYPE, (in) -> TestLifecycleType.INSTANCE)));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MockAction.NAME), MockAction::parse));
        entries.add(new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TestLifecycleType.TYPE),
                (p) -> TestLifecycleType.INSTANCE));
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected LifecyclePolicyMetadata doParseInstance(XContentParser parser) throws IOException {
        return LifecyclePolicyMetadata.parse(parser, lifecycleName);
    }

    @Override
    protected LifecyclePolicyMetadata createTestInstance() {
        Map<String, String> headers = new HashMap<>();
        int numberHeaders = between(0, 10);
        for (int i = 0; i < numberHeaders; i++) {
            headers.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return new LifecyclePolicyMetadata(LifecyclePolicyTests.randomTestLifecyclePolicy(lifecycleName), headers);
    }

    @Override
    protected Reader<LifecyclePolicyMetadata> instanceReader() {
        return LifecyclePolicyMetadata::new;
    }

    @Override
    protected LifecyclePolicyMetadata mutateInstance(LifecyclePolicyMetadata instance) throws IOException {
        LifecyclePolicy policy = instance.getPolicy();
        Map<String, String> headers = instance.getHeaders();
        switch (between(0, 1)) {
        case 0:
            policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, policy.getName() + randomAlphaOfLengthBetween(1, 5),
                    policy.getPhases());
            break;
        case 1:
            headers = new HashMap<>(headers);
            headers.put(randomAlphaOfLength(11), randomAlphaOfLength(11));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new LifecyclePolicyMetadata(policy, headers);
    }

}
