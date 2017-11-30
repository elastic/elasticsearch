/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleMetadata.IndexLifecycleMetadataDiff;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class IndexLifecycleMetadataTests extends AbstractDiffableSerializationTestCase<MetaData.Custom> {

    private NamedXContentRegistry registry;

    @Before
    public void setup() {
        List<NamedXContentRegistry.Entry> entries = Arrays
            .asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
                new NamedXContentRegistry.Entry(LifecyclePolicy.class, new ParseField("test"), TestLifecyclePolicy::parse));
        registry = new NamedXContentRegistry(entries);
    }

    @Override
    protected IndexLifecycleMetadata createTestInstance() {
        int numPolicies = randomInt(5);
        SortedMap<String, LifecyclePolicy> policies = new TreeMap<>();
        for (int i = 0; i < numPolicies; i++) {
            int numberPhases = randomInt(5);
            List<Phase> phases = new ArrayList<>(numberPhases);
            for (int j = 0; j < numberPhases; j++) {
                TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
                List<LifecycleAction> actions = new ArrayList<>();
                if (randomBoolean()) {
                    actions.add(new DeleteAction());
                }
                phases.add(new Phase(randomAlphaOfLength(10), after, actions));
            }
            String policyName = randomAlphaOfLength(10);
            policies.put(policyName, new TestLifecyclePolicy(policyName, phases));
        }
        long pollInterval = randomNonNegativeLong();
        return new IndexLifecycleMetadata(policies, pollInterval);
    }

    @Override
    protected IndexLifecycleMetadata doParseInstance(XContentParser parser) throws IOException {
        return IndexLifecycleMetadata.PARSER.apply(parser, registry);
    }

    @Override
    protected Reader<MetaData.Custom> instanceReader() {
        return IndexLifecycleMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                    new NamedWriteableRegistry.Entry(LifecyclePolicy.class, TestLifecyclePolicy.TYPE,
                        TestLifecyclePolicy::new)));
    }

    @Override
    protected MetaData.Custom mutateInstance(MetaData.Custom instance) {
        IndexLifecycleMetadata metadata = (IndexLifecycleMetadata) instance;
        SortedMap<String, LifecyclePolicy> policies = metadata.getPolicies();
        long pollInterval = metadata.getPollInterval();
        switch (between(0, 1)) {
        case 0:
            pollInterval = pollInterval + randomIntBetween(1, 1000);
            break;
        case 1:
            policies = new TreeMap<>(policies);
            String policyName = randomAlphaOfLength(10);
            policies.put(policyName, new TestLifecyclePolicy(policyName, Collections.emptyList()));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new IndexLifecycleMetadata(policies, pollInterval);
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected Reader<Diff<Custom>> diffReader() {
        return IndexLifecycleMetadataDiff::new;
    }

    public void testMinimumSupportedVersion() {
        assertEquals(Version.V_7_0_0_alpha1, createTestInstance().getMinimalSupportedVersion());
    }

    public void testcontext() {
        assertEquals(MetaData.ALL_CONTEXTS, createTestInstance().context());
    }

}
