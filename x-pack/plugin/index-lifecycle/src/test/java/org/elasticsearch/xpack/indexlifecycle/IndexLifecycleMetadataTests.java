/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
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
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata.IndexLifecycleMetadataDiff;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.TestLifecycleType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class IndexLifecycleMetadataTests extends AbstractDiffableSerializationTestCase<MetaData.Custom> {

    @Override
    protected IndexLifecycleMetadata createTestInstance() {
        int numPolicies = randomInt(5);
        SortedMap<String, LifecyclePolicyMetadata> policies = new TreeMap<>();
        for (int i = 0; i < numPolicies; i++) {
            int numberPhases = randomInt(5);
            Map<String, Phase> phases = new HashMap<>(numberPhases);
            for (int j = 0; j < numberPhases; j++) {
                TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
                Map<String, LifecycleAction> actions = Collections.emptyMap();
                if (randomBoolean()) {
                    actions = Collections.singletonMap(DeleteAction.NAME, new DeleteAction());
                }
                String phaseName = randomAlphaOfLength(10);
                phases.put(phaseName, new Phase(phaseName, after, actions));
            }
            String policyName = randomAlphaOfLength(10);
            policies.put(policyName, new LifecyclePolicyMetadata(new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName, phases),
                    Collections.emptyMap()));
        }
        return new IndexLifecycleMetadata(policies, randomFrom(OperationMode.values()));
    }

    @Override
    protected IndexLifecycleMetadata doParseInstance(XContentParser parser) throws IOException {
        return IndexLifecycleMetadata.PARSER.apply(parser, null);
    }

    @Override
    protected Reader<MetaData.Custom> instanceReader() {
        return IndexLifecycleMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                        new NamedWriteableRegistry.Entry(LifecycleType.class, TestLifecycleType.TYPE, (in) -> TestLifecycleType.INSTANCE)));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        entries.add(new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TestLifecycleType.TYPE),
                (p) -> TestLifecycleType.INSTANCE));
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected MetaData.Custom mutateInstance(MetaData.Custom instance) {
        IndexLifecycleMetadata metadata = (IndexLifecycleMetadata) instance;
        Map<String, LifecyclePolicyMetadata> policies = metadata.getPolicyMetadatas();
        policies = new TreeMap<>(policies);
        OperationMode mode = metadata.getMaintenanceMode();
        if (randomBoolean()) {
            String policyName = randomAlphaOfLength(10);
            policies.put(policyName, new LifecyclePolicyMetadata(
                new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName, Collections.emptyMap()), Collections.emptyMap()));
        } else {
            mode = randomValueOtherThan(metadata.getMaintenanceMode(), () -> randomFrom(OperationMode.values()));
        }
        return new IndexLifecycleMetadata(policies, mode);
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
