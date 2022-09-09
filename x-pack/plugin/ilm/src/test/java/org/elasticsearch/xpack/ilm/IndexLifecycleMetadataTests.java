/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Custom;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.SimpleDiffableSerializationTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata.IndexLifecycleMetadataDiff;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils.newTestLifecyclePolicy;
import static org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils.randomTimeseriesLifecyclePolicy;

public class IndexLifecycleMetadataTests extends SimpleDiffableSerializationTestCase<Custom> {

    @Override
    protected IndexLifecycleMetadata createTestInstance() {
        int numPolicies = randomIntBetween(1, 5);
        Map<String, LifecyclePolicyMetadata> policies = Maps.newMapWithExpectedSize(numPolicies);
        for (int i = 0; i < numPolicies; i++) {
            LifecyclePolicy policy = randomTimeseriesLifecyclePolicy(randomAlphaOfLength(4) + i);
            policies.put(
                policy.getName(),
                new LifecyclePolicyMetadata(policy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
            );
        }
        return new IndexLifecycleMetadata(policies, randomFrom(OperationMode.values()));
    }

    @Override
    protected IndexLifecycleMetadata doParseInstance(XContentParser parser) throws IOException {
        return IndexLifecycleMetadata.PARSER.apply(parser, null);
    }

    @Override
    protected Reader<Metadata.Custom> instanceReader() {
        return IndexLifecycleMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(
                    LifecycleType.class,
                    TimeseriesLifecycleType.TYPE,
                    (in) -> TimeseriesLifecycleType.INSTANCE
                ),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, WaitForSnapshotAction.NAME, WaitForSnapshotAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::readFrom),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ReadOnlyAction.NAME, ReadOnlyAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, FreezeAction.NAME, in -> FreezeAction.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, SetPriorityAction.NAME, SetPriorityAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, UnfollowAction.NAME, in -> UnfollowAction.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, MigrateAction.NAME, MigrateAction::readFrom),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, SearchableSnapshotAction.NAME, SearchableSnapshotAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DownsampleAction.NAME, DownsampleAction::new)
            )
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    LifecycleType.class,
                    new ParseField(TimeseriesLifecycleType.TYPE),
                    (p) -> TimeseriesLifecycleType.INSTANCE
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(WaitForSnapshotAction.NAME),
                    WaitForSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MigrateAction.NAME), MigrateAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(SearchableSnapshotAction.NAME),
                    SearchableSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DownsampleAction.NAME), DownsampleAction::parse)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected Metadata.Custom mutateInstance(Metadata.Custom instance) {
        IndexLifecycleMetadata metadata = (IndexLifecycleMetadata) instance;
        Map<String, LifecyclePolicyMetadata> policies = metadata.getPolicyMetadatas();
        policies = new TreeMap<>(policies);
        OperationMode mode = metadata.getOperationMode();
        if (randomBoolean()) {
            String policyName = randomAlphaOfLength(10);
            policies.put(
                policyName,
                new LifecyclePolicyMetadata(
                    randomTimeseriesLifecyclePolicy(policyName),
                    Collections.emptyMap(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            );
        } else {
            mode = randomValueOtherThan(metadata.getOperationMode(), () -> randomFrom(OperationMode.values()));
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
        Version min = createTestInstance().getMinimalSupportedVersion();
        assertTrue(min.onOrBefore(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT)));
    }

    public void testcontext() {
        assertEquals(Metadata.ALL_CONTEXTS, createTestInstance().context());
    }

    public static IndexLifecycleMetadata createTestInstance(int numPolicies, OperationMode mode) {
        SortedMap<String, LifecyclePolicyMetadata> policies = new TreeMap<>();
        for (int i = 0; i < numPolicies; i++) {
            int numberPhases = randomInt(5);
            Map<String, Phase> phases = Maps.newMapWithExpectedSize(numberPhases);
            for (int j = 0; j < numberPhases; j++) {
                TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
                Map<String, LifecycleAction> actions = Collections.emptyMap();
                if (randomBoolean()) {
                    actions = Collections.singletonMap(DeleteAction.NAME, DeleteAction.WITH_SNAPSHOT_DELETE);
                }
                String phaseName = randomAlphaOfLength(10);
                phases.put(phaseName, new Phase(phaseName, after, actions));
            }
            String policyName = randomAlphaOfLength(10);
            policies.put(
                policyName,
                new LifecyclePolicyMetadata(
                    newTestLifecyclePolicy(policyName, phases),
                    Collections.emptyMap(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            );
        }
        return new IndexLifecycleMetadata(policies, mode);
    }
}
