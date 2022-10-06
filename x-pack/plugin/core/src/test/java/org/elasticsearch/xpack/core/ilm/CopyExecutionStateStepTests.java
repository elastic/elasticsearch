/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionStateTests.createCustomMetadata;
import static org.hamcrest.Matchers.equalTo;

public class CopyExecutionStateStepTests extends AbstractStepTestCase<CopyExecutionStateStep> {
    @Override
    protected CopyExecutionStateStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String shrunkIndexPrefix = randomAlphaOfLength(10);
        StepKey targetNextStepKey = randomStepKey();
        return new CopyExecutionStateStep(stepKey, nextStepKey, (index, state) -> shrunkIndexPrefix + index, targetNextStepKey);
    }

    @Override
    protected CopyExecutionStateStep mutateInstance(CopyExecutionStateStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        BiFunction<String, LifecycleExecutionState, String> indexNameSupplier = instance.getTargetIndexNameSupplier();
        StepKey targetNextStepKey = instance.getTargetNextStepKey();

        switch (between(0, 3)) {
            case 0 -> key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.getPhase(), nextKey.getAction(), nextKey.getName() + randomAlphaOfLength(5));
            case 2 -> indexNameSupplier = (index, state) -> randomAlphaOfLengthBetween(11, 15) + index;
            case 3 -> targetNextStepKey = new StepKey(
                targetNextStepKey.getPhase(),
                targetNextStepKey.getAction(),
                targetNextStepKey.getName() + randomAlphaOfLength(5)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CopyExecutionStateStep(key, nextKey, indexNameSupplier, targetNextStepKey);
    }

    @Override
    protected CopyExecutionStateStep copyInstance(CopyExecutionStateStep instance) {
        return new CopyExecutionStateStep(
            instance.getKey(),
            instance.getNextStepKey(),
            instance.getTargetIndexNameSupplier(),
            instance.getTargetNextStepKey()
        );
    }

    public void testPerformAction() {
        CopyExecutionStateStep step = createRandomInstance();
        String indexName = randomAlphaOfLengthBetween(5, 20);
        Map<String, String> customMetadata = createCustomMetadata();

        IndexMetadata originalIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(1, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, customMetadata)
            .build();
        IndexMetadata shrunkIndexMetadata = IndexMetadata.builder(
            step.getTargetIndexNameSupplier().apply(indexName, LifecycleExecutionState.builder().build())
        ).settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(1, 5)).build();
        ClusterState originalClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(originalIndexMetadata, false).put(shrunkIndexMetadata, false))
            .build();

        ClusterState newClusterState = step.performAction(originalIndexMetadata.getIndex(), originalClusterState);

        LifecycleExecutionState oldIndexData = originalIndexMetadata.getLifecycleExecutionState();
        LifecycleExecutionState newIndexData = newClusterState.metadata()
            .index(step.getTargetIndexNameSupplier().apply(indexName, LifecycleExecutionState.builder().build()))
            .getLifecycleExecutionState();

        StepKey targetNextStepKey = step.getTargetNextStepKey();
        assertEquals(newIndexData.lifecycleDate(), oldIndexData.lifecycleDate());
        assertEquals(newIndexData.phase(), targetNextStepKey.getPhase());
        assertEquals(newIndexData.action(), targetNextStepKey.getAction());
        assertEquals(newIndexData.step(), targetNextStepKey.getName());
        assertEquals(newIndexData.snapshotRepository(), oldIndexData.snapshotRepository());
        assertEquals(newIndexData.snapshotName(), oldIndexData.snapshotName());
    }

    public void testAllStateCopied() {
        CopyExecutionStateStep step = createRandomInstance();
        String indexName = randomAlphaOfLengthBetween(5, 20);

        IndexMetadata originalIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(1, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, createCustomMetadata())
            .build();
        IndexMetadata shrunkIndexMetadata = IndexMetadata.builder(
            step.getTargetIndexNameSupplier().apply(indexName, LifecycleExecutionState.builder().build())
        ).settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(1, 5)).build();

        ClusterState originalClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(originalIndexMetadata, false).put(shrunkIndexMetadata, false))
            .build();

        ClusterState newClusterState = step.performAction(originalIndexMetadata.getIndex(), originalClusterState);

        LifecycleExecutionState oldIndexData = originalIndexMetadata.getLifecycleExecutionState();
        LifecycleExecutionState newIndexData = newClusterState.metadata()
            .index(step.getTargetIndexNameSupplier().apply(indexName, LifecycleExecutionState.builder().build()))
            .getLifecycleExecutionState();

        Map<String, String> beforeMap = new HashMap<>(oldIndexData.asMap());
        // The target step key's StepKey is used in the new metadata, so update the "before" map with the new info so it can be compared
        beforeMap.put("phase", step.getTargetNextStepKey().getPhase());
        beforeMap.put("action", step.getTargetNextStepKey().getAction());
        beforeMap.put("step", step.getTargetNextStepKey().getName());
        Map<String, String> newMap = newIndexData.asMap();
        assertThat(beforeMap, equalTo(newMap));
    }

    public void testPerformActionWithNoTarget() {
        CopyExecutionStateStep step = createRandomInstance();
        String indexName = randomAlphaOfLengthBetween(5, 20);
        Map<String, String> customMetadata = createCustomMetadata();

        IndexMetadata originalIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(1, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, customMetadata)
            .build();
        ClusterState originalClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(originalIndexMetadata, false))
            .build();

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> step.performAction(originalIndexMetadata.getIndex(), originalClusterState)
        );

        assertThat(
            e.getMessage(),
            equalTo(
                "unable to copy execution state from ["
                    + indexName
                    + "] to ["
                    + step.getTargetIndexNameSupplier()
                        .apply(originalIndexMetadata.getIndex().getName(), LifecycleExecutionState.builder().build())
                    + "] as target index does not exist"
            )
        );
    }
}
