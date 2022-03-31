/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;

import static org.hamcrest.Matchers.is;

public class CopySettingsStepTests extends AbstractStepTestCase<CopySettingsStep> {

    @Override
    protected CopySettingsStep createRandomInstance() {
        return new CopySettingsStep(
            randomStepKey(),
            randomStepKey(),
            randomAlphaOfLengthBetween(1, 10),
            IndexMetadata.SETTING_NUMBER_OF_SHARDS
        );
    }

    @Override
    protected CopySettingsStep mutateInstance(CopySettingsStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        String indexPrefix = instance.getIndexPrefix();
        String[] settingsKeys = instance.getSettingsKeys();

        switch (between(0, 3)) {
            case 0 -> key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 2 -> indexPrefix = randomValueOtherThan(indexPrefix, () -> randomAlphaOfLengthBetween(1, 10));
            case 3 -> settingsKeys = new String[] { randomAlphaOfLengthBetween(1, 10) };
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new CopySettingsStep(key, nextKey, indexPrefix, settingsKeys);
    }

    @Override
    protected CopySettingsStep copyInstance(CopySettingsStep instance) {
        return new CopySettingsStep(instance.getKey(), instance.getNextStepKey(), instance.getIndexPrefix(), instance.getSettingsKeys());
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        IndexMetadata.Builder sourceIndexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));

        String indexPrefix = "test-prefix-";
        String targetIndex = indexPrefix + indexName;

        IndexMetadata.Builder targetIndexMetadataBuilder = IndexMetadata.builder(targetIndex)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));

        final IndexMetadata sourceIndexMetadata = sourceIndexMetadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(sourceIndexMetadata, false).put(targetIndexMetadataBuilder).build())
            .build();

        CopySettingsStep copySettingsStep = new CopySettingsStep(
            randomStepKey(),
            randomStepKey(),
            indexPrefix,
            LifecycleSettings.LIFECYCLE_NAME
        );

        ClusterState newClusterState = copySettingsStep.performAction(sourceIndexMetadata.getIndex(), clusterState);
        IndexMetadata newTargetIndexMetadata = newClusterState.metadata().index(targetIndex);
        assertThat(newTargetIndexMetadata.getLifecyclePolicyName(), is(policyName));
    }
}
