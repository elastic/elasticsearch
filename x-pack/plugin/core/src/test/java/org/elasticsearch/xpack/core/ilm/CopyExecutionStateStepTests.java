/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionStateTests.createCustomMetadata;
import static org.hamcrest.Matchers.equalTo;

public class CopyExecutionStateStepTests extends AbstractStepTestCase<CopyExecutionStateStep> {
    @Override
    protected CopyExecutionStateStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String shrunkIndexPrefix = randomAlphaOfLength(10);
        return new CopyExecutionStateStep(stepKey, nextStepKey, shrunkIndexPrefix);
    }

    @Override
    protected CopyExecutionStateStep mutateInstance(CopyExecutionStateStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        String shrunkIndexPrefix = instance.getShrunkIndexPrefix();

        switch (between(0, 2)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                shrunkIndexPrefix += randomAlphaOfLength(5);
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        return new CopyExecutionStateStep(key, nextKey, shrunkIndexPrefix);
    }

    @Override
    protected CopyExecutionStateStep copyInstance(CopyExecutionStateStep instance) {
        return new CopyExecutionStateStep(instance.getKey(), instance.getNextStepKey(), instance.getShrunkIndexPrefix());
    }

    public void testPerformAction() {
        CopyExecutionStateStep step = createRandomInstance();
        String indexName = randomAlphaOfLengthBetween(5, 20);
        Map<String, String> customMetadata = createCustomMetadata();

        IndexMetaData originalIndexMetaData = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1,5))
            .numberOfReplicas(randomIntBetween(1,5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, customMetadata)
            .build();
        IndexMetaData shrunkIndexMetaData = IndexMetaData.builder(step.getShrunkIndexPrefix() + indexName)
            .settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1,5))
            .numberOfReplicas(randomIntBetween(1,5))
            .build();
        ClusterState originalClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder()
                .put(originalIndexMetaData, false)
                .put(shrunkIndexMetaData, false))
            .build();

        ClusterState newClusterState = step.performAction(originalIndexMetaData.getIndex(), originalClusterState);

        LifecycleExecutionState oldIndexData = LifecycleExecutionState.fromIndexMetadata(originalIndexMetaData);
        LifecycleExecutionState newIndexData = LifecycleExecutionState
            .fromIndexMetadata(newClusterState.metaData().index(step.getShrunkIndexPrefix() + indexName));

        assertEquals(oldIndexData.getLifecycleDate(), newIndexData.getLifecycleDate());
        assertEquals(oldIndexData.getPhase(), newIndexData.getPhase());
        assertEquals(oldIndexData.getAction(), newIndexData.getAction());
        assertEquals(ShrunkenIndexCheckStep.NAME, newIndexData.getStep());
    }
    public void testPerformActionWithNoTarget() {
        CopyExecutionStateStep step = createRandomInstance();
        String indexName = randomAlphaOfLengthBetween(5, 20);
        Map<String, String> customMetadata = createCustomMetadata();

        IndexMetaData originalIndexMetaData = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1,5))
            .numberOfReplicas(randomIntBetween(1,5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, customMetadata)
            .build();
        ClusterState originalClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder()
                .put(originalIndexMetaData, false))
            .build();

        IllegalStateException e = expectThrows(IllegalStateException.class,
            () -> step.performAction(originalIndexMetaData.getIndex(), originalClusterState));

        assertThat(e.getMessage(), equalTo("unable to copy execution state from [" +
            indexName + "] to [" + step.getShrunkIndexPrefix() + indexName + "] as target index does not exist"));
    }
}
