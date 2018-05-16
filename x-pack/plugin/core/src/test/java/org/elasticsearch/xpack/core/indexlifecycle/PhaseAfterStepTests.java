/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;


import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep.Result;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.util.concurrent.TimeUnit;

public class PhaseAfterStepTests extends AbstractStepTestCase<PhaseAfterStep> {

    @Override
    public PhaseAfterStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        TimeValue after = createRandomTimeValue();
        return new PhaseAfterStep(null, after, stepKey, nextStepKey);
    }

    private TimeValue createRandomTimeValue() {
        return new TimeValue(randomLongBetween(1, 10000), randomFrom(TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS));
    }

    @Override
    public PhaseAfterStep mutateInstance(PhaseAfterStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        TimeValue after = instance.getAfter();

        switch (between(0, 2)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 2:
            after = randomValueOtherThan(after, this::createRandomTimeValue);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new PhaseAfterStep(instance.getNowSupplier(), after, key, nextKey);
    }

    @Override
    public PhaseAfterStep copyInstance(PhaseAfterStep instance) {
        return new PhaseAfterStep(instance.getNowSupplier(), instance.getAfter(),
            instance.getKey(), instance.getNextStepKey());
    }

    public void testConditionMet() {
        long creationDate = randomNonNegativeLong();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, creationDate))
            .creationDate(creationDate)
            .numberOfShards(1).numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        long after = randomNonNegativeLong();
        long now = creationDate + after + randomIntBetween(0, 2);
        PhaseAfterStep step = new PhaseAfterStep(() -> now, TimeValue.timeValueMillis(after), null, null);
        Result result = step.isConditionMet(index, clusterState);
        assertTrue(result.isComplete());
        assertNull(result.getInfomationContext());
    }

    public void testConditionNotMet() {
        long creationDate = randomNonNegativeLong();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, creationDate))
            .creationDate(creationDate)
            .numberOfShards(1).numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        long after = randomNonNegativeLong();
        long now = creationDate + after - randomIntBetween(1, 1000);
        PhaseAfterStep step = new PhaseAfterStep(() -> now, TimeValue.timeValueMillis(after), null, null);
        Result result = step.isConditionMet(index, clusterState);
        assertFalse(result.isComplete());
        assertNull(result.getInfomationContext());
    }
}
