/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;

import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class GenerateDownsampleIndexNameStepTests extends AbstractStepTestCase<GenerateDownsampleIndexNameStep> {
    @Override
    protected GenerateDownsampleIndexNameStep createRandomInstance() {
        return new GenerateDownsampleIndexNameStep(
            randomStepKey(),
            randomStepKey(),
            randomAlphaOfLength(5),
            ConfigTestHelpers.randomInterval(),
            lifecycleStateSetter()
        );
    }

    private static BiFunction<String, LifecycleExecutionState.Builder, LifecycleExecutionState.Builder> lifecycleStateSetter() {
        return (generatedIndexName, lifecycleStateBuilder) -> lifecycleStateBuilder.setShrinkIndexName(generatedIndexName);
    }

    @Override
    protected GenerateDownsampleIndexNameStep mutateInstance(GenerateDownsampleIndexNameStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        String prefix = instance.prefix();
        DateHistogramInterval interval = instance.getInterval();

        switch (between(0, 3)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> prefix = randomValueOtherThan(prefix, () -> randomAlphaOfLengthBetween(5, 10));
            case 3 -> interval = randomValueOtherThan(interval, ConfigTestHelpers::randomInterval);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new GenerateDownsampleIndexNameStep(key, nextKey, prefix, interval, lifecycleStateSetter());
    }

    @Override
    protected GenerateDownsampleIndexNameStep copyInstance(GenerateDownsampleIndexNameStep instance) {
        return new GenerateDownsampleIndexNameStep(
            instance.getKey(),
            instance.getNextStepKey(),
            instance.prefix(),
            instance.getInterval(),
            instance.lifecycleStateSetter()
        );
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));

        final IndexMetadata indexMetadata = indexMetadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .build();

        GenerateDownsampleIndexNameStep generateDownsampleIndexNameStep = createRandomInstance();
        ClusterState newClusterState = generateDownsampleIndexNameStep.performAction(indexMetadata.getIndex(), clusterState);

        LifecycleExecutionState executionState = newClusterState.metadata().index(indexName).getLifecycleExecutionState();
        assertThat(
            "the " + GenerateDownsampleIndexNameStep.NAME + " step must generate a predictable name",
            executionState.shrinkIndexName(),
            notNullValue()
        );
        assertThat(
            executionState.shrinkIndexName(),
            equalTo(generateDownsampleIndexNameStep.prefix() + "-" + indexName + "-" + generateDownsampleIndexNameStep.getInterval())
        );
    }
}
