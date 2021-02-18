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

import java.util.Locale;

import static org.elasticsearch.xpack.core.ilm.AbstractStepMasterTimeoutTestCase.emptyClusterState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class GenerateRollupIndexNameStepTests extends AbstractStepTestCase<GenerateRollupIndexNameStep> {

    @Override
    protected GenerateRollupIndexNameStep createRandomInstance() {
        return new GenerateRollupIndexNameStep(randomStepKey(), randomStepKey());
    }

    @Override
    protected GenerateRollupIndexNameStep mutateInstance(GenerateRollupIndexNameStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new GenerateRollupIndexNameStep(key, nextKey);
    }

    @Override
    protected GenerateRollupIndexNameStep copyInstance(GenerateRollupIndexNameStep instance) {
        return new GenerateRollupIndexNameStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        String policyName = "test-ilm-policy";
        IndexMetadata.Builder indexMetadataBuilder =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));

        final IndexMetadata indexMetadata = indexMetadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
                .metadata(Metadata.builder().put(indexMetadata, false).build()).build();

        GenerateRollupIndexNameStep generateRollupIndexNameStep = createRandomInstance();
        ClusterState newClusterState = generateRollupIndexNameStep.performAction(indexMetadata.getIndex(), clusterState);

        LifecycleExecutionState executionState = LifecycleExecutionState.fromIndexMetadata(newClusterState.metadata().index(indexName));
        assertThat("the " + GenerateRollupIndexNameStep.NAME + " step must generate a rollup index name",
            executionState.getRollupIndexName(), notNullValue());
        assertThat(executionState.getRollupIndexName(), containsString("rollup-" + indexName + "-"));
    }

    public void testGenerateRollupIndexName() {
        String indexName = randomAlphaOfLength(5);
        String generated = GenerateRollupIndexNameStep.generateRollupIndexName(indexName);
        assertThat(generated, startsWith("rollup-" + indexName + "-"));
    }
}
