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
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState.Builder;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.time.DateFormatter;

import java.util.function.BiFunction;

import static org.elasticsearch.common.IndexNameGenerator.generateValidIndexName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class GenerateUniqueIndexNameStepTests extends AbstractStepTestCase<GenerateUniqueIndexNameStep> {

    @Override
    protected GenerateUniqueIndexNameStep createRandomInstance() {
        return new GenerateUniqueIndexNameStep(randomStepKey(), randomStepKey(), randomAlphaOfLengthBetween(5, 10), lifecycleStateSetter());
    }

    private static BiFunction<String, Builder, Builder> lifecycleStateSetter() {
        return (generatedIndexName, lifecycleStateBuilder) -> lifecycleStateBuilder.setShrinkIndexName(generatedIndexName);
    }

    @Override
    protected GenerateUniqueIndexNameStep mutateInstance(GenerateUniqueIndexNameStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        String prefix = instance.prefix();

        switch (between(0, 2)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> prefix = randomValueOtherThan(prefix, () -> randomAlphaOfLengthBetween(5, 10));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new GenerateUniqueIndexNameStep(key, nextKey, prefix, lifecycleStateSetter());
    }

    @Override
    protected GenerateUniqueIndexNameStep copyInstance(GenerateUniqueIndexNameStep instance) {
        return new GenerateUniqueIndexNameStep(
            instance.getKey(),
            instance.getNextStepKey(),
            instance.prefix(),
            instance.lifecycleStateSetter()
        );
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));

        final IndexMetadata indexMetadata = indexMetadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .build();

        GenerateUniqueIndexNameStep generateUniqueIndexNameStep = createRandomInstance();
        ClusterState newClusterState = generateUniqueIndexNameStep.performAction(indexMetadata.getIndex(), clusterState);

        LifecycleExecutionState executionState = newClusterState.metadata().index(indexName).getLifecycleExecutionState();
        assertThat(
            "the " + GenerateUniqueIndexNameStep.NAME + " step must generate an index name",
            executionState.shrinkIndexName(),
            notNullValue()
        );
        assertThat(executionState.shrinkIndexName(), containsString(indexName));
        assertThat(executionState.shrinkIndexName(), startsWith(generateUniqueIndexNameStep.prefix()));
    }

    public void testParseOriginationDateFromGeneratedIndexName() {
        String indexName = "testIndex-2021.03.13-000001";
        String generateValidIndexName = generateValidIndexName("shrink-", indexName);
        long extractedDateMillis = IndexLifecycleOriginationDateParser.parseIndexNameAndExtractDate(generateValidIndexName);
        assertThat(extractedDateMillis, is(DateFormatter.forPattern("uuuu.MM.dd").parseMillis("2021.03.13")));
    }
}
