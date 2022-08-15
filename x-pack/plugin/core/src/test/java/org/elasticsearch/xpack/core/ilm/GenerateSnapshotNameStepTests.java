/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.generateSnapshotName;
import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.validateGeneratedSnapshotName;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class GenerateSnapshotNameStepTests extends AbstractStepTestCase<GenerateSnapshotNameStep> {

    @Override
    protected GenerateSnapshotNameStep createRandomInstance() {
        return new GenerateSnapshotNameStep(randomStepKey(), randomStepKey(), randomAlphaOfLengthBetween(5, 10));
    }

    @Override
    protected GenerateSnapshotNameStep mutateInstance(GenerateSnapshotNameStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        String snapshotRepository = instance.getSnapshotRepository();

        switch (between(0, 2)) {
            case 0 -> key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 2 -> snapshotRepository = randomValueOtherThan(snapshotRepository, () -> randomAlphaOfLengthBetween(5, 10));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new GenerateSnapshotNameStep(key, nextKey, snapshotRepository);
    }

    @Override
    protected GenerateSnapshotNameStep copyInstance(GenerateSnapshotNameStep instance) {
        return new GenerateSnapshotNameStep(instance.getKey(), instance.getNextStepKey(), instance.getSnapshotRepository());
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        GenerateSnapshotNameStep generateSnapshotNameStep = createRandomInstance();

        // generate a snapshot repository with the expected name
        RepositoryMetadata repo = new RepositoryMetadata(generateSnapshotNameStep.getSnapshotRepository(), "fs", Settings.EMPTY);

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, false)
                    .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(Collections.singletonList(repo)))
                    .build()
            )
            .build();

        ClusterState newClusterState;

        // the snapshot index name, snapshot repository, and snapshot name are generated as expected
        newClusterState = generateSnapshotNameStep.performAction(indexMetadata.getIndex(), clusterState);
        LifecycleExecutionState executionState = newClusterState.metadata().index(indexName).getLifecycleExecutionState();
        assertThat(executionState.snapshotIndexName(), is(indexName));
        assertThat(
            "the " + GenerateSnapshotNameStep.NAME + " step must generate a snapshot name",
            executionState.snapshotName(),
            notNullValue()
        );
        assertThat(executionState.snapshotRepository(), is(generateSnapshotNameStep.getSnapshotRepository()));
        assertThat(executionState.snapshotName(), containsString(indexName.toLowerCase(Locale.ROOT)));
        assertThat(executionState.snapshotName(), containsString(policyName.toLowerCase(Locale.ROOT)));

        // re-running this step results in no change to the important outputs
        newClusterState = generateSnapshotNameStep.performAction(indexMetadata.getIndex(), newClusterState);
        LifecycleExecutionState repeatedState = newClusterState.metadata().index(indexName).getLifecycleExecutionState();
        assertThat(repeatedState.snapshotIndexName(), is(executionState.snapshotIndexName()));
        assertThat(repeatedState.snapshotRepository(), is(executionState.snapshotRepository()));
        assertThat(repeatedState.snapshotName(), is(executionState.snapshotName()));
    }

    public void testPerformActionRejectsNonexistentRepository() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        GenerateSnapshotNameStep generateSnapshotNameStep = createRandomInstance();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, false).putCustom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY).build())
            .build();

        IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> generateSnapshotNameStep.performAction(indexMetadata.getIndex(), clusterState)
        );
        assertThat(
            illegalStateException.getMessage(),
            is(
                "repository ["
                    + generateSnapshotNameStep.getSnapshotRepository()
                    + "] "
                    + "is missing. [test-ilm-policy] policy for index ["
                    + indexName
                    + "] cannot continue until the repository "
                    + "is created or the policy is changed"
            )
        );
    }

    public void testPerformActionWillOverwriteCachedRepository() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";

        LifecycleExecutionState.Builder newCustomData = LifecycleExecutionState.builder();
        newCustomData.setSnapshotName("snapshot-name-is-not-touched");
        newCustomData.setSnapshotRepository("snapshot-repository-will-be-reset");

        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, newCustomData.build().asMap())
            .build();

        GenerateSnapshotNameStep generateSnapshotNameStep = createRandomInstance();

        // generate a snapshot repository with the expected name
        RepositoryMetadata repo = new RepositoryMetadata(generateSnapshotNameStep.getSnapshotRepository(), "fs", Settings.EMPTY);

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, false)
                    .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(Collections.singletonList(repo)))
                    .build()
            )
            .build();

        ClusterState newClusterState = generateSnapshotNameStep.performAction(indexMetadata.getIndex(), clusterState);

        LifecycleExecutionState executionState = newClusterState.metadata().index(indexName).getLifecycleExecutionState();
        assertThat(executionState.snapshotName(), is("snapshot-name-is-not-touched"));
        assertThat(executionState.snapshotRepository(), is(generateSnapshotNameStep.getSnapshotRepository()));
    }

    public void testNameGeneration() {
        long time = 1552684146542L; // Fri Mar 15 2019 21:09:06 UTC
        assertThat(generateSnapshotName("name"), startsWith("name-"));
        assertThat(generateSnapshotName("name").length(), greaterThan("name-".length()));

        IndexNameExpressionResolver.ResolverContext resolverContext = new IndexNameExpressionResolver.ResolverContext(time);
        assertThat(generateSnapshotName("<name-{now}>", resolverContext), startsWith("name-2019.03.15-"));
        assertThat(generateSnapshotName("<name-{now}>", resolverContext).length(), greaterThan("name-2019.03.15-".length()));

        assertThat(generateSnapshotName("<name-{now/M}>", resolverContext), startsWith("name-2019.03.01-"));

        assertThat(generateSnapshotName("<name-{now/m{yyyy-MM-dd.HH:mm:ss}}>", resolverContext), startsWith("name-2019-03-15.21:09:00-"));
    }

    public void testNameValidation() {
        assertThat(validateGeneratedSnapshotName("name-", generateSnapshotName("name-")), nullValue());
        assertThat(validateGeneratedSnapshotName("<name-{now}>", generateSnapshotName("<name-{now}>")), nullValue());

        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("", generateSnapshotName(""));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name []: cannot be empty"));
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("#start", generateSnapshotName("#start"));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name [#start]: must not contain '#'"));
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("_start", generateSnapshotName("_start"));
            assertThat(validationException, notNullValue());
            assertThat(
                validationException.validationErrors(),
                containsInAnyOrder("invalid snapshot name [_start]: must not start with " + "'_'")
            );
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("aBcD", generateSnapshotName("aBcD"));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name [aBcD]: must be lowercase"));
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("na>me", generateSnapshotName("na>me"));
            assertThat(validationException, notNullValue());
            assertThat(
                validationException.validationErrors(),
                containsInAnyOrder(
                    "invalid snapshot name [na>me]: must not contain "
                        + "contain the following characters "
                        + Strings.INVALID_FILENAME_CHARS
                )
            );
        }
    }
}
