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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;

import java.util.Locale;

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
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                snapshotRepository = randomValueOtherThan(snapshotRepository, () -> randomAlphaOfLengthBetween(5, 10));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
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
        IndexMetadata.Builder indexMetadataBuilder =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));

        final IndexMetadata indexMetadata = indexMetadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
                .metadata(Metadata.builder().put(indexMetadata, false).build()).build();

        GenerateSnapshotNameStep generateSnapshotNameStep = createRandomInstance();
        ClusterState newClusterState = generateSnapshotNameStep.performAction(indexMetadata.getIndex(), clusterState);

        LifecycleExecutionState executionState = LifecycleExecutionState.fromIndexMetadata(newClusterState.metadata().index(indexName));
        assertThat("the " + GenerateSnapshotNameStep.NAME + " step must generate a snapshot name", executionState.getSnapshotName(),
            notNullValue());
        assertThat(executionState.getSnapshotRepository(), is(generateSnapshotNameStep.getSnapshotRepository()));
        assertThat(executionState.getSnapshotName(), containsString(indexName.toLowerCase(Locale.ROOT)));
        assertThat(executionState.getSnapshotName(), containsString(policyName.toLowerCase(Locale.ROOT)));
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
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name [_start]: must not start with " +
                "'_'"));
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("aBcD", generateSnapshotName("aBcD"));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name [aBcD]: must be lowercase"));
        }
        {
            ActionRequestValidationException validationException = validateGeneratedSnapshotName("na>me", generateSnapshotName("na>me"));
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("invalid snapshot name [na>me]: must not contain " +
                "contain the following characters " + Strings.INVALID_FILENAME_CHARS));
        }
    }
}
