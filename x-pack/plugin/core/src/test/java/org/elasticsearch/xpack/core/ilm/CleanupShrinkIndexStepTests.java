/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Map;

import static org.elasticsearch.common.IndexNameGenerator.generateValidIndexName;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

public class CleanupShrinkIndexStepTests extends AbstractStepTestCase<CleanupShrinkIndexStep> {

    @Override
    public CleanupShrinkIndexStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new CleanupShrinkIndexStep(stepKey, nextStepKey, client);
    }

    @Override
    protected CleanupShrinkIndexStep copyInstance(CleanupShrinkIndexStep instance) {
        return new CleanupShrinkIndexStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    @Override
    public CleanupShrinkIndexStep mutateInstance(CleanupShrinkIndexStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        switch (between(0, 1)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new CleanupShrinkIndexStep(key, nextKey, instance.getClient());
    }

    public void testPerformActionDoesntFailIfShrinkingIndexNameIsMissing() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));

        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, true));

        CleanupShrinkIndexStep cleanupShrinkIndexStep = createRandomInstance();
        cleanupShrinkIndexStep.performAction(indexMetadata, state, null, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                fail(
                    "expecting the step to not report any failure if there isn't any shrink index name stored in the ILM execution "
                        + "state but got:"
                        + e.getMessage()
                );
            }
        });
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        String shrinkIndexName = generateValidIndexName("shrink-", indexName);
        Map<String, String> ilmCustom = Map.of("shrink_index_name", shrinkIndexName);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, true));

        try (var threadPool = createThreadPool()) {
            final var client = getDeleteIndexRequestAssertingClient(threadPool, shrinkIndexName);
            CleanupShrinkIndexStep step = new CleanupShrinkIndexStep(randomStepKey(), randomStepKey(), client);
            step.performAction(indexMetadata, state, null, ActionListener.noop());
        }
    }

    public void testDeleteSkippedIfManagedIndexIsShrunkAndSourceDoesntExist() {
        String sourceIndex = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        String shrinkIndexName = generateValidIndexName("shrink-", sourceIndex);
        Map<String, String> ilmCustom = Map.of("shrink_index_name", shrinkIndexName);

        IndexMetadata.Builder shrunkIndexMetadataBuilder = IndexMetadata.builder(shrinkIndexName)
            .settings(
                settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName)
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex)
            )
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata shrunkIndexMetadata = shrunkIndexMetadataBuilder.build();

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(shrunkIndexMetadata, true));

        try (var threadPool = createThreadPool()) {
            final var client = getFailingIfCalledClient(threadPool);
            CleanupShrinkIndexStep step = new CleanupShrinkIndexStep(randomStepKey(), randomStepKey(), client);
            step.performAction(shrunkIndexMetadata, state, null, ActionListener.noop());
        }
    }

    private NoOpClient getDeleteIndexRequestAssertingClient(ThreadPool threadPool, String shrinkIndexName) {
        return new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action.name(), is(TransportDeleteIndexAction.TYPE.name()));
                assertTrue(request instanceof DeleteIndexRequest);
                assertThat(((DeleteIndexRequest) request).indices(), arrayContaining(shrinkIndexName));
            }
        };
    }

    private NoOpClient getFailingIfCalledClient(ThreadPool threadPool) {
        return new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                throw new IllegalStateException(
                    "not expecting client to be called, but received request [" + request + "] for action [" + action + "]"
                );
            }
        };
    }
}
