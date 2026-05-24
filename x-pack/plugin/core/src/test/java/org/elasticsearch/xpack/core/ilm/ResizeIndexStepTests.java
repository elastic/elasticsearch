/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.common.IndexNameGenerator.generateValidIndexName;
import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.equalTo;

public class ResizeIndexStepTests extends AbstractStepTestCase<ResizeIndexStep> {

    @Override
    public ResizeIndexStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        ResizeType resizeType = randomFrom(ResizeType.values());
        Settings.Builder settings = Settings.builder();
        ByteSizeValue maxPrimaryShardSize = null;
        // Only shrink supports max_primary_shard_size, so if we pick shrink we sometimes set it, otherwise we always
        // set number_of_shards.
        if (resizeType != ResizeType.SHRINK || randomBoolean()) {
            settings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 20));
        } else {
            maxPrimaryShardSize = ByteSizeValue.ofBytes(between(1, 100));
        }
        return new ResizeIndexStep(
            stepKey,
            nextStepKey,
            client,
            resizeType,
            (index, state) -> randomAlphaOfLength(5) + index,
            indexMetadata -> settings.build(),
            maxPrimaryShardSize
        );
    }

    @Override
    public ResizeIndexStep mutateInstance(ResizeIndexStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        ResizeType resizeType = instance.getResizeType();
        ByteSizeValue maxPrimaryShardSize = instance.getMaxPrimaryShardSize();

        switch (between(0, 2)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> {
                if (resizeType != ResizeType.SHRINK || randomBoolean()) {
                    resizeType = randomValueOtherThan(resizeType, () -> randomFrom(ResizeType.values()));
                    maxPrimaryShardSize = null;
                } else {
                    maxPrimaryShardSize = randomValueOtherThan(maxPrimaryShardSize, () -> ByteSizeValue.ofBytes(between(1, 100)));
                }
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new ResizeIndexStep(
            key,
            nextKey,
            instance.getClientWithoutProject(),
            resizeType,
            instance.getTargetIndexNameSupplier(),
            instance.getTargetIndexSettingsSupplier(),
            maxPrimaryShardSize
        );
    }

    @Override
    public ResizeIndexStep copyInstance(ResizeIndexStep instance) {
        return new ResizeIndexStep(
            instance.getKey(),
            instance.getNextStepKey(),
            instance.getClientWithoutProject(),
            instance.getResizeType(),
            instance.getTargetIndexNameSupplier(),
            instance.getTargetIndexSettingsSupplier(),
            instance.getMaxPrimaryShardSize()
        );
    }

    public void testPerformAction() throws Exception {
        String lifecycleName = randomAlphaOfLength(5);
        ResizeIndexStep step = createRandomInstance();
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putAlias(AliasMetadata.builder("my_alias"))
            .build();

        Mockito.doAnswer(invocation -> {
            final ResizeRequest request = invocation.getArgument(1);
            final ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            assertThat(request.getSourceIndex(), equalTo(sourceIndexMetadata.getIndex().getName()));
            assertThat(request.getTargetIndexRequest().aliases(), equalTo(Set.of()));

            Settings expectedSettings = Settings.builder()
                .put(step.getTargetIndexSettingsSupplier().apply(null))
                .put(LifecycleSettings.LIFECYCLE_SKIP, true)
                .build();
            assertThat(request.getTargetIndexRequest().settings(), equalTo(expectedSettings));
            assertThat(request.getMaxPrimaryShardSize(), equalTo(step.getMaxPrimaryShardSize()));
            listener.onResponse(new CreateIndexResponse(true, true, sourceIndexMetadata.getIndex().getName()));
            return null;
        }).when(projectClient).execute(Mockito.same(TransportResizeAction.TYPE), Mockito.any(), Mockito.any());

        final var state = projectStateWithEmptyProject();
        performActionAndWait(step, sourceIndexMetadata, state, null);

        Mockito.verify(client).projectClient(state.projectId());
        Mockito.verify(projectClient).execute(Mockito.same(TransportResizeAction.TYPE), Mockito.any(), Mockito.any());
        Mockito.verifyNoMoreInteractions(client, projectClient);
    }

    public void testPerformActionShrunkenIndexExists() throws Exception {
        String sourceIndexName = randomAlphaOfLength(10);
        String lifecycleName = randomAlphaOfLength(5);
        ResizeIndexStep step = createRandomInstance();
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());
        String generatedShrunkenIndexName = generateValidIndexName(SHRUNKEN_INDEX_PREFIX, sourceIndexName);
        lifecycleState.setShrinkIndexName(generatedShrunkenIndexName);
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(sourceIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putAlias(AliasMetadata.builder("my_alias"))
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(generatedShrunkenIndexName)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Map<String, IndexMetadata> indices = Map.of(generatedShrunkenIndexName, indexMetadata);
        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).indices(indices));

        step.performAction(sourceIndexMetadata, state, null, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                fail("onFailure should not be called in this test, called with exception: " + e.getMessage());
            }
        });
    }

    public void testPerformActionIsCompleteForUnAckedRequests() throws Exception {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ResizeIndexStep step = createRandomInstance();

        Mockito.doAnswer(invocation -> {
            final ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            listener.onResponse(new CreateIndexResponse(false, false, indexMetadata.getIndex().getName()));
            return null;
        }).when(projectClient).execute(Mockito.same(TransportResizeAction.TYPE), Mockito.any(), Mockito.any());

        final var state = projectStateWithEmptyProject();
        performActionAndWait(step, indexMetadata, state, null);

        Mockito.verify(client).projectClient(state.projectId());
        Mockito.verify(projectClient).execute(Mockito.same(TransportResizeAction.TYPE), Mockito.any(), Mockito.any());
        Mockito.verifyNoMoreInteractions(client, projectClient);
    }

    public void testPerformActionFailure() {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Exception exception = new RuntimeException();
        ResizeIndexStep step = createRandomInstance();

        Mockito.doAnswer(invocation -> {
            final ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            listener.onFailure(exception);
            return null;
        }).when(projectClient).execute(Mockito.same(TransportResizeAction.TYPE), Mockito.any(), Mockito.any());

        final var state = projectStateWithEmptyProject();
        assertSame(exception, expectThrows(Exception.class, () -> performActionAndWait(step, indexMetadata, state, null)));

        Mockito.verify(client).projectClient(state.projectId());
        Mockito.verify(projectClient).execute(Mockito.same(TransportResizeAction.TYPE), Mockito.any(), Mockito.any());
        Mockito.verifyNoMoreInteractions(client, projectClient);
    }

}
